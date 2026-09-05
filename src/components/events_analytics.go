package components

import (
	"context"
	"errors"
	"furniture-analytics/config"
	"furniture-analytics/repositories"
	kafkaservice "furniture-analytics/services/kafka"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	received  atomic.Int64
	handled   atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
}

type EventAnalyticsOrchestrator[T any] struct {
	eventName       string
	metrics         Metrics
	messagesChannel chan *kafkaservice.KafkaMessage
	eventsChannel   chan T
	kafkaTopic      string
	kafkaGroupId    string
	handleMessage   func(message kafkaservice.KafkaMessage) (*T, error)
	processEvent    func(event T) error
	finalize        func()
}

func NewEventAnalyticsOrchestrator[T any](eventName string, kafkaTopic string, kafkaGroupId string, handleMessage func(message kafkaservice.KafkaMessage) (*T, error), processEvent func(event T) error, finalize func()) *EventAnalyticsOrchestrator[T] {
	return &EventAnalyticsOrchestrator[T]{
		eventName:       eventName,
		metrics:         Metrics{},
		messagesChannel: make(chan *kafkaservice.KafkaMessage, 100),
		eventsChannel:   make(chan T, 100),
		kafkaTopic:      kafkaTopic,
		kafkaGroupId:    kafkaGroupId,
		handleMessage:   handleMessage,
		processEvent:    processEvent,
		finalize:        finalize,
	}
}

func (e *EventAnalyticsOrchestrator[T]) Start(ctx context.Context) {
	startTime := time.Now()

	var readersWg sync.WaitGroup
	readersWg.Add(1)
	go func() {
		defer readersWg.Done()
		e.readMessages(ctx)
		close(e.messagesChannel)
	}()

	var handlersWg sync.WaitGroup
	messageHandlerWorkerCount := 8
	for i := 0; i < messageHandlerWorkerCount; i++ {
		handlersWg.Add(1)
		go func() {
			defer handlersWg.Done()
			e.handleMessages()
		}()
	}
	go func() {
		handlersWg.Wait()
		close(e.eventsChannel)
	}()

	var processorWg sync.WaitGroup
	processorWg.Add(1)
	go func() {
		defer processorWg.Done()
		e.processEvents(ctx)
	}()

	done := make(chan struct{})
	go func() {
		readersWg.Wait()
		handlersWg.Wait()
		processorWg.Wait()
		e.finalize()
		close(done)
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf(
				"[%s] received=%d handled=%d processed=%d failed=%d",
				e.eventName,
				e.metrics.received.Load(),
				e.metrics.handled.Load(),
				e.metrics.processed.Load(),
				e.metrics.failed.Load(),
			)
		case <-done:
			log.Printf(
				"[%s] FINAL duration=%s received=%d handled=%d processed=%d failed=%d",
				e.eventName,
				time.Since(startTime),
				e.metrics.received.Load(),
				e.metrics.handled.Load(),
				e.metrics.processed.Load(),
				e.metrics.failed.Load(),
			)
			return
		}
	}

}

func (e *EventAnalyticsOrchestrator[T]) readMessages(ctx context.Context) {
	log.Printf("[%s] Starting Kafka reader for topic: %s, group: %s", e.eventName, e.kafkaTopic, e.kafkaGroupId)
	topic := e.kafkaTopic
	groupID := e.kafkaGroupId

	kafkaService := kafkaservice.NewKafkaService(config.GetKafkaBrokers(), topic, groupID)
	defer func() {
		if err := kafkaService.Close(); err != nil {
			log.Println("Error closing Kafka reader:", err)
		}
	}()
	const maxMessagesPerRun = 100000
	const perPollMax = 20
	const maxEmptyPolls = 3
	const pollTimeout = 15 * time.Second

	received := 0
	emptyPolls := 0
readLoop:
	for received < maxMessagesPerRun {
		remaining := maxMessagesPerRun - received
		pollSize := perPollMax
		if remaining < pollSize {
			pollSize = remaining
		}

		pollCtx, pollCancel := context.WithTimeout(ctx, pollTimeout)
		type pollResult struct {
			batch []*kafkaservice.KafkaMessage
			err   error
		}
		pollResultCh := make(chan pollResult, 1)
		go func() {
			batch, err := kafkaService.GetMessages(pollCtx, pollSize)
			pollResultCh <- pollResult{batch: batch, err: err}
		}()

		var batch []*kafkaservice.KafkaMessage
		var err error
		select {
		case result := <-pollResultCh:
			batch = result.batch
			err = result.err
			pollCancel()
		case <-pollCtx.Done():
			pollCancel()
			if err := kafkaService.Close(); err != nil {
				log.Printf("Error closing Kafka reader after poll timeout: %v", err)
			}
			log.Println("Kafka poll timed out; exiting batch run")
			break readLoop
		}

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				log.Println("Kafka poll timed out; exiting batch run")
				break readLoop
			}
			log.Println("Error getting messages from Kafka:", err)
			break readLoop
		}
		if len(batch) == 0 {
			emptyPolls++
			if emptyPolls >= maxEmptyPolls {
				log.Println("No messages received from Kafka")
				break
			}
			continue
		}

		emptyPolls = 0
		for _, m := range batch {
			e.messagesChannel <- m
			e.metrics.received.Add(1)
			received++
		}
	}
}

func (e *EventAnalyticsOrchestrator[T]) handleMessages() {
	for m := range e.messagesChannel {
		event, err := e.handleMessage(*m)
		if err != nil {
			log.Printf("failed to handle message: %v", err)
			e.metrics.failed.Add(1)
			continue
		}

		e.eventsChannel <- *event
		e.metrics.handled.Add(1)
	}
}

func (e *EventAnalyticsOrchestrator[T]) processEvents(ctx context.Context) {
	cfg := config.GetDatabaseConfig(ctx)
	pool, err := repositories.NewPostgresPool(ctx, cfg)
	if err != nil {
		log.Printf("failed to create Postgres pool: %v", err)
		os.Exit(1)
	}
	defer pool.Close()

	for event := range e.eventsChannel {
		err := e.processEvent(event)
		if err != nil {
			log.Printf("failed to process event: %v", err)
			e.metrics.failed.Add(1)
		}
	}
}
