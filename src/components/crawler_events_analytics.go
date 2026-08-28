package components

import (
	"context"
	"errors"
	"furniture-analytics/config"
	"furniture-analytics/models/events"
	"furniture-analytics/repositories"
	crawlereventrepository "furniture-analytics/repositories/crawler_event"
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

func CrawlerEventsAnalytics(ctx context.Context) {
	startTime := time.Now()

	kafkaMessages := make(chan *kafkaservice.KafkaMessage, 100)
	crawlerEvents := make(chan events.CrawlerEvent, 100)

	var metrics Metrics

	var readersWg sync.WaitGroup
	readersWg.Add(1)
	go func() {
		defer readersWg.Done()
		readMessages(ctx, kafkaMessages, &metrics)
		close(kafkaMessages)
	}()

	var handlersWg sync.WaitGroup
	messageHandlerWorkerCount := 8
	for i := 0; i < messageHandlerWorkerCount; i++ {
		handlersWg.Add(1)
		go func() {
			defer handlersWg.Done()
			handleMessages(kafkaMessages, crawlerEvents, &metrics)
		}()
	}
	go func() {
		handlersWg.Wait()
		close(crawlerEvents)
	}()

	var processorWg sync.WaitGroup
	processorWg.Add(1)
	go func() {
		defer processorWg.Done()
		processCrawlerEvents(ctx, crawlerEvents, &metrics)
	}()

	done := make(chan struct{})
	go func() {
		readersWg.Wait()
		handlersWg.Wait()
		processorWg.Wait()
		close(done)
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf(
				"received=%d handled=%d processed=%d failed=%d",
				metrics.received.Load(),
				metrics.handled.Load(),
				metrics.processed.Load(),
				metrics.failed.Load(),
			)
		case <-done:
			log.Printf(
				"FINAL duration=%s received=%d handled=%d processed=%d failed=%d",
				time.Since(startTime),
				metrics.received.Load(),
				metrics.handled.Load(),
				metrics.processed.Load(),
				metrics.failed.Load(),
			)
			return
		}
	}
}

func readMessages(ctx context.Context, messages chan<- *kafkaservice.KafkaMessage, metrics *Metrics) {
	topic := "crawler-events"
	groupID := "sdfdsfdsf"

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
			messages <- m
			metrics.received.Add(1)
			received++
		}
	}
}

func handleMessages(messages <-chan *kafkaservice.KafkaMessage, crawlerEvents chan<- events.CrawlerEvent, metrics *Metrics) {
	for m := range messages {
		crawlerEvent, err := events.NewCrawlerEventFromMessage(m.Timestamp, m.Message, m.Headers)
		if err != nil {
			log.Printf("failed to create CrawlerEvent from message: %v", err)
			continue
		}
		if crawlerEvent == nil {
			log.Println("Failed to create CrawlerEvent from message")
			log.Printf("Message: %s", m.Message)
			continue
		}

		crawlerEvents <- *crawlerEvent
		metrics.handled.Add(1)
	}
}

func processCrawlerEvents(ctx context.Context, crawlerEvents <-chan events.CrawlerEvent, metrics *Metrics) {
	cfg := config.GetDatabaseConfig(ctx)
	pool, err := repositories.NewPostgresPool(ctx, cfg)
	if err != nil {
		log.Printf("failed to create Postgres pool: %v", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := crawlereventrepository.NewCrawlerEventRepository(pool)
	successBatchedEvents := make([]events.CrawlerEvent, 0, 100)
	failedBatchedEvents := make([]events.CrawlerEvent, 0, 100)
	for event := range crawlerEvents {
		if event.Status == events.Success {
			successBatchedEvents = append(successBatchedEvents, event)
		} else if event.Status == events.Failure {
			failedBatchedEvents = append(failedBatchedEvents, event)
		}

		if len(successBatchedEvents) >= 100 {
			inserted, err := repo.InsertSuccessCrawlerEvents(ctx, successBatchedEvents)
			if err != nil {
				log.Printf("failed to insert success crawler events: %v", err)
			}
			metrics.processed.Add(int64(inserted))
			successBatchedEvents = successBatchedEvents[:0]
		}

		if len(failedBatchedEvents) >= 100 {
			inserted, err := repo.InsertFailedCrawlerEvents(ctx, failedBatchedEvents)
			if err != nil {
				log.Printf("failed to insert failed crawler events: %v", err)
			}
			metrics.processed.Add(int64(inserted))
			failedBatchedEvents = failedBatchedEvents[:0]
		}

	}

	if len(successBatchedEvents) > 0 {
		inserted, err := repo.InsertSuccessCrawlerEvents(ctx, successBatchedEvents)
		if err != nil {
			log.Printf("failed to insert success crawler events: %v", err)
		}
		metrics.processed.Add(int64(inserted))
	}

	if len(failedBatchedEvents) > 0 {
		inserted, err := repo.InsertFailedCrawlerEvents(ctx, failedBatchedEvents)
		if err != nil {
			log.Printf("failed to insert failed crawler events: %v", err)
		}
		metrics.processed.Add(int64(inserted))
	}

}
