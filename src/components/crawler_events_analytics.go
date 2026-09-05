package components

import (
	"context"
	"furniture-analytics/models/events"
	crawlereventrepository "furniture-analytics/repositories/crawler_event"
	kafkaservice "furniture-analytics/services/kafka"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type CrawlerEventAnalytics struct {
	Orchestrator           *EventAnalyticsOrchestrator[events.CrawlerEvent]
	crawlerEventRepository *crawlereventrepository.CrawlerEventRepository
	successBatchedEvents   []events.CrawlerEvent
	failedBatchedEvents    []events.CrawlerEvent
}

func NewCrawlerEventAnalytics(pool *pgxpool.Pool) *CrawlerEventAnalytics {
	a := &CrawlerEventAnalytics{
		crawlerEventRepository: crawlereventrepository.NewCrawlerEventRepository(pool),
		successBatchedEvents:   make([]events.CrawlerEvent, 0, 100),
		failedBatchedEvents:    make([]events.CrawlerEvent, 0, 100),
	}

	a.Orchestrator = NewEventAnalyticsOrchestrator[events.CrawlerEvent](
		"crawler_event",
		"crawler-events",
		"sdfdsfdsf",
		handleCrawlerEventMessage,
		a.processCrawlerEvent,
		a.flushBatchedEvents,
	)

	return a
}

func handleCrawlerEventMessage(message kafkaservice.KafkaMessage) (*events.CrawlerEvent, error) {
	event, err := events.NewCrawlerEventFromMessage(message.Timestamp, message.Message, message.Headers)
	return event, err
}

func (e *CrawlerEventAnalytics) processCrawlerEvent(event events.CrawlerEvent) error {
	if event.Status == events.Success {
		e.successBatchedEvents = append(e.successBatchedEvents, event)
	} else if event.Status == events.Failure {
		e.failedBatchedEvents = append(e.failedBatchedEvents, event)
	}

	if len(e.successBatchedEvents) >= 100 {
		inserted, err := e.crawlerEventRepository.InsertSuccessCrawlerEvents(context.Background(), e.successBatchedEvents)
		if err != nil {
			log.Printf("failed to insert success crawler events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.successBatchedEvents = e.successBatchedEvents[:0]
	}

	if len(e.failedBatchedEvents) >= 100 {
		inserted, err := e.crawlerEventRepository.InsertFailedCrawlerEvents(context.Background(), e.failedBatchedEvents)
		if err != nil {
			log.Printf("failed to insert failed crawler events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.failedBatchedEvents = e.failedBatchedEvents[:0]
	}

	return nil
}

func (e *CrawlerEventAnalytics) flushBatchedEvents() {
	if len(e.successBatchedEvents) > 0 {
		inserted, err := e.crawlerEventRepository.InsertSuccessCrawlerEvents(context.Background(), e.successBatchedEvents)
		if err != nil {
			log.Printf("failed to insert success crawler events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.successBatchedEvents = e.successBatchedEvents[:0]
	}

	if len(e.failedBatchedEvents) > 0 {
		inserted, err := e.crawlerEventRepository.InsertFailedCrawlerEvents(context.Background(), e.failedBatchedEvents)
		if err != nil {
			log.Printf("failed to insert failed crawler events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.failedBatchedEvents = e.failedBatchedEvents[:0]
	}
}
