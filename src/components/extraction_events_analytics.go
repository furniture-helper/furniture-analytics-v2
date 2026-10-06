package components

import (
	"context"
	"furniture-analytics/models/events"
	extractioneventrepository "furniture-analytics/repositories/extraction_event"
	kafkaservice "furniture-analytics/services/kafka"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ExtractionEventAnalytics struct {
	Orchestrator              *EventAnalyticsOrchestrator[events.ExtractionEvent]
	extractionEventRepository *extractioneventrepository.ExtractionEventRepository
	batchedEvents             []events.ExtractionEvent
}

func NewExtractionEventAnalytics(pool *pgxpool.Pool) *ExtractionEventAnalytics {
	a := &ExtractionEventAnalytics{
		extractionEventRepository: extractioneventrepository.NewExtractionEventRepository(pool),
		batchedEvents:             make([]events.ExtractionEvent, 0, 100),
	}

	a.Orchestrator = NewEventAnalyticsOrchestrator[events.ExtractionEvent](
		"extraction-event",
		"extraction-events",
		"extraction-events-ecs-consumer-group-2",
		handleExtractionEventMessage,
		a.processExtractionEvent,
		a.flushBatchedEvents,
	)

	return a
}

func handleExtractionEventMessage(message kafkaservice.KafkaMessage) (*events.ExtractionEvent, error) {
	event, err := events.NewExtractionEventFromMessage(message.Timestamp, message.Message, message.Headers)
	return event, err
}

func (e *ExtractionEventAnalytics) processExtractionEvent(event events.ExtractionEvent) error {
	e.batchedEvents = append(e.batchedEvents, event)

	if len(e.batchedEvents) >= 100 {
		inserted, err := e.extractionEventRepository.InsertExtractionEvents(context.Background(), e.batchedEvents)
		if err != nil {
			log.Printf("failed to insert extraction events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.batchedEvents = e.batchedEvents[:0]
	}

	return nil
}

func (e *ExtractionEventAnalytics) flushBatchedEvents() {
	if len(e.batchedEvents) > 0 {
		inserted, err := e.extractionEventRepository.InsertExtractionEvents(context.Background(), e.batchedEvents)
		if err != nil {
			log.Printf("failed to insert extraction events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.batchedEvents = e.batchedEvents[:0]
	}
}
