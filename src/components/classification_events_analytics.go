package components

import (
	"context"
	"furniture-analytics/models/events"
	classficationeventrepository "furniture-analytics/repositories/classification_event"
	kafkaservice "furniture-analytics/services/kafka"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ClassificationEventAnalytics struct {
	Orchestrator                  *EventAnalyticsOrchestrator[events.ClassificationEvent]
	classificationEventRepository *classficationeventrepository.ClassificationEventRepository
	batchedEvents                 []events.ClassificationEvent
}

func NewClassificationEventAnalytics(pool *pgxpool.Pool) *ClassificationEventAnalytics {
	a := &ClassificationEventAnalytics{
		classificationEventRepository: classficationeventrepository.NewClassificationEventRepository(pool),
		batchedEvents:                 make([]events.ClassificationEvent, 0, 100),
	}

	a.Orchestrator = NewEventAnalyticsOrchestrator[events.ClassificationEvent](
		"classification-event",
		"classification-events",
		"classification-events-ecs-consumer-group-2",
		handleClassificationEventMessage,
		a.processClassificationEvent,
		a.flushBatchedEvents,
	)

	return a
}

func handleClassificationEventMessage(message kafkaservice.KafkaMessage) (*events.ClassificationEvent, error) {
	event, err := events.NewClassificationEventFromMessage(message.Timestamp, message.Message, message.Headers)
	return event, err
}

func (e *ClassificationEventAnalytics) processClassificationEvent(event events.ClassificationEvent) error {
	e.batchedEvents = append(e.batchedEvents, event)

	if len(e.batchedEvents) >= 100 {
		inserted, err := e.classificationEventRepository.InsetClassificationEvents(context.Background(), e.batchedEvents)
		if err != nil {
			log.Printf("failed to insert classification events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.batchedEvents = e.batchedEvents[:0]
	}

	return nil
}

func (e *ClassificationEventAnalytics) flushBatchedEvents() {
	if len(e.batchedEvents) > 0 {
		inserted, err := e.classificationEventRepository.InsetClassificationEvents(context.Background(), e.batchedEvents)
		if err != nil {
			log.Printf("failed to insert success minimizer events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.batchedEvents = e.batchedEvents[:0]
	}
}
