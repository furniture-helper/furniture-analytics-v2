package components

import (
	"context"
	"furniture-analytics/models/events"
	minimizereventrepository "furniture-analytics/repositories/minimizer_event"
	kafkaservice "furniture-analytics/services/kafka"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
)

type MinimizerEventAnalytics struct {
	Orchestrator             *EventAnalyticsOrchestrator[events.MinimizerEvent]
	minimizerEventRepository *minimizereventrepository.MinimizerEventRepository
	batchedEvents            []events.MinimizerEvent
}

func NewMinimizerEventAnalytics(pool *pgxpool.Pool) *MinimizerEventAnalytics {
	a := &MinimizerEventAnalytics{
		minimizerEventRepository: minimizereventrepository.NewMinimizerEventRepository(pool),
		batchedEvents:            make([]events.MinimizerEvent, 0, 100),
	}

	a.Orchestrator = NewEventAnalyticsOrchestrator[events.MinimizerEvent](
		"minimizer_event",
		"minimizer-events",
		"minimizer-events-ecs-consumer-group",
		handleMinimizerEventMessage,
		a.processMinimizerEvent,
		a.flushBatchedEvents,
	)

	return a
}

func handleMinimizerEventMessage(message kafkaservice.KafkaMessage) (*events.MinimizerEvent, error) {
	event, err := events.NewMinimizerEventFromMessage(message.Timestamp, message.Message, message.Headers)
	return event, err
}

func (e *MinimizerEventAnalytics) processMinimizerEvent(event events.MinimizerEvent) error {
	e.batchedEvents = append(e.batchedEvents, event)

	if len(e.batchedEvents) >= 100 {
		inserted, err := e.minimizerEventRepository.InsertMinimizerEvents(context.Background(), e.batchedEvents)
		if err != nil {
			log.Printf("failed to insert success minimizer events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.batchedEvents = e.batchedEvents[:0]
	}

	return nil
}

func (e *MinimizerEventAnalytics) flushBatchedEvents() {
	if len(e.batchedEvents) > 0 {
		inserted, err := e.minimizerEventRepository.InsertMinimizerEvents(context.Background(), e.batchedEvents)
		if err != nil {
			log.Printf("failed to insert success minimizer events: %v", err)
		}
		e.Orchestrator.metrics.processed.Add(int64(inserted))
		e.batchedEvents = e.batchedEvents[:0]
	}
}
