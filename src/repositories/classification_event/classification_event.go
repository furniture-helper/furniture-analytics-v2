package classficationeventrepository

import (
	"context"
	"furniture-analytics/models/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ClassificationEventRepository struct {
	pool *pgxpool.Pool
}

func NewClassificationEventRepository(pool *pgxpool.Pool) *ClassificationEventRepository {
	return &ClassificationEventRepository{
		pool: pool,
	}
}

func (r *ClassificationEventRepository) InsetClassificationEvents(ctx context.Context, events []events.ClassificationEvent) (int, error) {
	const query = `
		INSERT INTO analytics.classification_event (event_ts, url, domain, classification, confidence, source)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	if len(events) == 0 {
		return 0, nil
	}

	batch := &pgx.Batch{}
	for _, event := range events {
		batch.Queue(query,
			event.Timestamp,
			event.URL,
			event.Domain,
			event.Classification,
			event.Confidence,
			event.Source,
		)
	}

	br := r.pool.SendBatch(ctx, batch)
	defer func() {
		if err := br.Close(); err != nil {
			// Ignore close errors for batch result cleanup.
		}
	}()

	inserted := 0
	for i := 0; i < len(events); i++ {
		tag, err := br.Exec()
		if err != nil {
			return inserted, err
		}
		inserted += int(tag.RowsAffected())
	}

	return inserted, nil
}
