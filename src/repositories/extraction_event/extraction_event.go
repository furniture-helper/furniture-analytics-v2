package extractioneventrepository

import (
	"context"
	"furniture-analytics/models/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ExtractionEventRepository struct {
	pool *pgxpool.Pool
}

func NewExtractionEventRepository(pool *pgxpool.Pool) *ExtractionEventRepository {
	return &ExtractionEventRepository{
		pool: pool,
	}
}

func (r *ExtractionEventRepository) InsertExtractionEvents(ctx context.Context, events []events.ExtractionEvent) (int, error) {
	const query = `
		INSERT INTO analytics.extraction_events (event_ts, url, domain, source)
		VALUES ($1, $2, $3, $4)
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
