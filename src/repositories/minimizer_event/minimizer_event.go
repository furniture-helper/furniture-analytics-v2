package minimizereventrepository

import (
	"context"
	"furniture-analytics/models/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MinimizerEventRepository struct {
	pool *pgxpool.Pool
}

func NewMinimizerEventRepository(pool *pgxpool.Pool) *MinimizerEventRepository {
	return &MinimizerEventRepository{
		pool: pool,
	}
}

func (r *MinimizerEventRepository) InsertMinimizerEvents(ctx context.Context, events []events.MinimizerEvent) (int, error) {
	const query = `
		INSERT INTO analytics.minimizer_events (event_ts, url, domain, original_size, minimized_size)
		VALUES ($1, $2, $3, $4, $5)
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
			event.OriginalSize,
			event.MinimizedSize,
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
