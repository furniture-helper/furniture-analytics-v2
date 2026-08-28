package crawlereventrepository

import (
	"context"
	"furniture-analytics/models/events"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CrawlerEventRepository struct {
	pool *pgxpool.Pool
}

func NewCrawlerEventRepository(pool *pgxpool.Pool) *CrawlerEventRepository {
	return &CrawlerEventRepository{
		pool: pool,
	}
}

func (r *CrawlerEventRepository) InsertSuccessCrawlerEvents(ctx context.Context, events []events.CrawlerEvent) (int, error) {
	const query = `
		INSERT INTO analytics.success_crawler_events (event_ts, url, domain, host, region, duration)
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
			event.Host,
			event.Region,
			event.Duration,
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

func (r *CrawlerEventRepository) InsertFailedCrawlerEvents(ctx context.Context, events []events.CrawlerEvent) (int, error) {
	const query = `
		INSERT INTO analytics.failed_crawler_events (event_ts, url, domain, host, region, error_message)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	batch := &pgx.Batch{}
	queued := 0
	for _, event := range events {
		if event.Error == nil {
			continue
		}
		batch.Queue(query,
			event.Timestamp,
			event.URL,
			event.Domain,
			event.Host,
			event.Region,
			*event.Error,
		)
		queued++
	}

	if queued == 0 {
		return 0, nil
	}

	br := r.pool.SendBatch(ctx, batch)
	defer func() {
		if err := br.Close(); err != nil {
			// Ignore close errors for batch result cleanup.
		}
	}()

	inserted := 0
	for i := 0; i < queued; i++ {
		tag, err := br.Exec()
		if err != nil {
			return inserted, err
		}
		inserted += int(tag.RowsAffected())
	}

	return inserted, nil
}
