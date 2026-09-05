package main

import (
	"context"
	"furniture-analytics/components"
	"furniture-analytics/config"
	"furniture-analytics/repositories"
	"log"
	"os"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.GetMaximumRuntime())*time.Second)
	defer cancel()

	cfg := config.GetDatabaseConfig(ctx)
	pool, err := repositories.NewPostgresPool(ctx, cfg)
	if err != nil {
		log.Printf("failed to create Postgres pool: %v", err)
		os.Exit(1)
	}
	defer pool.Close()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		crawlerEventAnalytics := components.NewCrawlerEventAnalytics(pool)
		crawlerEventAnalytics.Orchestrator.Start(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		minimizerEventAnalytics := components.NewMinimizerEventAnalytics(pool)
		minimizerEventAnalytics.Orchestrator.Start(ctx)
	}()

	wg.Wait()
}
