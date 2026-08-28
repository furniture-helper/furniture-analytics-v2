package main

import (
	"context"
	"furniture-analytics/components"
	"furniture-analytics/config"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.GetMaximumRuntime())*time.Second)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		components.CrawlerEventsAnalytics(ctx)
	}()

	wg.Wait()
}
