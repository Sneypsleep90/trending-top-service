package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Sneypsleep90/trending-top/internal/config"
	"github.com/Sneypsleep90/trending-top/internal/metrics"
)

type MemoryStore struct {
	wheel   *BucketWheel
	cache   *TopCache
	metrics *metrics.Metrics
}

func NewMemoryStore(cfg config.Config, metricSet *metrics.Metrics) (*MemoryStore, error) {
	wheel, err := NewBucketWheel(cfg.BucketCount, cfg.BucketDuration())
	if err != nil {
		return nil, fmt.Errorf("store.NewMemoryStore: %w", err)
	}

	cache := NewTopCache(wheel, cfg.TopCacheTTL(), defaultTopLimit, func(uniqueQueries int) {
		metricSet.SetStoreUniqueQueries(uniqueQueries)
	})

	return &MemoryStore{
		wheel:   wheel,
		cache:   cache,
		metrics: metricSet,
	}, nil
}

func (s *MemoryStore) Add(ctx context.Context, query string) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("memory_store.Add: %w", ctx.Err())
	default:
	}

	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return fmt.Errorf("memory_store.Add: query is empty")
	}

	s.wheel.Add(query)
	s.metrics.IncStoreWrites()

	return nil
}

func (s *MemoryStore) Top(ctx context.Context, n int) ([]TopItem, time.Time, error) {
	select {
	case <-ctx.Done():
		return nil, time.Time{}, fmt.Errorf("memory_store.Top: %w", ctx.Err())
	default:
	}

	items, generatedAt := s.cache.Top(n)

	return items, generatedAt, nil
}

func (s *MemoryStore) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		s.wheel.StartCleaner(ctx)
	}()

	go func() {
		defer wg.Done()
		s.cache.Run(ctx.Done())
	}()

	wg.Wait()
}

func (s *MemoryStore) Counts() map[string]int {
	return s.wheel.Counts()
}
