package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type bucket struct {
	mu     sync.RWMutex
	counts map[string]int32
}

type BucketWheel struct {
	buckets        []bucket
	bucketDuration time.Duration
	current        atomic.Uint64
}

func NewBucketWheel(bucketCount int, bucketDuration time.Duration) (*BucketWheel, error) {
	if bucketCount <= 0 {
		return nil, fmt.Errorf("bucket count must be positive")
	}
	if bucketDuration <= 0 {
		return nil, fmt.Errorf("bucket duration must be positive")
	}

	buckets := make([]bucket, bucketCount)
	for i := range buckets {
		buckets[i].counts = make(map[string]int32)
	}

	return &BucketWheel{
		buckets:        buckets,
		bucketDuration: bucketDuration,
	}, nil
}

func (w *BucketWheel) Add(query string) {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return
	}

	idx := int(w.current.Load() % uint64(len(w.buckets)))
	b := &w.buckets[idx]

	b.mu.Lock()
	b.counts[query]++
	b.mu.Unlock()
}

func (w *BucketWheel) Counts() map[string]int {
	result := make(map[string]int)
	for i := range w.buckets {
		b := &w.buckets[i]

		b.mu.RLock()
		for query, count := range b.counts {
			result[query] += int(count)
		}
		b.mu.RUnlock()
	}

	return result
}

func (w *BucketWheel) Rotate() {
	next := (w.current.Load() + 1) % uint64(len(w.buckets))
	b := &w.buckets[next]

	b.mu.Lock()
	clear(b.counts)
	b.mu.Unlock()

	w.current.Store(next)
}

func (w *BucketWheel) StartCleaner(ctx context.Context) {
	ticker := time.NewTicker(w.bucketDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.Rotate()
		}
	}
}

func (w *BucketWheel) WindowDuration() time.Duration {
	return time.Duration(len(w.buckets)) * w.bucketDuration
}
