package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sneypsleep90/trending-top/internal/config"
	"github.com/Sneypsleep90/trending-top/internal/metrics"
)

func TestBucketWheel_Add(t *testing.T) {
	wheel, err := NewBucketWheel(30, 10*time.Second)
	if err != nil {
		t.Fatalf("NewBucketWheel: %v", err)
	}

	wheel.Add("кроссовки найк")
	wheel.Add("кроссовки найк")
	wheel.Add("айфон 15")

	counts := wheel.Counts()
	if got := counts["кроссовки найк"]; got != 2 {
		t.Fatalf("expected кроссовки найк count 2, got %d", got)
	}
	if got := counts["айфон 15"]; got != 1 {
		t.Fatalf("expected айфон 15 count 1, got %d", got)
	}
}

func TestBucketWheel_Rotate(t *testing.T) {
	wheel, err := NewBucketWheel(30, 10*time.Second)
	if err != nil {
		t.Fatalf("NewBucketWheel: %v", err)
	}

	wheel.Add("кроссовки найк")
	for i := 0; i < 30; i++ {
		wheel.Rotate()
	}

	if len(wheel.Counts()) != 0 {
		t.Fatalf("expected empty counts after full rotation, got %#v", wheel.Counts())
	}
}

func TestBucketWheel_SlidingWindow(t *testing.T) {
	wheel, err := NewBucketWheel(3, time.Second)
	if err != nil {
		t.Fatalf("NewBucketWheel: %v", err)
	}

	wheel.Add("платье летнее")
	wheel.Rotate()
	wheel.Add("айфон 15")
	wheel.Rotate()
	wheel.Add("кроссовки найк")

	counts := wheel.Counts()
	if got := counts["платье летнее"]; got != 1 {
		t.Fatalf("expected платье летнее count 1 before expiration, got %d", got)
	}

	wheel.Rotate()
	counts = wheel.Counts()
	if _, ok := counts["платье летнее"]; ok {
		t.Fatalf("expected платье летнее to expire, got %#v", counts)
	}
	if got := counts["айфон 15"]; got != 1 {
		t.Fatalf("expected айфон 15 count 1, got %d", got)
	}
}

func TestTopCache_Top(t *testing.T) {
	wheel, err := NewBucketWheel(30, 10*time.Second)
	if err != nil {
		t.Fatalf("NewBucketWheel: %v", err)
	}

	for i := 1; i <= 20; i++ {
		query := fmt.Sprintf("query-%02d", i)
		for j := 0; j < i; j++ {
			wheel.Add(query)
		}
	}

	cache := NewTopCache(wheel, time.Hour, 100, nil)
	cache.Rebuild()

	items, generatedAt := cache.Top(5)
	if generatedAt.IsZero() {
		t.Fatal("expected generatedAt to be set")
	}
	if len(items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(items))
	}
	if items[0].Query != "query-20" || items[0].Count != 20 {
		t.Fatalf("unexpected first item: %#v", items[0])
	}
	if items[4].Query != "query-16" || items[4].Count != 16 {
		t.Fatalf("unexpected fifth item: %#v", items[4])
	}
}

func TestMemoryStore_Concurrent(t *testing.T) {
	cfg := config.Config{
		BucketCount:       30,
		BucketDurationSec: 10,
		TopCacheTTLMS:     200,
	}
	metricSet := metrics.New(prometheus.NewRegistry())
	store, err := NewMemoryStore(cfg, metricSet)
	if err != nil {
		t.Fatalf("NewMemoryStore: %v", err)
	}

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if err := store.Add(context.Background(), "кроссовки найк"); err != nil {
				t.Errorf("Add: %v", err)
			}
		}()
	}
	wg.Wait()

	counts := store.Counts()
	if got := counts["кроссовки найк"]; got != goroutines {
		t.Fatalf("expected %d, got %d", goroutines, got)
	}
}
