package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sneypsleep90/trending-top/internal/config"
	"github.com/Sneypsleep90/trending-top/internal/metrics"
)

const redisTopCacheKey = "trending:top_cache"

// RedisStore implements Store with Redis hashes for buckets and a JSON top cache.
type RedisStore struct {
	rdb            *redis.Client
	bucketCount    int
	bucketDuration time.Duration
	topCacheTTL    time.Duration
	windowDuration time.Duration
	metrics        *metrics.Metrics
	rebuildMu      sync.Mutex
}

// NewRedisStore creates a Redis-backed store.
func NewRedisStore(cfg config.Config, metricSet *metrics.Metrics) *RedisStore {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})

	return &RedisStore{
		rdb:            rdb,
		bucketCount:    cfg.BucketCount,
		bucketDuration: cfg.BucketDuration(),
		topCacheTTL:    cfg.TopCacheTTL(),
		windowDuration: cfg.WindowDuration(),
		metrics:        metricSet,
	}
}

// Ping checks Redis connectivity.
func (s *RedisStore) Ping(ctx context.Context) error {
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis_store.Ping: %w", err)
	}

	return nil
}

// Add increments the Redis hash counter for the current time slot.
func (s *RedisStore) Add(ctx context.Context, query string) error {
	query = strings.TrimSpace(strings.ToLower(query))
	if query == "" {
		return fmt.Errorf("redis_store.Add: query is empty")
	}

	slot := time.Now().Unix() / int64(s.bucketDuration.Seconds())
	key := fmt.Sprintf("trending:bucket:%d", slot)

	pipe := s.rdb.Pipeline()
	pipe.HIncrBy(ctx, key, query, 1)
	pipe.Expire(ctx, key, s.windowDuration*2)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis_store.Add: %w", err)
	}

	s.metrics.IncStoreWrites()

	return nil
}

// Top returns cached top items or lazily rebuilds the Redis cache.
func (s *RedisStore) Top(ctx context.Context, n int) ([]TopItem, time.Time, error) {
	if items, ok, err := s.getCachedTop(ctx); err != nil {
		return nil, time.Time{}, fmt.Errorf("redis_store.Top: %w", err)
	} else if ok {
		return limitTop(items, n), time.Now().UTC(), nil
	}

	s.rebuildMu.Lock()
	defer s.rebuildMu.Unlock()

	if items, ok, err := s.getCachedTop(ctx); err != nil {
		return nil, time.Time{}, fmt.Errorf("redis_store.Top: %w", err)
	} else if ok {
		return limitTop(items, n), time.Now().UTC(), nil
	}

	items, generatedAt, err := s.rebuildTop(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("redis_store.Top: %w", err)
	}

	return limitTop(items, n), generatedAt, nil
}

// Run waits for cancellation; Redis top rebuilds are lazy on reads.
func (s *RedisStore) Run(ctx context.Context) {
	<-ctx.Done()
}

// Close closes the Redis client.
func (s *RedisStore) Close() error {
	if err := s.rdb.Close(); err != nil {
		return fmt.Errorf("redis_store.Close: %w", err)
	}

	return nil
}

func (s *RedisStore) getCachedTop(ctx context.Context) ([]TopItem, bool, error) {
	raw, err := s.rdb.Get(ctx, redisTopCacheKey).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, false, nil
		}

		return nil, false, err
	}

	var items []TopItem
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, false, fmt.Errorf("decode cached top: %w", err)
	}

	return items, true, nil
}

func (s *RedisStore) rebuildTop(ctx context.Context) ([]TopItem, time.Time, error) {
	slotDurationSeconds := int64(s.bucketDuration.Seconds())
	nowSlot := time.Now().Unix() / slotDurationSeconds
	counts := make(map[string]int)

	for offset := 0; offset < s.bucketCount; offset++ {
		key := fmt.Sprintf("trending:bucket:%d", nowSlot-int64(offset))
		values, err := s.rdb.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("read bucket %s: %w", key, err)
		}

		for query, rawCount := range values {
			count, err := strconv.Atoi(rawCount)
			if err != nil {
				return nil, time.Time{}, fmt.Errorf("parse count for %s: %w", query, err)
			}
			counts[query] += count
		}
	}

	items := topN(counts, defaultTopLimit)
	payload, err := json.Marshal(items)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("encode top cache: %w", err)
	}

	if err := s.rdb.Set(ctx, redisTopCacheKey, payload, s.topCacheTTL*2).Err(); err != nil {
		return nil, time.Time{}, fmt.Errorf("write top cache: %w", err)
	}

	s.metrics.SetStoreUniqueQueries(len(counts))

	return items, time.Now().UTC(), nil
}

func limitTop(items []TopItem, n int) []TopItem {
	if n <= 0 || n > len(items) {
		n = len(items)
	}

	result := make([]TopItem, n)
	copy(result, items[:n])

	return result
}
