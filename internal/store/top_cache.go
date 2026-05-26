package store

import (
	"container/heap"
	"slices"
	"sync/atomic"
	"time"
)

const defaultTopLimit = 100

type cachedResult struct {
	Items       []TopItem
	GeneratedAt time.Time
}

type TopCache struct {
	wheel     *BucketWheel
	ttl       time.Duration
	limit     int
	onRebuild func(uniqueQueries int)
	value     atomic.Value
}

func NewTopCache(wheel *BucketWheel, ttl time.Duration, limit int, onRebuild func(uniqueQueries int)) *TopCache {
	if limit <= 0 {
		limit = defaultTopLimit
	}
	if ttl <= 0 {
		ttl = 200 * time.Millisecond
	}

	cache := &TopCache{
		wheel:     wheel,
		ttl:       ttl,
		limit:     limit,
		onRebuild: onRebuild,
	}
	cache.value.Store(&cachedResult{
		Items:       []TopItem{},
		GeneratedAt: time.Now().UTC(),
	})

	return cache
}

func (c *TopCache) Run(ctxDone <-chan struct{}) {
	c.Rebuild()

	ticker := time.NewTicker(c.ttl)
	defer ticker.Stop()

	for {
		select {
		case <-ctxDone:
			return
		case <-ticker.C:
			c.Rebuild()
		}
	}
}

func (c *TopCache) Rebuild() {
	counts := c.wheel.Counts()
	items := topN(counts, c.limit)
	if c.onRebuild != nil {
		c.onRebuild(len(counts))
	}

	c.value.Store(&cachedResult{
		Items:       items,
		GeneratedAt: time.Now().UTC(),
	})
}

func (c *TopCache) Top(n int) ([]TopItem, time.Time) {
	raw := c.value.Load()
	if raw == nil {
		return []TopItem{}, time.Now().UTC()
	}

	result := raw.(*cachedResult)
	if n <= 0 || n > len(result.Items) {
		n = len(result.Items)
	}

	items := make([]TopItem, n)
	copy(items, result.Items[:n])

	return items, result.GeneratedAt
}

type topItemHeap []TopItem

func (h topItemHeap) Len() int {
	return len(h)
}

func (h topItemHeap) Less(i int, j int) bool {
	if h[i].Count == h[j].Count {
		return h[i].Query > h[j].Query
	}

	return h[i].Count < h[j].Count
}

func (h topItemHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *topItemHeap) Push(x any) {
	*h = append(*h, x.(TopItem))
}

func (h *topItemHeap) Pop() any {
	old := *h
	item := old[len(old)-1]
	*h = old[:len(old)-1]

	return item
}

func topN(counts map[string]int, limit int) []TopItem {
	if limit <= 0 {
		return []TopItem{}
	}

	h := make(topItemHeap, 0, min(limit, len(counts)))
	for query, count := range counts {
		if count <= 0 {
			continue
		}

		item := TopItem{Query: query, Count: count}
		if h.Len() < limit {
			heap.Push(&h, item)
			continue
		}

		if betterThan(item, h[0]) {
			h[0] = item
			heap.Fix(&h, 0)
		}
	}

	items := make([]TopItem, len(h))
	copy(items, h)
	slices.SortFunc(items, func(a TopItem, b TopItem) int {
		if a.Count != b.Count {
			return b.Count - a.Count
		}
		if a.Query < b.Query {
			return -1
		}
		if a.Query > b.Query {
			return 1
		}

		return 0
	})

	return items
}

func betterThan(a TopItem, b TopItem) bool {
	if a.Count != b.Count {
		return a.Count > b.Count
	}

	return a.Query < b.Query
}
