package consumer

import (
	"context"
	"strings"
	"sync"
	"time"
)

type sessionEntry struct {
	mu        sync.Mutex
	counts    map[string]int
	expiresAt time.Time
}

// FraudDetector rate-limits identical queries from one session inside a time window.
type FraudDetector struct {
	sessions sync.Map
	maxCount int
	window   time.Duration
}

// NewFraudDetector creates a fraud detector.
func NewFraudDetector(maxCount int, window time.Duration) *FraudDetector {
	if maxCount <= 0 {
		maxCount = 50
	}
	if window <= 0 {
		window = time.Minute
	}

	return &FraudDetector{
		maxCount: maxCount,
		window:   window,
	}
}

// IsFraud increments a session/query counter and reports whether it exceeds the limit.
func (d *FraudDetector) IsFraud(sessionID string, query string) bool {
	sessionID = stringsTrim(sessionID)
	query = NormalizeQuery(query)
	if sessionID == "" || query == "" {
		return false
	}

	now := time.Now()
	value, _ := d.sessions.LoadOrStore(sessionID, &sessionEntry{
		counts:    make(map[string]int),
		expiresAt: now.Add(d.window),
	})
	entry := value.(*sessionEntry)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if now.After(entry.expiresAt) {
		clear(entry.counts)
		entry.expiresAt = now.Add(d.window)
	}

	entry.counts[query]++

	return entry.counts[query] > d.maxCount
}

// Run removes expired session entries until ctx is canceled.
func (d *FraudDetector) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.cleanup(time.Now())
		}
	}
}

func (d *FraudDetector) cleanup(now time.Time) {
	d.sessions.Range(func(key any, value any) bool {
		entry := value.(*sessionEntry)

		entry.mu.Lock()
		expired := now.After(entry.expiresAt)
		entry.mu.Unlock()

		if expired {
			d.sessions.Delete(key)
		}

		return true
	})
}

func stringsTrim(value string) string {
	return strings.TrimSpace(value)
}
