package store

import (
	"context"
	"time"
)

// Store accepts search queries and returns a precomputed trending top.
type Store interface {
	Add(ctx context.Context, query string) error
	Top(ctx context.Context, n int) ([]TopItem, time.Time, error)
	Run(ctx context.Context)
}

// TopItem is one row in the trending top response.
type TopItem struct {
	Query string `json:"query"`
	Count int    `json:"count"`
}
