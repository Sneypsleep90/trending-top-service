package store

import (
	"context"
	"time"
)

type Store interface {
	Add(ctx context.Context, query string) error
	Top(ctx context.Context, n int) ([]TopItem, time.Time, error)
	Run(ctx context.Context)
}

type TopItem struct {
	Query string `json:"query"`
	Count int    `json:"count"`
}
