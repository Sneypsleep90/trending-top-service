package stoplist

import (
	"context"
	"strings"
)

type StopList interface {
	Add(ctx context.Context, query string) error
	Remove(ctx context.Context, query string) error
	Contains(ctx context.Context, query string) (bool, error)
	List(ctx context.Context) ([]string, error)
}

func NormalizeQuery(query string) string {
	return strings.TrimSpace(strings.ToLower(query))
}
