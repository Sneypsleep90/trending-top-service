package stoplist

import (
	"context"
	"strings"
)

// StopList stores blocked search queries.
type StopList interface {
	Add(ctx context.Context, query string) error
	Remove(ctx context.Context, query string) error
	Contains(ctx context.Context, query string) (bool, error)
	List(ctx context.Context) ([]string, error)
}

// NormalizeQuery normalizes stop-list queries in the same way as events.
func NormalizeQuery(query string) string {
	return strings.TrimSpace(strings.ToLower(query))
}
