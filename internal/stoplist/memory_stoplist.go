package stoplist

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

// MemoryStopList is an in-memory StopList implementation.
type MemoryStopList struct {
	items sync.Map
}

// NewMemoryStopList creates an empty in-memory stop list.
func NewMemoryStopList() *MemoryStopList {
	return &MemoryStopList{}
}

// Add inserts query into the stop list.
func (s *MemoryStopList) Add(ctx context.Context, query string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory_stoplist.Add: %w", err)
	}

	query = NormalizeQuery(query)
	if query == "" {
		return fmt.Errorf("memory_stoplist.Add: query is empty")
	}

	s.items.Store(query, struct{}{})

	return nil
}

// Remove deletes query from the stop list.
func (s *MemoryStopList) Remove(ctx context.Context, query string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memory_stoplist.Remove: %w", err)
	}

	query = NormalizeQuery(query)
	if query == "" {
		return fmt.Errorf("memory_stoplist.Remove: query is empty")
	}

	s.items.Delete(query)

	return nil
}

// Contains reports whether query exists in the stop list.
func (s *MemoryStopList) Contains(ctx context.Context, query string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("memory_stoplist.Contains: %w", err)
	}

	query = NormalizeQuery(query)
	if query == "" {
		return false, nil
	}

	_, ok := s.items.Load(query)

	return ok, nil
}

// List returns all stop-list queries sorted lexicographically.
func (s *MemoryStopList) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memory_stoplist.List: %w", err)
	}

	items := make([]string, 0)
	s.items.Range(func(key any, _ any) bool {
		items = append(items, key.(string))
		return true
	})
	slices.Sort(items)

	return items, nil
}
