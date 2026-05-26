package stoplist

import (
	"context"
	"fmt"
	"slices"
	"sync"
)

type MemoryStopList struct {
	items sync.Map
}

func NewMemoryStopList() *MemoryStopList {
	return &MemoryStopList{}
}

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
