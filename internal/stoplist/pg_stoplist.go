package stoplist

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/jackc/pgx/v5"
)

type PgStopList struct {
	db    *pgx.Conn
	cache sync.Map
	mu    sync.Mutex
}

func NewPgStopList(ctx context.Context, dsn string) (*PgStopList, error) {
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pg_stoplist.NewPgStopList: %w", err)
	}

	stopList := &PgStopList{db: db}
	if err := stopList.load(ctx); err != nil {
		closeErr := db.Close(ctx)
		if closeErr != nil {
			return nil, fmt.Errorf("pg_stoplist.NewPgStopList: load: %w; close: %w", err, closeErr)
		}

		return nil, fmt.Errorf("pg_stoplist.NewPgStopList: %w", err)
	}

	return stopList, nil
}

func (s *PgStopList) Add(ctx context.Context, query string) error {
	query = NormalizeQuery(query)
	if query == "" {
		return fmt.Errorf("pg_stoplist.Add: query is empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(ctx, `INSERT INTO stoplist (query) VALUES ($1) ON CONFLICT (query) DO NOTHING`, query)
	if err != nil {
		return fmt.Errorf("pg_stoplist.Add: %w", err)
	}

	s.cache.Store(query, struct{}{})

	return nil
}

func (s *PgStopList) Remove(ctx context.Context, query string) error {
	query = NormalizeQuery(query)
	if query == "" {
		return fmt.Errorf("pg_stoplist.Remove: query is empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(ctx, `DELETE FROM stoplist WHERE query = $1`, query)
	if err != nil {
		return fmt.Errorf("pg_stoplist.Remove: %w", err)
	}

	s.cache.Delete(query)

	return nil
}

func (s *PgStopList) Contains(ctx context.Context, query string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("pg_stoplist.Contains: %w", err)
	}

	query = NormalizeQuery(query)
	if query == "" {
		return false, nil
	}

	_, ok := s.cache.Load(query)

	return ok, nil
}

func (s *PgStopList) List(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pg_stoplist.List: %w", err)
	}

	items := make([]string, 0)
	s.cache.Range(func(key any, _ any) bool {
		items = append(items, key.(string))
		return true
	})
	slices.Sort(items)

	return items, nil
}

func (s *PgStopList) Close(ctx context.Context) error {
	if err := s.db.Close(ctx); err != nil {
		return fmt.Errorf("pg_stoplist.Close: %w", err)
	}

	return nil
}

func (s *PgStopList) load(ctx context.Context) error {
	rows, err := s.db.Query(ctx, `SELECT query FROM stoplist`)
	if err != nil {
		return fmt.Errorf("load stoplist: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var query string
		if err := rows.Scan(&query); err != nil {
			return fmt.Errorf("scan query: %w", err)
		}
		s.cache.Store(NormalizeQuery(query), struct{}{})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows: %w", err)
	}

	return nil
}
