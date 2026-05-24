package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Sneypsleep90/trending-top/internal/metrics"
	"github.com/Sneypsleep90/trending-top/internal/stoplist"
	"github.com/Sneypsleep90/trending-top/internal/store"
)

type mockStore struct {
	items       []store.TopItem
	generatedAt time.Time
}

func (s *mockStore) Add(_ context.Context, _ string) error {
	return nil
}

func (s *mockStore) Top(_ context.Context, _ int) ([]store.TopItem, time.Time, error) {
	items := make([]store.TopItem, len(s.items))
	copy(items, s.items)

	return items, s.generatedAt, nil
}

func (s *mockStore) Run(_ context.Context) {}

func TestGetTop_Empty(t *testing.T) {
	router := newTestRouter(&mockStore{generatedAt: time.Date(2024, 1, 15, 14, 23, 5, 0, time.UTC)}, stoplist.NewMemoryStopList())

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/top", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}

	var response topResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(response.Items) != 0 {
		t.Fatalf("expected empty items, got %#v", response.Items)
	}
}

func TestGetTop_WithData(t *testing.T) {
	router := newTestRouter(&mockStore{
		generatedAt: time.Date(2024, 1, 15, 14, 23, 5, 0, time.UTC),
		items: []store.TopItem{
			{Query: "кроссовки найк", Count: 4231},
			{Query: "айфон 15", Count: 3187},
		},
	}, stoplist.NewMemoryStopList())

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/top?n=1", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}

	var response topResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(response.Items))
	}
	if response.Items[0].Query != "кроссовки найк" || response.Items[0].Count != 4231 {
		t.Fatalf("unexpected item: %#v", response.Items[0])
	}
}

func TestGetTop_StopListFilter(t *testing.T) {
	stopList := stoplist.NewMemoryStopList()
	if err := stopList.Add(context.Background(), "кроссовки найк"); err != nil {
		t.Fatalf("Add stop word: %v", err)
	}

	router := newTestRouter(&mockStore{
		generatedAt: time.Now(),
		items: []store.TopItem{
			{Query: "кроссовки найк", Count: 4231},
			{Query: "айфон 15", Count: 3187},
		},
	}, stopList)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/top?n=1", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}

	var response topResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(response.Items))
	}
	if response.Items[0].Query != "айфон 15" {
		t.Fatalf("unexpected item after filtering: %#v", response.Items[0])
	}
}

func TestGetTop_InvalidN(t *testing.T) {
	router := newTestRouter(&mockStore{generatedAt: time.Now()}, stoplist.NewMemoryStopList())

	for _, path := range []string{"/api/v1/top?n=0", "/api/v1/top?n=200", "/api/v1/top?n=bad"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		router.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("path %s: expected 400, got %d", path, recorder.Code)
		}
	}
}

func TestAddStopWord(t *testing.T) {
	stopList := stoplist.NewMemoryStopList()
	router := newTestRouter(&mockStore{generatedAt: time.Now()}, stopList)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/stoplist", strings.NewReader(`{"query":" кроссовки найк "}`))
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", recorder.Code)
	}

	contains, err := stopList.Contains(context.Background(), "кроссовки найк")
	if err != nil {
		t.Fatalf("Contains: %v", err)
	}
	if !contains {
		t.Fatal("expected stop word to be stored")
	}
}

func TestAddStopWord_EmptyQuery(t *testing.T) {
	router := newTestRouter(&mockStore{generatedAt: time.Now()}, stoplist.NewMemoryStopList())

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/stoplist", strings.NewReader(`{"query":"   "}`))
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", recorder.Code)
	}
}

func newTestRouter(st store.Store, stopList stoplist.StopList) http.Handler {
	metricSet := metrics.New(prometheus.NewRegistry())
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	handler := NewHandler(st, stopList, metricSet, logger, 300)

	return NewRouter(handler, logger, metricSet.Registry())
}
