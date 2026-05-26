package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/Sneypsleep90/trending-top/internal/metrics"
	"github.com/Sneypsleep90/trending-top/internal/stoplist"
	"github.com/Sneypsleep90/trending-top/internal/store"
)

const (
	defaultTopLimit   = 10
	maxTopLimit       = 100
	maxStopQueryRunes = 100
)

type Handler struct {
	store         store.Store
	stopList      stoplist.StopList
	metrics       *metrics.Metrics
	logger        *slog.Logger
	windowSeconds int
}

func NewHandler(
	st store.Store,
	stopList stoplist.StopList,
	metricSet *metrics.Metrics,
	logger *slog.Logger,
	windowSeconds int,
) *Handler {
	return &Handler{
		store:         st,
		stopList:      stopList,
		metrics:       metricSet,
		logger:        logger,
		windowSeconds: windowSeconds,
	}
}

func (h *Handler) GetTop(w http.ResponseWriter, r *http.Request) {
	startedAt := time.Now()
	defer func() {
		h.metrics.ObserveTopRequest(time.Since(startedAt))
	}()

	n, err := parseTopLimit(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	items, generatedAt, err := h.store.Top(r.Context(), maxTopLimit)
	if err != nil {
		h.logger.Error("get top from store", slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, errors.New("store error"))
		return
	}

	filtered := make([]store.TopItem, 0, min(n, len(items)))
	for _, item := range items {
		blocked, err := h.stopList.Contains(r.Context(), item.Query)
		if err != nil {
			h.logger.Error("check stoplist", slog.Any("error", err), slog.String("query", item.Query))
			writeError(w, http.StatusInternalServerError, errors.New("stoplist error"))
			return
		}
		if blocked {
			h.metrics.IncStoplistDropped()
			continue
		}

		filtered = append(filtered, item)
		if len(filtered) == n {
			break
		}
	}

	writeJSON(w, http.StatusOK, topResponse{
		Items:         filtered,
		GeneratedAt:   generatedAt.UTC(),
		WindowSeconds: h.windowSeconds,
	})
}

func (h *Handler) AddStopWord(w http.ResponseWriter, r *http.Request) {
	var request stopWordRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid json: %w", err))
		return
	}

	query, err := validateStopQuery(request.Query)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if err := h.stopList.Add(r.Context(), query); err != nil {
		h.logger.Error("add stop word", slog.Any("error", err), slog.String("query", query))
		writeError(w, http.StatusInternalServerError, errors.New("stoplist error"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) RemoveStopWord(w http.ResponseWriter, r *http.Request) {
	query, err := validateStopQuery(chi.URLParam(r, "query"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if err := h.stopList.Remove(r.Context(), query); err != nil {
		h.logger.Error("remove stop word", slog.Any("error", err), slog.String("query", query))
		writeError(w, http.StatusInternalServerError, errors.New("stoplist error"))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ListStopWords(w http.ResponseWriter, r *http.Request) {
	items, err := h.stopList.List(r.Context())
	if err != nil {
		h.logger.Error("list stop words", slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, errors.New("stoplist error"))
		return
	}
	if items == nil {
		items = []string{}
	}

	writeJSON(w, http.StatusOK, stopListResponse{Items: items})
}

func (h *Handler) Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
}

type stopWordRequest struct {
	Query string `json:"query"`
}

type topResponse struct {
	Items         []store.TopItem `json:"items"`
	GeneratedAt   time.Time       `json:"generated_at"`
	WindowSeconds int             `json:"window_seconds"`
}

type stopListResponse struct {
	Items []string `json:"items"`
}

type healthResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func parseTopLimit(r *http.Request) (int, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("n"))
	if raw == "" {
		return defaultTopLimit, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("n must be an integer")
	}
	if n < 1 || n > maxTopLimit {
		return 0, fmt.Errorf("n must be between 1 and 100")
	}

	return n, nil
}

func validateStopQuery(query string) (string, error) {
	query = stoplist.NormalizeQuery(query)
	if query == "" {
		return "", fmt.Errorf("query must not be empty")
	}
	if utf8.RuneCountInString(query) > maxStopQueryRunes {
		return "", fmt.Errorf("query length must be at most 100 characters")
	}

	return query, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, errorResponse{Error: err.Error()})
}
