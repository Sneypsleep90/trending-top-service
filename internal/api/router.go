package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter builds the HTTP router for the service.
func NewRouter(handler *Handler, logger *slog.Logger, gatherer prometheus.Gatherer) http.Handler {
	r := chi.NewRouter()
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(LoggingMiddleware(logger))
	r.Use(chimiddleware.Recoverer)

	r.Get("/healthz", handler.Healthz)
	r.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	r.Get("/api/v1/top", handler.GetTop)
	r.Post("/api/v1/stoplist", handler.AddStopWord)
	r.Delete("/api/v1/stoplist/{query}", handler.RemoveStopWord)
	r.Get("/api/v1/stoplist", handler.ListStopWords)

	return r
}
