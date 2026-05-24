package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// LoggingMiddleware logs request method, path, status, duration, and request id.
func LoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			startedAt := time.Now()
			recorder := &statusRecorder{
				ResponseWriter: w,
				status:         http.StatusOK,
			}

			next.ServeHTTP(recorder, r)

			duration := time.Since(startedAt)
			status := recorder.status
			attrs := []slog.Attr{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Duration("duration", duration),
				slog.String("request_id", middleware.GetReqID(r.Context())),
			}

			switch {
			case status >= 500:
				logger.LogAttrs(r.Context(), slog.LevelError, "http request", attrs...)
			case status >= 400:
				logger.LogAttrs(r.Context(), slog.LevelWarn, "http request", attrs...)
			default:
				logger.LogAttrs(r.Context(), slog.LevelInfo, "http request", attrs...)
			}
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}
