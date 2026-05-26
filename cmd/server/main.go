package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	httpapi "github.com/Sneypsleep90/trending-top/internal/api"
	"github.com/Sneypsleep90/trending-top/internal/config"
	"github.com/Sneypsleep90/trending-top/internal/consumer"
	"github.com/Sneypsleep90/trending-top/internal/metrics"
	"github.com/Sneypsleep90/trending-top/internal/stoplist"
	"github.com/Sneypsleep90/trending-top/internal/store"
)

func main() {
	if err := run(); err != nil {
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		logger.Error("service stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("main.run: %w", err)
	}

	logger := newLogger(cfg)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	metricSet := metrics.New(prometheus.NewRegistry())

	trendingStore, err := newStore(ctx, cfg, metricSet)
	if err != nil {
		return fmt.Errorf("main.run: %w", err)
	}
	defer closeStore(logger, trendingStore)

	stopList, err := newStopList(ctx, cfg)
	if err != nil {
		return fmt.Errorf("main.run: %w", err)
	}
	defer closeStopList(logger, stopList)

	fraud := consumer.NewFraudDetector(cfg.FraudMaxCount, cfg.FraudWindow())
	kafkaConsumer, err := consumer.NewConsumer(cfg, trendingStore, fraud, metricSet, logger)
	if err != nil {
		return fmt.Errorf("main.run: %w", err)
	}

	handler := httpapi.NewHandler(trendingStore, stopList, metricSet, logger, cfg.WindowSeconds())
	router := httpapi.NewRouter(handler, logger, metricSet.Registry())
	server := &http.Server{
		Addr:         cfg.HTTPAddr(),
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		trendingStore.Run(ctx)
	}()

	go func() {
		defer wg.Done()
		fraud.Run(ctx)
	}()

	go func() {
		defer wg.Done()
		kafkaConsumer.Run(ctx)
	}()

	go func() {
		defer wg.Done()
		logger.Info("http server started", slog.String("addr", cfg.HTTPAddr()))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", slog.Any("error", err))
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", slog.Any("error", err))
	}

	wg.Wait()
	logger.Info("server stopped gracefully")

	return nil
}

func newStore(ctx context.Context, cfg config.Config, metricSet *metrics.Metrics) (store.Store, error) {
	switch cfg.StoreBackend {
	case "redis":
		redisStore := store.NewRedisStore(cfg, metricSet)
		if err := redisStore.Ping(ctx); err != nil {
			return nil, fmt.Errorf("init redis store: %w", err)
		}

		return redisStore, nil
	case "memory":
		memoryStore, err := store.NewMemoryStore(cfg, metricSet)
		if err != nil {
			return nil, fmt.Errorf("init memory store: %w", err)
		}

		return memoryStore, nil
	default:
		return nil, fmt.Errorf("unsupported store backend %q", cfg.StoreBackend)
	}
}

func newStopList(ctx context.Context, cfg config.Config) (stoplist.StopList, error) {
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return stoplist.NewMemoryStopList(), nil
	}

	pgStopList, err := stoplist.NewPgStopList(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("init postgres stoplist: %w", err)
	}

	return pgStopList, nil
}

func closeStore(logger *slog.Logger, st store.Store) {
	closer, ok := st.(interface {
		Close() error
	})
	if !ok {
		return
	}

	if err := closer.Close(); err != nil {
		logger.Error("close store", slog.Any("error", err))
	}
}

func closeStopList(logger *slog.Logger, list stoplist.StopList) {
	closer, ok := list.(interface {
		Close(context.Context) error
	})
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := closer.Close(ctx); err != nil {
		logger.Error("close stoplist", slog.Any("error", err))
	}
}

func newLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	if cfg.LogLevel == "debug" {
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, opts))
}
