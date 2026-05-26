package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultKafkaBrokers      = "localhost:9092"
	defaultKafkaTopic        = "search.events"
	defaultKafkaGroupID      = "trending-top"
	defaultHTTPPort          = 8080
	defaultBucketCount       = 30
	defaultBucketDurationSec = 10
	defaultTopCacheTTLMS     = 200
	defaultFraudMaxCount     = 50
	defaultFraudWindowSec    = 60
	defaultStoreBackend      = "memory"
	defaultRedisAddr         = "localhost:6379"
	defaultRedisPassword     = ""
	defaultRedisDB           = 0
	defaultDatabaseURL       = "postgres://postgres:postgres@localhost:5432/trending?sslmode=disable"
	defaultLogLevel          = "info"
)

type Config struct {
	KafkaBrokers      string
	KafkaTopic        string
	KafkaGroupID      string
	HTTPPort          int
	BucketCount       int
	BucketDurationSec int
	TopCacheTTLMS     int
	FraudMaxCount     int
	FraudWindowSec    int
	StoreBackend      string
	RedisAddr         string
	RedisPassword     string
	RedisDB           int
	DatabaseURL       string
	LogLevel          string
}

func Load() (Config, error) {
	cfg := Config{
		KafkaBrokers:      getString("KAFKA_BROKERS", defaultKafkaBrokers),
		KafkaTopic:        getString("KAFKA_TOPIC", defaultKafkaTopic),
		KafkaGroupID:      getString("KAFKA_GROUP_ID", defaultKafkaGroupID),
		HTTPPort:          getInt("HTTP_PORT", defaultHTTPPort),
		BucketCount:       getInt("BUCKET_COUNT", defaultBucketCount),
		BucketDurationSec: getInt("BUCKET_DURATION_SEC", defaultBucketDurationSec),
		TopCacheTTLMS:     getInt("TOP_CACHE_TTL_MS", defaultTopCacheTTLMS),
		FraudMaxCount:     getInt("FRAUD_MAX_COUNT", defaultFraudMaxCount),
		FraudWindowSec:    getInt("FRAUD_WINDOW_SEC", defaultFraudWindowSec),
		StoreBackend:      strings.ToLower(getString("STORE_BACKEND", defaultStoreBackend)),
		RedisAddr:         getString("REDIS_ADDR", defaultRedisAddr),
		RedisPassword:     getString("REDIS_PASSWORD", defaultRedisPassword),
		RedisDB:           getInt("REDIS_DB", defaultRedisDB),
		DatabaseURL:       getString("DATABASE_URL", defaultDatabaseURL),
		LogLevel:          strings.ToLower(getString("LOG_LEVEL", defaultLogLevel)),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("config.Load: %w", err)
	}

	return cfg, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.KafkaBrokers) == "" {
		return fmt.Errorf("KAFKA_BROKERS must not be empty")
	}
	if strings.TrimSpace(c.KafkaTopic) == "" {
		return fmt.Errorf("KAFKA_TOPIC must not be empty")
	}
	if strings.TrimSpace(c.KafkaGroupID) == "" {
		return fmt.Errorf("KAFKA_GROUP_ID must not be empty")
	}
	if c.HTTPPort <= 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("HTTP_PORT must be between 1 and 65535")
	}
	if c.BucketCount <= 0 {
		return fmt.Errorf("BUCKET_COUNT must be positive")
	}
	if c.BucketDurationSec <= 0 {
		return fmt.Errorf("BUCKET_DURATION_SEC must be positive")
	}
	if c.TopCacheTTLMS <= 0 {
		return fmt.Errorf("TOP_CACHE_TTL_MS must be positive")
	}
	if c.FraudMaxCount <= 0 {
		return fmt.Errorf("FRAUD_MAX_COUNT must be positive")
	}
	if c.FraudWindowSec <= 0 {
		return fmt.Errorf("FRAUD_WINDOW_SEC must be positive")
	}
	if c.StoreBackend != "memory" && c.StoreBackend != "redis" {
		return fmt.Errorf("STORE_BACKEND must be memory or redis")
	}
	if c.StoreBackend == "redis" && strings.TrimSpace(c.RedisAddr) == "" {
		return fmt.Errorf("REDIS_ADDR must not be empty when STORE_BACKEND=redis")
	}

	return nil
}

func (c Config) KafkaBrokerList() []string {
	parts := strings.Split(c.KafkaBrokers, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	return brokers
}

func (c Config) HTTPAddr() string {
	return fmt.Sprintf(":%d", c.HTTPPort)
}

func (c Config) BucketDuration() time.Duration {
	return time.Duration(c.BucketDurationSec) * time.Second
}

func (c Config) WindowDuration() time.Duration {
	return time.Duration(c.BucketCount*c.BucketDurationSec) * time.Second
}

func (c Config) WindowSeconds() int {
	return c.BucketCount * c.BucketDurationSec
}

func (c Config) TopCacheTTL() time.Duration {
	return time.Duration(c.TopCacheTTLMS) * time.Millisecond
}

func (c Config) FraudWindow() time.Duration {
	return time.Duration(c.FraudWindowSec) * time.Second
}

func getString(key string, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	return value
}

func getInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}
