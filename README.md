# trending-top-service

Backend service for the "Now searching" marketplace widget.

## Project Layout

```text
trending-top/
├── go.mod
├── go.sum
├── Makefile
├── docker-compose.yml
├── .golangci.yml
├── README.md
├── cmd/
│   ├── server/
│   │   └── main.go
│   └── producer/
│       └── main.go
└── internal/
    ├── config/
    │   └── config.go
    ├── consumer/
    │   ├── consumer.go
    │   ├── event.go
    │   └── fraud.go
    ├── store/
    │   ├── store.go
    │   ├── bucket_wheel.go
    │   ├── top_cache.go
    │   └── store_test.go
    ├── stoplist/
    │   ├── stoplist.go
    │   └── stoplist_test.go
    ├── api/
    │   ├── router.go
    │   ├── handler.go
    │   ├── middleware.go
    │   └── handler_test.go
    └── metrics/
        └── metrics.go
```

## Responsibilities

- `cmd/server` wires the service, starts goroutines, and handles graceful shutdown.
- `cmd/producer` sends test search events to Kafka.
- `internal/config` loads environment configuration.
- `internal/consumer` reads and validates Kafka search events.
- `internal/store` keeps the sliding window counters and top cache.
- `internal/stoplist` stores blocked queries.
- `internal/api` exposes HTTP routes and middleware.
- `internal/metrics` defines Prometheus metrics.

## Data Flow

```text
Kafka search.events
        |
        v
consumer.KafkaSource
        |
        v
SearchEvent decode and normalize
        |
        v
FraudDetector
        |
        v
StopList
        |
        v
BucketWheel
        |
        v
TopCache
        |
        v
HTTP GET /api/v1/top
```
