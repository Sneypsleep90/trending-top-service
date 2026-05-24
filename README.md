# trending-top

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
├── Dockerfile
├── migrations/
│   └── 001_create_stoplist.sql
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
    │   ├── memory.go
    │   ├── bucket_wheel.go
    │   ├── top_cache.go
    │   ├── redis.go
    │   └── store_test.go
    ├── stoplist/
    │   ├── stoplist.go
    │   ├── memory_stoplist.go
    │   ├── pg_stoplist.go
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
- `internal/store` provides memory and Redis backends for trending counters.
- `internal/stoplist` provides memory and Postgres-backed stop lists.
- `internal/api` exposes HTTP routes and middleware.
- `internal/metrics` defines Prometheus metrics.
- `migrations` contains the Postgres stop-list schema.

## API

- `GET /api/v1/top?n=10`
- `POST /api/v1/stoplist`
- `DELETE /api/v1/stoplist/{query}`
- `GET /api/v1/stoplist`
- `GET /healthz`
- `GET /metrics`

## Run

```bash
make docker-up
make produce
curl 'http://localhost:8080/api/v1/top?n=10'
```

For local Postgres, apply the migration manually:

```bash
make migrate
```

## Test

```bash
make test
```

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
