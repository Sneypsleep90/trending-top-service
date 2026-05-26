# trending-top

Production-like Go backend service for the marketplace widget "Сейчас ищут".

`trending-top` consumes search events from Kafka, keeps a rolling five-minute popularity window, filters blocked queries through a stop-list, and exposes a low-latency HTTP API for rendering the current trending searches.

![Go](https://img.shields.io/badge/Go-1.26.3-00ADD8)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-B32629)
![Redis](https://img.shields.io/badge/Redis-supported-DC382D)
![Postgres](https://img.shields.io/badge/Postgres-stoplist-336791)
![Prometheus](https://img.shields.io/badge/Prometheus-metrics-E6522C)

## What The Service Does

The service solves a common marketplace problem: show users what other people are searching for right now without recalculating heavy aggregates on every page request.

Main scenario:

1. A producer sends normalized search events to Kafka topic `search.events`.
2. The service consumes events with a Sarama consumer group.
3. Fraud protection drops repeated spam from the same session.
4. The selected store backend increments query counters inside a five-minute sliding window.
5. The HTTP API returns a precomputed top list in O(1) for the in-memory backend or from a short Redis cache for the Redis backend.
6. Stop-list entries are filtered before the response is returned to the widget.

## Architecture

```mermaid
flowchart LR
    Producer["Search event producer"] --> Kafka["Kafka / Redpanda topic search.events"]
    Kafka --> Consumer["Consumer group"]
    Consumer --> Decode["Decode, normalize, validate"]
    Decode --> Fraud["FraudDetector"]
    Fraud --> Store["Store interface"]
    Store --> Memory["MemoryStore: BucketWheel + TopCache"]
    Store --> Redis["RedisStore: hash buckets + JSON cache"]
    Postgres["Postgres stoplist table"] --> StopList["StopList cache"]
    StopList --> API["HTTP API"]
    Memory --> API
    Redis --> API
    API --> Widget["Marketplace widget"]
    API --> Prometheus["Prometheus /metrics"]
```

### Components

| Component | Responsibility | Implementation |
| --- | --- | --- |
| `cmd/server` | Wires dependencies, starts goroutines, handles graceful shutdown | `context`, `sync.WaitGroup`, `slog`, `http.Server` |
| `cmd/producer` | Generates local Kafka traffic for development | Sarama sync producer |
| `internal/config` | Loads and validates runtime settings | Environment variables with defaults |
| `internal/consumer` | Reads Kafka, validates events, applies fraud checks | IBM Sarama consumer group |
| `internal/store` | Stores counters and returns top queries | Memory or Redis backend behind one interface |
| `internal/stoplist` | Blocks unwanted queries | In-memory implementation or Postgres-backed cache |
| `internal/api` | Exposes HTTP API | chi router, structured logging middleware |
| `internal/metrics` | Owns service metrics | Prometheus registry and collectors |

### Data Flow

```text
Kafka event
  -> JSON decode
  -> trim + lowercase query
  -> required field validation
  -> session/query fraud rate limit
  -> Store.Add
  -> rolling window counters
  -> cached top list
  -> stop-list filtering
  -> GET /api/v1/top response
```

### Why This Architecture

The read path is optimized for the widget. A marketplace page can call `/api/v1/top` frequently, so the handler must not scan raw events or rebuild aggregates per request.

The service separates write-heavy ingestion from read-heavy delivery:

| Requirement | Decision | Result |
| --- | --- | --- |
| Low latency reads | Precomputed top cache | Fast widget response |
| Rolling five-minute window | Fixed bucket wheel or Redis time slots | Bounded memory and predictable cleanup |
| Horizontal scalability | Kafka consumer group and Redis backend | Multiple service replicas can share counters |
| Fast stop-list checks | `sync.Map` cache loaded from Postgres | O(1) reads without database calls |
| Observability | Prometheus metrics and structured logs | Easier debugging under load |

### Data Structures

`BucketWheel` is a fixed-size circular array of buckets. Each bucket stores `map[string]int32` protected by its own `sync.RWMutex`. With 30 buckets and 10 seconds per bucket, the service keeps a 300-second window. Rotation clears only one bucket at a time, so cleanup cost is bounded.

`TopCache` periodically rebuilds the top list from `BucketWheel.Counts()` every `TOP_CACHE_TTL_MS`. The result is stored in `atomic.Value`, so HTTP reads do not lock and do not sort.

`RedisStore` uses one Redis hash per time slot:

```text
trending:bucket:{unix_slot} -> query => count
trending:top_cache          -> JSON []TopItem
```

Redis keys have TTL, so old buckets naturally expire. The top cache is rebuilt lazily when missing.

`FraudDetector` stores per-session counters in `sync.Map`. Each session has a small map of query counters and an expiration timestamp. If one session sends the same query more than `FRAUD_MAX_COUNT` times inside `FRAUD_WINDOW_SEC`, the event is dropped.

`StopList` has two implementations. `MemoryStopList` is useful for tests and local fallback. `PgStopList` persists entries in Postgres and keeps an in-memory `sync.Map` cache for fast reads.

## Project Structure

```text
trending-top-service/
├── cmd/
│   ├── producer/
│   │   └── main.go
│   └── server/
│       └── main.go
├── internal/
│   ├── api/
│   │   ├── handler.go
│   │   ├── handler_test.go
│   │   ├── middleware.go
│   │   └── router.go
│   ├── config/
│   │   └── config.go
│   ├── consumer/
│   │   ├── consumer.go
│   │   ├── event.go
│   │   ├── fraud.go
│   │   └── fraud_test.go
│   ├── metrics/
│   │   └── metrics.go
│   ├── stoplist/
│   │   ├── memory_stoplist.go
│   │   ├── pg_stoplist.go
│   │   ├── stoplist.go
│   │   └── stoplist_test.go
│   └── store/
│       ├── bucket_wheel.go
│       ├── memory.go
│       ├── redis.go
│       ├── store.go
│       ├── store_test.go
│       └── top_cache.go
├── migrations/
│   └── 001_create_stoplist.sql
├── .dockerignore
├── .env.example
├── .golangci.yml
├── Dockerfile
├── Makefile
├── docker-compose.yml
├── go.mod
├── go.sum
└── README.md
```

## Local Run

### Requirements

| Tool | Version |
| --- | --- |
| Go | 1.26.3+ |
| Docker | 24+ |
| Docker Compose | v2+ |
| golangci-lint | 2.x |
| psql | Optional, only for manual migrations |

### Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated Kafka brokers |
| `KAFKA_TOPIC` | `search.events` | Kafka topic with search events |
| `KAFKA_GROUP_ID` | `trending-top` | Consumer group ID |
| `HTTP_PORT` | `8080` | HTTP listen port |
| `BUCKET_COUNT` | `30` | Number of rolling buckets |
| `BUCKET_DURATION_SEC` | `10` | Duration of one bucket |
| `TOP_CACHE_TTL_MS` | `200` | Top cache rebuild interval |
| `FRAUD_MAX_COUNT` | `50` | Max identical query count per session window |
| `FRAUD_WINDOW_SEC` | `60` | Fraud detection window |
| `STORE_BACKEND` | `memory` | `memory` or `redis` |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `REDIS_PASSWORD` | empty | Redis password |
| `REDIS_DB` | `0` | Redis database |
| `DATABASE_URL` | local Postgres DSN | Post-list Postgres DSN |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

### Docker Compose

```bash
git clone https://github.com/Sneypsleep90/trending-top.git
cd trending-top
cp .env.example .env
docker compose up --build
```

The compose stack starts:

| Service | Port | Purpose |
| --- | --- | --- |
| `redpanda` | `9092` | Kafka-compatible broker |
| `redis` | `6379` | Shared counter backend |
| `postgres` | `5432` | Stop-list persistence |
| `trending-top` | `8080` | HTTP API and Kafka consumer |

Generate test traffic:

```bash
make produce
```

Read the top list:

```bash
curl 'http://localhost:8080/api/v1/top?n=10'
```

### Local Process Run

```bash
go mod download
make run
```

For Redis-backed local run:

```bash
export STORE_BACKEND=redis
export REDIS_ADDR=localhost:6379
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/trending?sslmode=disable'
make run
```

Apply the stop-list migration manually when running Postgres outside Compose:

```bash
make migrate DATABASE_URL='postgres://postgres:postgres@localhost:5432/trending?sslmode=disable'
```

## API Examples

### Health

```bash
curl -i 'http://localhost:8080/healthz'
```

```json
{
  "status": "ok"
}
```

### Get Trending Top

```bash
curl -i 'http://localhost:8080/api/v1/top?n=2'
```

```json
{
  "items": [
    {
      "query": "кроссовки найк",
      "count": 4231
    },
    {
      "query": "айфон 15",
      "count": 3187
    }
  ],
  "generated_at": "2026-05-26T18:44:27.526930012Z",
  "window_seconds": 300
}
```

### Add Stop-List Query

```bash
curl -i -X POST 'http://localhost:8080/api/v1/stoplist' \
  -H 'Content-Type: application/json' \
  -d '{"query":"кроссовки найк"}'
```

Expected response:

```text
HTTP/1.1 204 No Content
```

### List Stop-List Queries

```bash
curl -i 'http://localhost:8080/api/v1/stoplist'
```

```json
{
  "items": [
    "кроссовки найк"
  ]
}
```

### Remove Stop-List Query

```bash
curl -i -X DELETE 'http://localhost:8080/api/v1/stoplist/кроссовки%20найк'
```

Expected response:

```text
HTTP/1.1 204 No Content
```

### Metrics

```bash
curl 'http://localhost:8080/metrics'
```

Important metrics:

| Metric | Meaning |
| --- | --- |
| `trending_events_consumed_total` | Accepted Kafka events |
| `trending_fraud_dropped_total` | Events dropped by fraud detector |
| `trending_stoplist_dropped_total` | Top items hidden by stop-list filtering |
| `trending_kafka_errors_total` | Kafka consume and decode errors |
| `trending_store_writes_total` | Writes to the selected store |
| `trending_store_unique_queries` | Current unique query count in the window |
| `trending_top_request_duration_seconds` | Top endpoint latency histogram |

### Error Examples

Invalid `n`:

```bash
curl -i 'http://localhost:8080/api/v1/top?n=0'
```

```json
{
  "error": "n must be between 1 and 100"
}
```

Empty stop-list query:

```bash
curl -i -X POST 'http://localhost:8080/api/v1/stoplist' \
  -H 'Content-Type: application/json' \
  -d '{"query":"   "}'
```

```json
{
  "error": "query must not be empty"
}
```

### Status Codes

| Code | Meaning |
| --- | --- |
| `200` | Successful JSON response |
| `204` | Stop-list mutation completed |
| `400` | Invalid request parameters or payload |
| `500` | Store or stop-list backend error |

## Kafka Data Contract

Topic: `search.events`

Payload:

```json
{
  "query": "кроссовки найк",
  "timestamp": "2024-01-15T14:23:01.123Z",
  "session_id": "a1b2c3d4",
  "region": "RU-MOW"
}
```

| Field | Required | Purpose |
| --- | --- | --- |
| `query` | Yes | Search phrase used as the counter key |
| `timestamp` | Yes | Event time for validation and future extensions |
| `session_id` | Yes | Fraud detection key |
| `region` | No | Reserved for regional tops |

Critical business fields are `query`, `timestamp`, and `session_id`. Invalid or empty values are rejected before the event reaches the store.

## Trade-offs And Business Logic

| Trade-off | Decision | Why |
| --- | --- | --- |
| Speed vs exact freshness | Top is refreshed every 200 ms by default | Small staleness gives stable low-latency reads |
| Memory vs latency | Keep counters in bounded buckets and cache top results | Avoid per-request aggregation |
| Realtime vs consistency | Redis backend uses short-lived cache and time-slot hashes | Works across replicas with eventual top freshness |
| Simplicity vs perfect event-time windows | Current stores increment current processing-time bucket | Lower complexity for the widget scenario |
| Database durability vs read speed | Stop-list writes go to Postgres, reads hit memory cache | Admin changes persist while user traffic stays fast |

Known limitations:

| Limitation | Current behavior | Possible extension |
| --- | --- | --- |
| Region is not used | One global top list | Add region to store keys |
| No auth on stop-list endpoints | Intended for trusted internal network | Add admin auth middleware |
| Redis rebuild is lazy | First request after cache miss does aggregation | Add background rebuild for Redis |
| Kafka topic is assumed to exist | Redpanda can auto-create locally | Manage topics through infrastructure |

## Quality Gates

```bash
make test
make lint
make bench
```

`make test` runs all tests with the race detector.

## Useful Commands

```bash
make docker-up
make produce
make test
make lint
make docker-down
```
