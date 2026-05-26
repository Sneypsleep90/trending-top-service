# trending-top

Production-like backend-сервис на Go для маркетплейс-виджета «Сейчас ищут».

`trending-top` читает события поисковых запросов из Kafka, считает популярные запросы в скользящем окне, фильтрует нежелательные фразы через stop-list и отдает быстрый HTTP API для фронтенд-виджета.

![Go](https://img.shields.io/badge/Go-1.26.3-00ADD8)
![Kafka](https://img.shields.io/badge/Kafka-Redpanda-B32629)
![Redis](https://img.shields.io/badge/Redis-supported-DC382D)
![Postgres](https://img.shields.io/badge/Postgres-stoplist-336791)
![Prometheus](https://img.shields.io/badge/Prometheus-metrics-E6522C)

## Что Делает Сервис

Сервис решает задачу отображения актуальных популярных поисковых запросов на маркетплейсе. Вместо тяжелого пересчета статистики на каждый запрос страницы сервис заранее агрегирует события и держит готовый топ.

Основной сценарий:

1. Продюсер отправляет нормализованные поисковые события в Kafka topic `search.events`.
2. Сервис читает Kafka через Sarama consumer group.
3. Невалидные события и подозрительные повторы от одной сессии отбрасываются.
4. Принятые query записываются в выбранное хранилище: `memory` или `redis`.
5. Топ запросов хранится в готовом виде и быстро отдается через `GET /api/v1/top`.
6. Перед ответом API фильтрует запросы, которые попали в stop-list.

## Архитектура

```mermaid
flowchart LR
    Producer["cmd/producer"] --> Kafka["Kafka / Redpanda search.events"]
    Kafka --> Consumer["internal/consumer"]
    Consumer --> Decode["decode + normalize + validate"]
    Decode --> Fraud["FraudDetector"]
    Fraud --> Store["Store interface"]
    Store --> Memory["MemoryStore: BucketWheel + TopCache"]
    Store --> Redis["RedisStore: Redis hashes + JSON cache"]
    Postgres["Postgres stoplist"] --> StopList["StopList cache"]
    Memory --> API["HTTP API"]
    Redis --> API
    StopList --> API
    API --> Widget["Marketplace widget"]
    API --> Metrics["Prometheus /metrics"]
```

### Компоненты

| Компонент | За что отвечает | Реализация |
| --- | --- | --- |
| `cmd/server` | Инициализация зависимостей, запуск горутин, graceful shutdown | `context`, `sync.WaitGroup`, `slog`, `http.Server` |
| `cmd/producer` | Генератор тестовых Kafka-событий | Sarama sync producer |
| `internal/config` | Загрузка и валидация env-конфига | `os.Getenv`, дефолты, typed helpers |
| `internal/consumer` | Чтение Kafka, decode, validation, fraud check | IBM Sarama consumer group |
| `internal/store` | Подсчет query и выдача топа | `MemoryStore` или `RedisStore` за одним интерфейсом |
| `internal/stoplist` | Блокировка нежелательных запросов | `sync.Map`, Postgres persistence |
| `internal/api` | HTTP API, JSON responses, middleware | chi router |
| `internal/metrics` | Метрики сервиса | Prometheus registry |

### Поток Данных

```text
Kafka JSON event
  -> DecodeSearchEvent
  -> normalize query
  -> validate required fields
  -> FraudDetector.IsFraud
  -> Store.Add
  -> rolling counters
  -> cached top
  -> stop-list filter
  -> GET /api/v1/top response
```

### Почему Так

Главная нагрузка ожидается на чтение топа виджетом. Поэтому `/api/v1/top` не пересчитывает агрегаты на каждый HTTP-запрос. Сервис разделяет ingestion path и read path: consumer постоянно принимает события, а API читает уже подготовленный результат.

| Требование | Решение | Что это дает |
| --- | --- | --- |
| Быстрые ответы виджету | Предрассчитанный top cache | Низкая latency на `/top` |
| Скользящее окно 5 минут | Bucket wheel или Redis time slots | Ограниченная память и понятная очистка |
| Горизонтальное масштабирование | Kafka consumer group + Redis backend | Можно запускать несколько реплик |
| Быстрый stop-list | In-memory cache поверх Postgres | Проверка без похода в БД |
| Наблюдаемость | Prometheus + structured logs | Проще искать проблемы в работе |

### Структуры Данных

`BucketWheel` — кольцевой буфер из бакетов. По умолчанию используется 30 бакетов по 10 секунд, то есть окно равно 300 секунд. Каждый бакет хранит `map[string]int32` и имеет свой `sync.RWMutex`. При ротации очищается только следующий бакет, поэтому очистка старых данных предсказуемая и дешевая.

`TopCache` пересчитывает топ каждые `TOP_CACHE_TTL_MS`. Он берет snapshot из `BucketWheel.Counts()`, строит top-100 через min-heap и кладет результат в `atomic.Value`. HTTP handler читает готовый slice без сортировки и без блокировок на hot path.

`RedisStore` нужен для запуска нескольких реплик. Он хранит счетчики в Redis hash-ключах по временным слотам:

```text
trending:bucket:{unix_slot} -> query => count
trending:top_cache          -> JSON []TopItem
```

У bucket-ключей есть TTL, поэтому старые слоты удаляются автоматически. Top cache в Redis пересобирается lazy-образом при cache miss.

`FraudDetector` хранит счетчики `session_id + query` в `sync.Map`. Если одна сессия отправила один и тот же query больше `FRAUD_MAX_COUNT` раз за `FRAUD_WINDOW_SEC`, событие считается подозрительным и не попадает в store.

`StopList` имеет две реализации. `MemoryStopList` используется для тестов и fallback-сценариев. `PgStopList` сохраняет данные в Postgres, но читает `Contains` только из in-memory cache, чтобы фильтрация `/top` оставалась быстрой.

## Структура Проекта

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

### Что Лежит В Основных Файлах

| Файл | Назначение |
| --- | --- |
| `cmd/server/main.go` | Wire компонентов, выбор backend-ов, запуск HTTP/Kafka/store/fraud goroutines, graceful shutdown |
| `cmd/producer/main.go` | Локальный генератор поисковых событий в Kafka |
| `internal/config/config.go` | Env config, defaults, validation, duration helpers |
| `internal/consumer/event.go` | Kafka payload, JSON decode, normalize, validate |
| `internal/consumer/fraud.go` | Rate limit одинаковых query по `session_id` |
| `internal/consumer/consumer.go` | Sarama ConsumerGroup и обработка Kafka messages |
| `internal/store/store.go` | Общий интерфейс `Store` и DTO `TopItem` |
| `internal/store/bucket_wheel.go` | In-memory sliding window |
| `internal/store/top_cache.go` | Atomic cache готового топа |
| `internal/store/memory.go` | Memory backend поверх `BucketWheel + TopCache` |
| `internal/store/redis.go` | Redis backend для shared counters |
| `internal/stoplist/stoplist.go` | Общий интерфейс `StopList` |
| `internal/stoplist/memory_stoplist.go` | In-memory stop-list |
| `internal/stoplist/pg_stoplist.go` | Postgres stop-list с in-memory cache |
| `internal/api/router.go` | HTTP routes |
| `internal/api/handler.go` | Business handlers |
| `internal/api/middleware.go` | Structured request logging |
| `internal/metrics/metrics.go` | Prometheus metrics |
| `migrations/001_create_stoplist.sql` | Таблица `stoplist` |

## Локальный Запуск

### Требования

| Инструмент | Версия |
| --- | --- |
| Go | 1.26.3+ |
| Docker | 24+ |
| Docker Compose | v2+ |
| golangci-lint | 2.x |
| psql | Опционально, для ручного применения миграции |

### Переменные Окружения

| Переменная | Дефолт | Описание |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka brokers через запятую |
| `KAFKA_TOPIC` | `search.events` | Topic поисковых событий |
| `KAFKA_GROUP_ID` | `trending-top` | Consumer group |
| `HTTP_PORT` | `8080` | HTTP port |
| `BUCKET_COUNT` | `30` | Количество бакетов окна |
| `BUCKET_DURATION_SEC` | `10` | Длина одного бакета |
| `TOP_CACHE_TTL_MS` | `200` | Интервал пересчета top cache |
| `FRAUD_MAX_COUNT` | `50` | Максимум одинаковых query от сессии |
| `FRAUD_WINDOW_SEC` | `60` | Окно fraud detector |
| `STORE_BACKEND` | `memory` | `memory` или `redis` |
| `REDIS_ADDR` | `localhost:6379` | Адрес Redis |
| `REDIS_PASSWORD` | empty | Пароль Redis |
| `REDIS_DB` | `0` | Номер Redis DB |
| `DATABASE_URL` | local Postgres DSN | DSN для Postgres stop-list |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

### Запуск Через Docker Compose

```bash
git clone https://github.com/Sneypsleep90/trending-top.git
cd trending-top
cp .env.example .env
docker compose up --build
```

Compose поднимает:

| Service | Port | Назначение |
| --- | --- | --- |
| `redpanda` | `9092` | Kafka-compatible broker |
| `redis` | `6379` | Shared store backend |
| `postgres` | `5432` | Persistence для stop-list |
| `trending-top` | `8080` | HTTP API и Kafka consumer |

### Создание Kafka Topic

В локальном compose-стенде Redpanda может создать topic автоматически при первой записи. Для production-like запуска лучше создать topic явно:

```bash
docker compose exec -T redpanda rpk topic create search.events \
  --partitions 1 \
  --replicas 1
```

Если topic уже существует, Redpanda вернет ошибку о существующем topic. Это нормально, повторно создавать его не нужно.

Проверить, что topic существует:

```bash
docker compose exec -T redpanda rpk topic list
```

Посмотреть описание topic:

```bash
docker compose exec -T redpanda rpk topic describe search.events
```

Сгенерировать тестовые события:

```bash
make produce
```

Получить топ:

```bash
curl 'http://localhost:8080/api/v1/top?n=10'
```

### Быстрый Smoke Test

```bash
curl 'http://localhost:8080/healthz'
curl 'http://localhost:8080/api/v1/top?n=10'
curl 'http://localhost:8080/api/v1/stoplist'
curl 'http://localhost:8080/metrics'
```

Ожидаемое поведение:

| Команда | Ожидаемый результат |
| --- | --- |
| `GET /healthz` | `{"status":"ok"}` |
| `GET /api/v1/top?n=10` | JSON с `items`, `generated_at`, `window_seconds` |
| `GET /api/v1/stoplist` | JSON со списком stop-list queries |
| `GET /metrics` | Prometheus metrics |

### Локальный Запуск Без Docker-Образа

```bash
go mod download
make run
```

Для запуска с Redis backend:

```bash
export STORE_BACKEND=redis
export REDIS_ADDR=localhost:6379
export DATABASE_URL='postgres://postgres:postgres@localhost:5432/trending?sslmode=disable'
make run
```

Миграция stop-list применяется вручную:

```bash
make migrate DATABASE_URL='postgres://postgres:postgres@localhost:5432/trending?sslmode=disable'
```

## Как Проверить Корректность

Этот сценарий проверяет не только HTTP-статусы, а саму бизнес-логику: Kafka ingestion, нормализацию query, подсчет top, сортировку, Redis counters и stop-list filtering.

Подготовить чистое тестовое состояние:

```bash
docker compose exec -T redis sh -c 'redis-cli --scan --pattern "trending:*" | xargs -r redis-cli DEL'
docker compose exec -T postgres psql -U postgres -d trending -c 'TRUNCATE stoplist;'
docker compose restart trending-top
```

Отправить детерминированные события в Kafka:

```bash
{
  for i in $(seq 1 12); do
    printf '{"query":"  CODEX Alpha  ","timestamp":"2026-05-26T19:05:00.000Z","session_id":"sem-alpha-%s","region":"RU-MOW"}\n' "$i"
  done

  for i in $(seq 1 7); do
    printf '{"query":"codex beta","timestamp":"2026-05-26T19:05:00.000Z","session_id":"sem-beta-%s","region":"RU-MOW"}\n' "$i"
  done

  for i in $(seq 1 3); do
    printf '{"query":"codex gamma","timestamp":"2026-05-26T19:05:00.000Z","session_id":"sem-gamma-%s","region":"RU-MOW"}\n' "$i"
  done
} | docker compose exec -T redpanda rpk topic produce search.events
```

Проверить точный top:

```bash
curl -sS 'http://localhost:8080/api/v1/top?n=3' | jq
```

Автоматическая проверка через `jq`:

```bash
curl -sS 'http://localhost:8080/api/v1/top?n=3' | jq -e '
  .items == [
    {"query":"codex alpha","count":12},
    {"query":"codex beta","count":7},
    {"query":"codex gamma","count":3}
  ]
  and .window_seconds == 300
  and (.generated_at | type == "string")
'
```

Ожидаемый результат:

```json
{
  "items": [
    {
      "query": "codex alpha",
      "count": 12
    },
    {
      "query": "codex beta",
      "count": 7
    },
    {
      "query": "codex gamma",
      "count": 3
    }
  ],
  "generated_at": "current UTC timestamp",
  "window_seconds": 300
}
```

Проверить Redis counters:

```bash
docker compose exec -T redis sh -c '
for key in $(redis-cli --scan --pattern "trending:bucket:*"); do
  redis-cli HGET "$key" "codex alpha"
  redis-cli HGET "$key" "codex beta"
  redis-cli HGET "$key" "codex gamma"
done
'
```

Ожидаемые значения: `12`, `7`, `3`.

Проверить stop-list filtering:

```bash
curl -i -X POST 'http://localhost:8080/api/v1/stoplist' \
  -H 'Content-Type: application/json' \
  -d '{"query":"  CODEX BETA  "}'

curl -sS 'http://localhost:8080/api/v1/stoplist' | jq
curl -sS 'http://localhost:8080/api/v1/top?n=3' | jq
```

После добавления stop-list должен хранить `"codex beta"`, а `/api/v1/top` должен вернуть только `codex alpha` и `codex gamma`.

Удалить тестовую stop-list запись:

```bash
curl -i -X DELETE 'http://localhost:8080/api/v1/stoplist/CODEX%20BETA'
```

Проверить fraud detector:

```bash
for i in $(seq 1 52); do
  printf '{"query":"codex fraud","timestamp":"2026-05-26T19:05:10.000Z","session_id":"same-fraud-session","region":"RU-MOW"}\n'
done | docker compose exec -T redpanda rpk topic produce search.events
```

Ожидаемое поведение:

| Проверка | Ожидаемый результат |
| --- | --- |
| `codex fraud` в `/api/v1/top` | `count: 50` |
| `trending_fraud_dropped_total` | `2` |

Проверить невалидное Kafka-сообщение:

```bash
printf 'not-json\n' | docker compose exec -T redpanda rpk topic produce search.events
curl -sS 'http://localhost:8080/metrics' | grep 'trending_kafka_errors_total'
```

`trending_kafka_errors_total` должен увеличиться, а `trending_store_writes_total` не должен увеличиться из-за невалидного сообщения.

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

### Получить Топ

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

### Добавить Query В Stop-list

```bash
curl -i -X POST 'http://localhost:8080/api/v1/stoplist' \
  -H 'Content-Type: application/json' \
  -d '{"query":"кроссовки найк"}'
```

Ожидаемый ответ:

```text
HTTP/1.1 204 No Content
```

### Получить Stop-list

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

### Удалить Query Из Stop-list

```bash
curl -i -X DELETE 'http://localhost:8080/api/v1/stoplist/кроссовки%20найк'
```

Ожидаемый ответ:

```text
HTTP/1.1 204 No Content
```

### Metrics

```bash
curl 'http://localhost:8080/metrics'
```

Ключевые метрики:

| Metric | Что показывает |
| --- | --- |
| `trending_events_consumed_total` | Количество принятых Kafka events |
| `trending_fraud_dropped_total` | Events, отброшенные fraud detector |
| `trending_stoplist_dropped_total` | Элементы топа, скрытые stop-list фильтром |
| `trending_kafka_errors_total` | Ошибки Kafka consume, decode или store |
| `trending_store_writes_total` | Записи в выбранный store |
| `trending_store_unique_queries` | Уникальные queries в текущем окне |
| `trending_top_request_duration_seconds` | Latency histogram для `/api/v1/top` |

### Примеры Ошибок

Некорректный `n`:

```bash
curl -i 'http://localhost:8080/api/v1/top?n=0'
```

```json
{
  "error": "n must be between 1 and 100"
}
```

Пустой query для stop-list:

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

### HTTP Коды

| Code | Значение |
| --- | --- |
| `200` | Успешный JSON response |
| `204` | Мутация stop-list выполнена |
| `400` | Невалидные query params или JSON body |
| `500` | Ошибка store или stop-list backend |

## Контракт Kafka-Данных

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

| Поле | Обязательное | Зачем нужно |
| --- | --- | --- |
| `query` | Да | Ключ для подсчета популярности |
| `timestamp` | Да | Время события и базовая валидация контракта |
| `session_id` | Да | Ключ для fraud detection |
| `region` | Нет | Зарезервировано для региональных топов |

Критичные поля для бизнес-логики: `query`, `timestamp`, `session_id`. Если они пустые или невалидные, событие не попадает в store.

## Trade-offs И Бизнес-Логика

| Компромисс | Решение | Причина |
| --- | --- | --- |
| Скорость vs абсолютная свежесть | Top cache обновляется раз в 200 ms | Небольшая задержка дает стабильную latency |
| Память vs latency | Счетчики лежат в ограниченных buckets, top предрассчитан | Нет тяжелой агрегации на каждый request |
| Realtime vs consistency | Redis backend использует короткий cache и time-slot hashes | Несколько реплик получают общий store с eventual freshness |
| Простота vs идеальное event-time окно | Store пишет в текущий processing-time bucket | Для виджета важнее скорость и простота эксплуатации |
| Долговечность vs скорость чтения | Stop-list пишется в Postgres, читается из cache | Admin changes сохраняются, user traffic не ходит в БД |

Ограничения текущей версии:

| Ограничение | Сейчас | Как расширить |
| --- | --- | --- |
| `region` не участвует в подсчете | Один общий топ | Добавить region в store key |
| Stop-list API без auth | Рассчитано на внутреннюю сеть | Добавить admin auth middleware |
| Redis top rebuild lazy | Первый запрос после cache miss делает агрегацию | Добавить background rebuilder |
| Kafka topic должен существовать | В compose Redpanda может создать topic автоматически | Управлять topic через infrastructure |

## Проверки Качества

```bash
make test
make lint
make bench
```

`make test` запускает тесты с race detector.

## Полезные Команды

```bash
make docker-up
make produce
make test
make lint
make docker-down
```
