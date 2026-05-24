run:
	go run ./cmd/server

produce:
	go run ./cmd/producer -rps 200

test:
	go test -race ./...

bench:
	go test -bench=. -benchmem ./internal/store/

lint:
	golangci-lint run ./...

migrate:
	psql $(DATABASE_URL) -f migrations/001_create_stoplist.sql

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down -v
