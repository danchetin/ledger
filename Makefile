DB_URL ?= postgres://ledger:ledger@localhost:55432/ledger?sslmode=disable

.PHONY: db test run loadtest lint up down

db: ## start a throwaway Postgres for tests on port 55432
	docker run -d --rm --name ledger-pg -e POSTGRES_USER=ledger -e POSTGRES_PASSWORD=ledger \
		-e POSTGRES_DB=ledger -p 127.0.0.1:55432:5432 postgres:16-alpine

test: ## run all tests against $(DB_URL)
	LEDGER_TEST_DATABASE_URL="$(DB_URL)" go test -race -count=1 ./...

run: ## run the API on :8080 against $(DB_URL)
	DATABASE_URL="$(DB_URL)" go run ./cmd/ledger serve

loadtest: ## drive random transfers against a running API for 30s
	go run ./cmd/loadtest -url http://localhost:8080 -duration 30s

lint:
	gofmt -l . && go vet ./...

up: ## API + Postgres in Docker
	docker compose up --build

down:
	docker compose down -v
