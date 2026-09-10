# Convenience targets. Everything here is a plain go/docker command -- nothing
# in the project requires make.

DB_URL ?= postgres://queue:queue@localhost:5433/queue?sslmode=disable

.PHONY: build test test-unit test-integration up down logs fmt vet clean

## build: compile all three binaries into ./bin
build:
	go build -o bin/queue-server ./cmd/server
	go build -o bin/queue-worker ./cmd/worker
	go build -o bin/queue        ./cmd/queue

## test-unit: run tests that need no database
test-unit:
	go test -race ./internal/...

## test-integration: run tests against a real PostgreSQL
test-integration:
	TEST_DATABASE_URL='$(DB_URL)' go test -race -count=1 ./tests/

## test: everything
test:
	TEST_DATABASE_URL='$(DB_URL)' go test -race -count=1 ./...

## up: start Postgres, the server and two workers
up:
	docker compose up -d

## down: stop everything (add ARGS=-v to also delete the job history)
down:
	docker compose down $(ARGS)

## logs: follow the stack's logs
logs:
	docker compose logs -f

fmt:
	gofmt -w .

vet:
	go vet ./...

clean:
	rm -rf bin/
