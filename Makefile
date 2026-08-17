.PHONY: build test lint run-api run-worker run-scheduler compose-up compose-down clean

BIN_DIR := bin

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/api ./cmd/api
	go build -o $(BIN_DIR)/worker ./cmd/worker
	go build -o $(BIN_DIR)/scheduler ./cmd/scheduler

test:
	go test -race ./...

lint:
	golangci-lint run ./...

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

run-scheduler:
	go run ./cmd/scheduler

compose-up:
	docker compose up --build

compose-down:
	docker compose down

clean:
	rm -rf $(BIN_DIR)