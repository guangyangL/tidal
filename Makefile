.PHONY: run test deps lint build

run:
	go run ./cmd/server

test:
	go test -race -count=1 ./...

deps:
	go mod tidy
	go mod download

lint:
	golangci-lint run ./...

build:
	CGO_ENABLED=0 go build -o bin/tidal ./cmd/server

docker-up:
	docker compose -f deploy/docker-compose.yml up -d

docker-down:
	docker compose -f deploy/docker-compose.yml down
