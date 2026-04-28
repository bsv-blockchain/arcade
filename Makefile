.PHONY: build test lint docker-up docker-down run

build:
	go build ./...

test:
	go test ./...

lint:
	golangci-lint run

docker-up:
	podman-compose up -d

docker-down:
	podman-compose down

run:
	go run ./cmd/arcade
