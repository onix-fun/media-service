.PHONY: generate build run docker-up docker-down migrate-up migrate-down dev test

# Load .env file if it exists
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

DATABASE_URL ?= postgres://postgres:password@localhost:5432/media?sslmode=disable

generate:
	protoc -I api/proto --go_out=. --go_opt=module=github.com/onix-fun/media \
		--go-grpc_out=. --go-grpc_opt=module=github.com/onix-fun/media \
		api/proto/onix/media/media.proto

build:
	go build -o bin/media cmd/media/main.go

run: build
	./bin/media

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

# Make sure you have golang-migrate installed locally (brew install golang-migrate)
migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down -all

dev: docker-up run

test:
	go test ./...

race:
	go test -race ./...
