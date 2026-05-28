.PHONY: build run swagger docker-up docker-down migrate-up migrate-down dev test

# Load .env file if it exists
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

DATABASE_URL ?= postgres://postgres:password@localhost:5432/media?sslmode=disable

build: swagger
	go build -o bin/media-service cmd/media-service/main.go

run: build
	./bin/media-service

swagger:
	go run github.com/swaggo/swag/cmd/swag@latest init -g cmd/media-service/main.go --parseDependency --parseInternal -o docs
	cp docs/swagger.json docs/openapi.json

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

# Make sure you have golang-migrate installed locally (brew install golang-migrate)
migrate-up:
	migrate -path migrations -database "$(DATABASE_URL)" up

migrate-down:
	migrate -path migrations -database "$(DATABASE_URL)" down -all

dev: docker-up swagger run

test:
	python3 tests/test_e2e.py
