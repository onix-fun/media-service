FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/media-service ./cmd/media-service

FROM alpine:3.23

RUN apk --no-cache add ca-certificates tzdata \
    && adduser -D -u 10001 app

WORKDIR /app/
COPY --from=build /out/media-service ./media-service
COPY migrations ./migrations

EXPOSE 8080
USER app
CMD ["./media-service"]
