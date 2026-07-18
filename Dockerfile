FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    for attempt in 1 2 3 4 5; do \
        go mod download && exit 0; \
        [ "$attempt" -eq 5 ] && exit 1; \
        sleep $((attempt * 3)); \
    done
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/media ./cmd/media

FROM alpine:3.23

RUN apk --no-cache add ca-certificates tzdata ffmpeg vips-tools \
    && adduser -D -u 10001 app

WORKDIR /app/
COPY --from=build /out/media ./media
COPY migrations ./migrations
COPY config ./config

EXPOSE 8080 9093
USER app
ENTRYPOINT ["./media"]
CMD ["serve", "--config=config/config.example.yaml"]
