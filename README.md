# Media Service

Микросервис для работы с медиа-контентом.

## Документация
Вся подробная документация находится в папке [docs/](docs/README.md):
- [Архитектура](docs/architecture.md)
- [Конфигурация](docs/configuration.md)
- [Разработка](docs/development.md)

## Быстрый старт
```bash
make docker-up
make run
```

## Content media assets

`MediaStore` сохраняет прежние multipart RPC для blob-совместимости и добавляет
`InitAssetUpload`, `CompleteAssetUpload`, `GetAsset` и `RetryAssetProcessing`.
Asset проходит состояния `UPLOADING → PROCESSING → READY|FAILED`; Content
публикует только `READY` asset. Профили `CONTENT_IMAGE`, `CONTENT_VIDEO` и
`CONTENT_AUDIO` создают соответственно WebP 480/960/1440/2048, MP4 1080p с
poster и M4A с waveform. После готовности всех вариантов worker освобождает
оригинал, если он больше не нужен другому asset или legacy reference.

Новые asset RPC принимают необязательный `owner_key`. Пустое значение оставляет
identity вызывающего сервиса; только `x-onix-service=content` может передать
активного владельца (`user-id` или `content:user-id`).
