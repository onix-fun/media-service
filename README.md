# Media Service

Самостоятельный control plane для S3-compatible media storage: multipart upload, CAS/deduplication, references, lifecycle/GC и durable processing jobs.

## Архитектура

- PostgreSQL хранит metadata, upload sessions, references и relations.
- S3/MinIO является data plane для прямой загрузки и скачивания.
- RabbitMQ хранит durable hash и processing jobs.
- Blob удаляется GC после grace period, если на него нет references.
- YAML profiles описывают команды libvips/FFmpeg; processing API развивается без изменения blob contract.
- Опциональный ClamAV stage предусмотрен конфигурацией.

## Запуск

```bash
media-service config validate --config=config/config.example.yaml
media-service serve --config=config/config.example.yaml --role=all
```

Versioned API доступен под `/v1`: uploads, blobs, download URLs и references. Health endpoints: `/livez`, `/readyz`, `/metrics`.

Лицензия: MIT.
