package storage

import (
	"context"
	"io"
	"time"

	"media-service/internal/domain"

	"github.com/google/uuid"
)

// MetadataRepo handles the PostgreSQL database operations
type MetadataRepo interface {
	CreateBlob(ctx context.Context, blob *domain.Blob) error
	GetBlob(ctx context.Context, id uuid.UUID) (*domain.Blob, error)
	GetBlobBySHA256(ctx context.Context, sha256 string) (*domain.Blob, error)
	UpdateBlob(ctx context.Context, blob *domain.Blob) error

	CreateUploadSession(ctx context.Context, session *domain.UploadSession) error
	GetUploadSession(ctx context.Context, id uuid.UUID) (*domain.UploadSession, error)
	UpdateUploadSession(ctx context.Context, session *domain.UploadSession) error

	SaveUploadPart(ctx context.Context, part *domain.UploadPart) error
	GetUploadParts(ctx context.Context, sessionID uuid.UUID) ([]*domain.UploadPart, error)
	IsUniqueViolation(err error) bool

	// GC Methods
	GetOrphanedBlobs(ctx context.Context, gracePeriod time.Duration) ([]*domain.Blob, error)
	DeleteBlobRecord(ctx context.Context, id uuid.UUID) error
	GetExpiredSessions(ctx context.Context) ([]*domain.UploadSession, error)

	// Processing DAG Methods
	CreateBlobRelation(ctx context.Context, sourceID, targetID uuid.UUID, relationType string) error
}

// BlobStorage abstraction over S3/MinIO
type BlobStorage interface {
	CreateMultipartUpload(ctx context.Context, key string, contentType string) (string, error)
	GeneratePresignedPartURL(ctx context.Context, key string, uploadID string, partNumber int, expiry time.Duration) (string, error)
	CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []domain.UploadPart) error
	AbortMultipartUpload(ctx context.Context, key string, uploadID string) error

	GetBlobStream(ctx context.Context, key string) (io.ReadCloser, error)
	CopyBlob(ctx context.Context, srcKey, dstKey string) error
	DeleteBlob(ctx context.Context, key string) error
	GetPresignedDownloadURL(ctx context.Context, key string, expiry time.Duration) (string, error)
}
