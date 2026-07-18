package storage

import (
	"context"
	"io"
	"time"

	"github.com/onix-fun/media/internal/domain"

	"github.com/google/uuid"
)

// MetadataRepo handles the PostgreSQL database operations
type MetadataRepo interface {
	CreateBlob(ctx context.Context, blob *domain.Blob) error
	CreateOrGetBlob(ctx context.Context, blob *domain.Blob) (*domain.Blob, bool, error)
	GetBlob(ctx context.Context, id uuid.UUID) (*domain.Blob, error)
	GetBlobBySHA256(ctx context.Context, sha256 string) (*domain.Blob, error)
	UpdateBlob(ctx context.Context, blob *domain.Blob) error
	GrantBlobAccess(ctx context.Context, blobID uuid.UUID, ownerKey string) error
	GrantServiceAliasAccess(ctx context.Context, ownerKey string, aliases []string) error
	HasBlobAccess(ctx context.Context, blobID uuid.UUID, ownerKey string) (bool, error)
	CreateReference(ctx context.Context, blobID uuid.UUID, ownerKey, referenceType, referenceID string) error
	DeleteReference(ctx context.Context, blobID uuid.UUID, ownerKey, referenceType, referenceID string) error

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

	// Content media assets
	CreateMediaAsset(ctx context.Context, asset *domain.MediaAsset) error
	GetMediaAsset(ctx context.Context, id uuid.UUID) (*domain.MediaAsset, error)
	GetMediaAssetByUploadSession(ctx context.Context, sessionID uuid.UUID) (*domain.MediaAsset, error)
	UpdateMediaAsset(ctx context.Context, asset *domain.MediaAsset) error
	UpsertMediaAssetVariant(ctx context.Context, variant *domain.MediaAssetVariant) error
	ListMediaAssetVariants(ctx context.Context, assetID uuid.UUID) ([]domain.MediaAssetVariant, error)
	ListMediaAssetVariantsForGeneration(ctx context.Context, assetID uuid.UUID, generation int64) ([]domain.MediaAssetVariant, error)
	GetMediaAssetVariant(ctx context.Context, assetID uuid.UUID, generation int64, name string) (*domain.MediaAssetVariant, error)
	CreateProcessingRun(ctx context.Context, run *domain.ProcessingRun) error
	GetProcessingRun(ctx context.Context, id uuid.UUID) (*domain.ProcessingRun, error)
	GetProcessingRunByIdempotency(ctx context.Context, namespace, key string) (*domain.ProcessingRun, error)
	GetLatestProcessingRun(ctx context.Context, assetID uuid.UUID) (*domain.ProcessingRun, error)
	UpdateProcessingRun(ctx context.Context, run *domain.ProcessingRun) error
	// ReleaseAssetOriginal marks one completed asset as no longer requiring its
	// source. It returns a source blob only when no asset or legacy reference
	// still needs it, so callers may safely delete the original object.
	ReleaseAssetOriginal(ctx context.Context, assetID uuid.UUID) (*domain.Blob, error)
	AppendAssetLifecycleEvent(ctx context.Context, event *domain.AssetLifecycleEvent) error
	ListAssetLifecycleEvents(ctx context.Context, afterSequence int64, limit int) ([]domain.AssetLifecycleEvent, error)
	ListAssetLifecycleEventsForNamespace(ctx context.Context, namespace string, afterSequence int64, limit int) ([]domain.AssetLifecycleEvent, error)
	ListExpiredFailedAssetIDs(ctx context.Context, retention time.Duration, limit int) ([]uuid.UUID, error)
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
	PutBlob(ctx context.Context, key string, reader io.Reader, size int64, contentType string) error
}
