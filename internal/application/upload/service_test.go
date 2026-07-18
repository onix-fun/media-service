package upload

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/onix-fun/media/internal/domain"
)

func TestGetDownloadURLAllowsConfiguredLegacyAlias(t *testing.T) {
	blobID := uuid.MustParse("019f5cca-5072-75d9-a51a-4b8ca3dbdd45")
	hash := "e2e852f947e1199d138639f9ad89aebfb1b4b2369ad52923fdf4f9999a444009"
	repo := &fakeMetadataRepo{
		blobs: map[uuid.UUID]*domain.Blob{
			blobID: {
				ID:               blobID,
				SHA256:           &hash,
				MimeType:         "video/webm",
				CreatedByService: "content",
			},
		},
		access: map[uuid.UUID]map[string]bool{
			blobID: {"content": true},
		},
	}
	service := NewService(repo, fakeBlobStorage{}, fakeJobs{}, time.Hour, time.Hour, map[string][]string{
		"content": {"content"},
	})

	url, err := service.GetDownloadURL(context.Background(), blobID, "content")
	if err != nil {
		t.Fatalf("expected alias access, got %v", err)
	}
	if url == "" {
		t.Fatal("expected download url")
	}
}

func TestGetDownloadURLRejectsUnrelatedCaller(t *testing.T) {
	blobID := uuid.MustParse("019f5cca-5072-75d9-a51a-4b8ca3dbdd45")
	hash := "e2e852f947e1199d138639f9ad89aebfb1b4b2369ad52923fdf4f9999a444009"
	repo := &fakeMetadataRepo{
		blobs: map[uuid.UUID]*domain.Blob{
			blobID: {
				ID:               blobID,
				SHA256:           &hash,
				MimeType:         "video/webm",
				CreatedByService: "content",
			},
		},
		access: map[uuid.UUID]map[string]bool{
			blobID: {"content": true},
		},
	}
	service := NewService(repo, fakeBlobStorage{}, fakeJobs{}, time.Hour, time.Hour, map[string][]string{
		"content": {"content"},
	})

	if _, err := service.GetDownloadURL(context.Background(), blobID, "profile"); err == nil {
		t.Fatal("expected unrelated caller to be rejected")
	}
}

type fakeMetadataRepo struct {
	blobs  map[uuid.UUID]*domain.Blob
	access map[uuid.UUID]map[string]bool
}

func (f *fakeMetadataRepo) CreateBlob(context.Context, *domain.Blob) error { return nil }
func (f *fakeMetadataRepo) CreateOrGetBlob(_ context.Context, blob *domain.Blob) (*domain.Blob, bool, error) {
	return blob, true, nil
}
func (f *fakeMetadataRepo) GetBlob(_ context.Context, id uuid.UUID) (*domain.Blob, error) {
	return f.blobs[id], nil
}
func (f *fakeMetadataRepo) GetBlobBySHA256(context.Context, string) (*domain.Blob, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) UpdateBlob(context.Context, *domain.Blob) error { return nil }
func (f *fakeMetadataRepo) GrantBlobAccess(_ context.Context, blobID uuid.UUID, ownerKey string) error {
	if f.access == nil {
		f.access = map[uuid.UUID]map[string]bool{}
	}
	if f.access[blobID] == nil {
		f.access[blobID] = map[string]bool{}
	}
	f.access[blobID][ownerKey] = true
	return nil
}
func (f *fakeMetadataRepo) GrantServiceAliasAccess(context.Context, string, []string) error {
	return nil
}
func (f *fakeMetadataRepo) HasBlobAccess(_ context.Context, blobID uuid.UUID, ownerKey string) (bool, error) {
	return f.access[blobID][ownerKey], nil
}
func (f *fakeMetadataRepo) CreateReference(context.Context, uuid.UUID, string, string, string) error {
	return nil
}
func (f *fakeMetadataRepo) DeleteReference(context.Context, uuid.UUID, string, string, string) error {
	return nil
}
func (f *fakeMetadataRepo) CreateUploadSession(context.Context, *domain.UploadSession) error {
	return nil
}
func (f *fakeMetadataRepo) GetUploadSession(context.Context, uuid.UUID) (*domain.UploadSession, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) UpdateUploadSession(context.Context, *domain.UploadSession) error {
	return nil
}
func (f *fakeMetadataRepo) SaveUploadPart(context.Context, *domain.UploadPart) error { return nil }
func (f *fakeMetadataRepo) GetUploadParts(context.Context, uuid.UUID) ([]*domain.UploadPart, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) IsUniqueViolation(error) bool { return false }
func (f *fakeMetadataRepo) GetOrphanedBlobs(context.Context, time.Duration) ([]*domain.Blob, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) DeleteBlobRecord(context.Context, uuid.UUID) error { return nil }
func (f *fakeMetadataRepo) GetExpiredSessions(context.Context) ([]*domain.UploadSession, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) CreateBlobRelation(context.Context, uuid.UUID, uuid.UUID, string) error {
	return nil
}
func (f *fakeMetadataRepo) CreateMediaAsset(context.Context, *domain.MediaAsset) error { return nil }
func (f *fakeMetadataRepo) GetMediaAsset(context.Context, uuid.UUID) (*domain.MediaAsset, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) GetMediaAssetByUploadSession(context.Context, uuid.UUID) (*domain.MediaAsset, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) UpdateMediaAsset(context.Context, *domain.MediaAsset) error { return nil }
func (f *fakeMetadataRepo) UpsertMediaAssetVariant(context.Context, *domain.MediaAssetVariant) error {
	return nil
}
func (f *fakeMetadataRepo) ListMediaAssetVariants(context.Context, uuid.UUID) ([]domain.MediaAssetVariant, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) ListMediaAssetVariantsForGeneration(context.Context, uuid.UUID, int64) ([]domain.MediaAssetVariant, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) GetMediaAssetVariant(context.Context, uuid.UUID, int64, string) (*domain.MediaAssetVariant, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) CreateProcessingRun(context.Context, *domain.ProcessingRun) error {
	return nil
}
func (f *fakeMetadataRepo) GetProcessingRun(context.Context, uuid.UUID) (*domain.ProcessingRun, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) GetProcessingRunByIdempotency(context.Context, string, string) (*domain.ProcessingRun, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) GetLatestProcessingRun(context.Context, uuid.UUID) (*domain.ProcessingRun, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) UpdateProcessingRun(context.Context, *domain.ProcessingRun) error {
	return nil
}
func (f *fakeMetadataRepo) ReleaseAssetOriginal(context.Context, uuid.UUID) (*domain.Blob, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) AppendAssetLifecycleEvent(context.Context, *domain.AssetLifecycleEvent) error {
	return nil
}
func (f *fakeMetadataRepo) ListAssetLifecycleEvents(context.Context, int64, int) ([]domain.AssetLifecycleEvent, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) ListAssetLifecycleEventsForNamespace(context.Context, string, int64, int) ([]domain.AssetLifecycleEvent, error) {
	return nil, nil
}
func (f *fakeMetadataRepo) ListExpiredFailedAssetIDs(context.Context, time.Duration, int) ([]uuid.UUID, error) {
	return nil, nil
}

type fakeBlobStorage struct{}

func (fakeBlobStorage) CreateMultipartUpload(context.Context, string, string) (string, error) {
	return "upload-id", nil
}
func (fakeBlobStorage) GeneratePresignedPartURL(context.Context, string, string, int, time.Duration) (string, error) {
	return "http://internal/application/upload", nil
}
func (fakeBlobStorage) CompleteMultipartUpload(context.Context, string, string, []domain.UploadPart) error {
	return nil
}
func (fakeBlobStorage) AbortMultipartUpload(context.Context, string, string) error { return nil }
func (fakeBlobStorage) GetBlobStream(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(nil), nil
}
func (fakeBlobStorage) CopyBlob(context.Context, string, string) error { return nil }
func (fakeBlobStorage) DeleteBlob(context.Context, string) error       { return nil }
func (fakeBlobStorage) GetPresignedDownloadURL(context.Context, string, time.Duration) (string, error) {
	return "http://media.onix.localhost:8088/media-blobs/blob", nil
}
func (fakeBlobStorage) PutBlob(context.Context, string, io.Reader, int64, string) error { return nil }

type fakeJobs struct{}

func (fakeJobs) PublishHash(context.Context, uuid.UUID) error            { return nil }
func (fakeJobs) PublishProcess(context.Context, uuid.UUID, string) error { return nil }
