package upload

import (
	"context"
	"fmt"
	"path"
	"time"

	"media-service/internal/domain"
	"media-service/internal/storage"

	"github.com/google/uuid"
)

type Service interface {
	InitUpload(ctx context.Context, mimeType string, expectedSize *int64, partsCount int, createdBy string) (*domain.UploadSession, map[int]string, error)
	CompleteUpload(ctx context.Context, sessionID uuid.UUID, parts []domain.UploadPart) error
	CancelUpload(ctx context.Context, sessionID uuid.UUID) error
	GetSession(ctx context.Context, sessionID uuid.UUID) (*domain.UploadSession, error)
	GetDownloadURL(ctx context.Context, blobID uuid.UUID) (string, error)
	DeleteBlob(ctx context.Context, blobID uuid.UUID) error
}

type service struct {
	metadata               storage.MetadataRepo
	storage                storage.BlobStorage
	hashChan               chan<- uuid.UUID
	presignedUploadExpiry  time.Duration
	presignedDownloadExpiry time.Duration
}

func NewService(metadata storage.MetadataRepo, s storage.BlobStorage, hashChan chan<- uuid.UUID, presignedUploadExpiry, presignedDownloadExpiry time.Duration) Service {
	return &service{
		metadata:                metadata,
		storage:                 s,
		hashChan:                hashChan,
		presignedUploadExpiry:   presignedUploadExpiry,
		presignedDownloadExpiry: presignedDownloadExpiry,
	}
}

func (s *service) InitUpload(ctx context.Context, mimeType string, expectedSize *int64, partsCount int, createdBy string) (*domain.UploadSession, map[int]string, error) {
	sessionID := uuid.New()

	// Create CAS prefix for temp upload or just use session ID.
	// We'll rename or move to canonical path after hashing.
	// For now, S3 destination is a temp path until hashing deduplicates it.
	tempKey := path.Join("uploads", sessionID.String())

	// 1. Create S3 Multipart Upload
	uploadID, err := s.storage.CreateMultipartUpload(ctx, tempKey, mimeType)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create multipart upload: %w", err)
	}

	// 2. Generate Presigned URLs
	expiry := s.presignedUploadExpiry
	presignedURLs := make(map[int]string)
	for i := 1; i <= partsCount; i++ {
		url, err := s.storage.GeneratePresignedPartURL(ctx, tempKey, uploadID, i, expiry)
		if err != nil {
			// Best effort abort
			_ = s.storage.AbortMultipartUpload(ctx, tempKey, uploadID)
			return nil, nil, fmt.Errorf("failed to generate part URL: %w", err)
		}
		presignedURLs[i] = url
	}

	// 3. Save Session
	session := &domain.UploadSession{
		ID:                sessionID,
		MultipartUploadID: uploadID,
		ObjectKey:         tempKey,
		ExpectedSize:      expectedSize,
		MimeType:          mimeType,
		Status:            domain.SessionUploading,
		CreatedByService:  createdBy,
		CreatedAt:         time.Now(),
		ExpiresAt:         time.Now().Add(expiry),
	}

	if err := s.metadata.CreateUploadSession(ctx, session); err != nil {
		// Best effort abort
		_ = s.storage.AbortMultipartUpload(ctx, tempKey, uploadID)
		return nil, nil, fmt.Errorf("failed to save upload session: %w", err)
	}

	return session, presignedURLs, nil
}

func (s *service) CompleteUpload(ctx context.Context, sessionID uuid.UUID, parts []domain.UploadPart) error {
	// 1. Fetch Session
	session, err := s.metadata.GetUploadSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}
	if session == nil {
		return fmt.Errorf("session not found")
	}

	if session.Status != domain.SessionUploading && session.Status != domain.SessionFinalizing {
		return fmt.Errorf("invalid session status for completion: %s", session.Status)
	}

	// Set status to finalizing (Idempotent marker)
	session.Status = domain.SessionFinalizing
	if err := s.metadata.UpdateUploadSession(ctx, session); err != nil {
		return fmt.Errorf("failed to update session status: %w", err)
	}

	// 2. Save parts to DB for record keeping
	for _, p := range parts {
		p.UploadSessionID = sessionID
		p.CreatedAt = time.Now()
		if err := s.metadata.SaveUploadPart(ctx, &p); err != nil {
			return fmt.Errorf("failed to save part %d: %w", p.PartNumber, err)
		}
	}

	// 3. Complete S3 Multipart Upload
	err = s.storage.CompleteMultipartUpload(ctx, session.ObjectKey, session.MultipartUploadID, parts)
	if err != nil {
		return fmt.Errorf("failed to complete s3 multipart upload: %w", err)
	}

	// 4. Update session to completed
	session.Status = domain.SessionCompleted
	if err := s.metadata.UpdateUploadSession(ctx, session); err != nil {
		return fmt.Errorf("failed to mark session completed: %w", err)
	}

	// 5. Trigger Async Hashing & Dedup Pipeline
	select {
	case s.hashChan <- session.ID:
		// Message sent
	default:
		// Queue is full - this should ideally be persistent (e.g. NATS)
		// For the sake of the initial version, we use an in-memory channel.
	}

	return nil
}

func (s *service) CancelUpload(ctx context.Context, sessionID uuid.UUID) error {
	session, err := s.metadata.GetUploadSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}
	if session == nil {
		return fmt.Errorf("session not found")
	}
	if session.Status != domain.SessionUploading {
		return fmt.Errorf("cannot cancel session in status: %s", session.Status)
	}

	if err := s.storage.AbortMultipartUpload(ctx, session.ObjectKey, session.MultipartUploadID); err != nil {
		return fmt.Errorf("failed to abort S3 multipart upload: %w", err)
	}

	session.Status = domain.SessionAbandoned
	if err := s.metadata.UpdateUploadSession(ctx, session); err != nil {
		return fmt.Errorf("failed to update session status: %w", err)
	}

	return nil
}

func (s *service) GetSession(ctx context.Context, sessionID uuid.UUID) (*domain.UploadSession, error) {
	return s.metadata.GetUploadSession(ctx, sessionID)
}

func (s *service) GetDownloadURL(ctx context.Context, blobID uuid.UUID) (string, error) {
	blob, err := s.metadata.GetBlob(ctx, blobID)
	if err != nil {
		return "", fmt.Errorf("failed to get blob metadata: %w", err)
	}
	if blob == nil {
		return "", fmt.Errorf("blob not found")
	}
	if blob.SHA256 == nil {
		return "", fmt.Errorf("blob is not fully processed yet")
	}

	hashStr := *blob.SHA256
	canonicalKey := fmt.Sprintf("blobs/%s/%s/%s", hashStr[:2], hashStr[2:4], hashStr)

	url, err := s.storage.GetPresignedDownloadURL(ctx, canonicalKey, s.presignedDownloadExpiry)
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned url: %w", err)
	}
	return url, nil
}

func (s *service) DeleteBlob(ctx context.Context, blobID uuid.UUID) error {
	blob, err := s.metadata.GetBlob(ctx, blobID)
	if err != nil {
		return fmt.Errorf("failed to fetch blob: %w", err)
	}
	if blob == nil {
		return fmt.Errorf("blob not found")
	}

	// Delete from S3
	if blob.SHA256 != nil {
		hashStr := *blob.SHA256
		canonicalKey := fmt.Sprintf("blobs/%s/%s/%s", hashStr[:2], hashStr[2:4], hashStr)
		err := s.storage.DeleteBlob(ctx, canonicalKey)
		if err != nil {
			return fmt.Errorf("failed to delete blob from S3: %w", err)
		}
	}

	// Delete from Database
	if err := s.metadata.DeleteBlobRecord(ctx, blobID); err != nil {
		return fmt.Errorf("failed to delete blob record from database: %w", err)
	}

	return nil
}
