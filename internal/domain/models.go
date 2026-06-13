package domain

import (
	"time"

	"github.com/google/uuid"
)

type RetentionState string

const (
	RetentionPendingReference RetentionState = "PENDING_REFERENCE"
	RetentionReferenced       RetentionState = "REFERENCED"
	RetentionOrphaned         RetentionState = "ORPHANED"
	RetentionMarkedForDel     RetentionState = "MARKED_FOR_DELETION"
)

type UploadStatus string

const (
	UploadHashPending UploadStatus = "HASH_PENDING"
	UploadReady       UploadStatus = "READY"
	UploadFailed      UploadStatus = "FAILED"
)

type SessionStatus string

const (
	SessionUploading  SessionStatus = "UPLOADING"
	SessionFinalizing SessionStatus = "FINALIZING"
	SessionCompleted  SessionStatus = "COMPLETED"
	SessionAbandoned  SessionStatus = "ABANDONED"
)

type Blob struct {
	ID               uuid.UUID      `json:"id"`
	SHA256           *string        `json:"sha256"`
	SizeBytes        *int64         `json:"sizeBytes"`
	MimeType         string         `json:"mimeType"`
	RetentionState   RetentionState `json:"retentionState"`
	UploadStatus     UploadStatus   `json:"uploadStatus"`
	CreatedByService string         `json:"createdByService"`
	CreatedAt        time.Time      `json:"createdAt"`
	UpdatedAt        time.Time      `json:"updatedAt"`
}

type UploadSession struct {
	ID                uuid.UUID     `json:"id"`
	MultipartUploadID string        `json:"multipartUploadId"`
	ObjectKey         string        `json:"objectKey"`
	BlobID            *uuid.UUID    `json:"blobId"`
	ExpectedSize      *int64        `json:"expectedSize"`
	MimeType          string        `json:"mimeType"`
	Status            SessionStatus `json:"status"`
	CreatedByService  string        `json:"createdByService"`
	CreatedAt         time.Time     `json:"createdAt"`
	ExpiresAt         time.Time     `json:"expiresAt"`
}

type UploadPart struct {
	UploadSessionID uuid.UUID `json:"uploadSessionId"`
	PartNumber      int       `json:"partNumber"`
	ETag            string    `json:"etag"`
	SizeBytes       int64     `json:"sizeBytes"`
	CreatedAt       time.Time `json:"createdAt"`
}
