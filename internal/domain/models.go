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

// AssetStatus describes the user-visible lifecycle of a processed media asset.
// It deliberately differs from UploadStatus: a source upload can be complete
// while its delivery variants are still processing.
type AssetStatus string

const (
	AssetUploading  AssetStatus = "UPLOADING"
	AssetVerifying  AssetStatus = "VERIFYING"
	AssetAvailable  AssetStatus = "AVAILABLE"
	AssetProcessing AssetStatus = "PROCESSING"
	AssetReady      AssetStatus = "READY"
	AssetFailed     AssetStatus = "FAILED"
	AssetRejected   AssetStatus = "REJECTED"
	AssetCancelled  AssetStatus = "CANCELLED"
)

// ProcessingProfile is a closed set of media delivery pipelines exposed to
// Content. Generic worker profiles remain an implementation detail.
type ProcessingProfile string

const (
	ProcessingProfileContentImage ProcessingProfile = "CONTENT_IMAGE"
	ProcessingProfileContentVideo ProcessingProfile = "CONTENT_VIDEO"
	ProcessingProfileContentAudio ProcessingProfile = "CONTENT_AUDIO"
	PipelineUnassigned            ProcessingProfile = "UNASSIGNED"
	PipelineImageResponsiveWebV1  ProcessingProfile = "image-responsive-web-v1"
	PipelineVideoWeb1080V1        ProcessingProfile = "video-web-1080-v1"
	PipelineAudioWebV1            ProcessingProfile = "audio-web-v1"
)

type MediaKind string

const (
	MediaKindImage MediaKind = "IMAGE"
	MediaKindVideo MediaKind = "VIDEO"
	MediaKindAudio MediaKind = "AUDIO"
)

type SourceStatus string

const (
	SourceUploading SourceStatus = "UPLOADING"
	SourceVerifying SourceStatus = "VERIFYING"
	SourceAvailable SourceStatus = "AVAILABLE"
	SourceRejected  SourceStatus = "REJECTED"
)

type ProcessingStatus string

const (
	ProcessingWaitingSource ProcessingStatus = "WAITING_SOURCE"
	ProcessingQueued        ProcessingStatus = "QUEUED"
	ProcessingRunning       ProcessingStatus = "PROCESSING"
	ProcessingReady         ProcessingStatus = "READY"
	ProcessingFailed        ProcessingStatus = "FAILED"
	ProcessingCancelled     ProcessingStatus = "CANCELLED"
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

// MediaAsset is the durable public identity used by Content. SourceBlobID is
// transient and becomes nil after successful delivery variants have been made
// and the original can safely be removed.
type MediaAsset struct {
	ID                uuid.UUID         `json:"id"`
	UploadSessionID   *uuid.UUID        `json:"uploadSessionId,omitempty"`
	SourceBlobID      *uuid.UUID        `json:"sourceBlobId,omitempty"`
	OwnerKey          string            `json:"ownerKey"`
	ClientNamespace   string            `json:"clientNamespace"`
	OwnerRef          string            `json:"ownerRef"`
	DeclaredKind      MediaKind         `json:"declaredKind"`
	SourcePolicyID    string            `json:"sourcePolicyId"`
	SourceStatus      SourceStatus      `json:"sourceStatus"`
	SourceFailureCode string            `json:"sourceFailureCode,omitempty"`
	Profile           ProcessingProfile `json:"profile"`
	Status            AssetStatus       `json:"status"`
	MimeType          string            `json:"mimeType"`
	Width             int               `json:"width"`
	Height            int               `json:"height"`
	DurationMS        int64             `json:"durationMs"`
	FailureReason     string            `json:"failureReason,omitempty"`
	FailedAt          *time.Time        `json:"failedAt,omitempty"`
	OriginalRemoved   bool              `json:"originalRemoved"`
	// Generation is incremented for every explicit retry.  It makes a retry a
	// new durable unit of work instead of reviving a terminal queue record.
	Generation int64               `json:"generation"`
	CreatedAt  time.Time           `json:"createdAt"`
	UpdatedAt  time.Time           `json:"updatedAt"`
	Variants   []MediaAssetVariant `json:"variants,omitempty"`
}

// MediaAssetVariant is a delivery object, never an upload source. Name is
// stable within one asset (for example image-960, video-1080, poster).
type MediaAssetVariant struct {
	AssetID    uuid.UUID `json:"assetId"`
	Name       string    `json:"name"`
	Generation int64     `json:"generation"`
	BlobID     uuid.UUID `json:"blobId"`
	MimeType   string    `json:"mimeType"`
	Width      int       `json:"width"`
	Height     int       `json:"height"`
	DurationMS int64     `json:"durationMs"`
	Bitrate    int64     `json:"bitrate"`
	URL        string    `json:"url,omitempty"`
}

type ProcessingRun struct {
	ID              uuid.UUID         `json:"id"`
	AssetID         uuid.UUID         `json:"assetId"`
	ClientNamespace string            `json:"clientNamespace"`
	OwnerRef        string            `json:"ownerRef"`
	PipelineID      ProcessingProfile `json:"pipelineId"`
	PipelineVersion string            `json:"pipelineVersion"`
	Generation      int64             `json:"generation"`
	IdempotencyKey  string            `json:"idempotencyKey"`
	Status          ProcessingStatus  `json:"status"`
	FailureCode     string            `json:"failureCode,omitempty"`
	CreatedAt       time.Time         `json:"createdAt"`
	UpdatedAt       time.Time         `json:"updatedAt"`
}

// AssetLifecycleEvent is an append-only integration record.  eventID is
// deterministic for (asset, generation, type), so redelivery is harmless.
type AssetLifecycleEvent struct {
	Sequence        int64
	EventID         string
	Type            string
	AssetID         uuid.UUID
	RunID           *uuid.UUID
	ClientNamespace string
	OwnerRef        string
	Generation      int64
	OwnerKey        string
	FailureCode     string
	CreatedAt       time.Time
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
