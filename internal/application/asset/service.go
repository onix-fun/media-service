package asset

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/onix-fun/media/internal/application/storage"
	"github.com/onix-fun/media/internal/application/upload"
	"github.com/onix-fun/media/internal/domain"
)

// JobPublisher is deliberately small so the asset lifecycle is not coupled to
// the SQL queue implementation.
type JobPublisher interface {
	PublishAsset(context.Context, uuid.UUID, int64) error
}

type Service interface {
	Init(context.Context, string, *int64, int, domain.ProcessingProfile, string) (*domain.MediaAsset, *domain.UploadSession, map[int]string, error)
	Complete(context.Context, uuid.UUID, uuid.UUID, []domain.UploadPart, string) (*domain.MediaAsset, error)
	Get(context.Context, uuid.UUID, string) (*domain.MediaAsset, error)
	Retry(context.Context, uuid.UUID, string) (*domain.MediaAsset, error)
	BeginProcessingForSession(context.Context, uuid.UUID) (*domain.MediaAsset, error)
	ListLifecycleEvents(context.Context, int64, int) ([]domain.AssetLifecycleEvent, error)
	BeginUpload(context.Context, string, string, domain.MediaKind, string, *int64, int, string) (*domain.MediaAsset, *domain.UploadSession, map[int]string, error)
	GetSource(context.Context, uuid.UUID, string, string) (*domain.MediaAsset, *domain.ProcessingRun, error)
	RequestProcessing(context.Context, uuid.UUID, string, string, domain.ProcessingProfile, string) (*domain.ProcessingRun, error)
	GetProcessingRun(context.Context, uuid.UUID, string, string) (*domain.ProcessingRun, error)
	RetryProcessing(context.Context, uuid.UUID, string, string, string) (*domain.ProcessingRun, error)
	CancelProcessing(context.Context, uuid.UUID, string, string) (*domain.ProcessingRun, error)
	ResolveDelivery(context.Context, uuid.UUID, int64, string, string, string) (string, string, error)
	ResolveSource(context.Context, uuid.UUID, string, string) (string, string, error)
	GetDeliveryManifest(context.Context, uuid.UUID, int64, string, string) ([]domain.MediaAssetVariant, error)
	ReleaseSource(context.Context, uuid.UUID, int64, string, string) error
	ListLifecycleEventsForNamespace(context.Context, string, int64, int) ([]domain.AssetLifecycleEvent, error)
}

type service struct {
	metadata storage.MetadataRepo
	uploads  upload.Service
	jobs     JobPublisher
	blobs    storage.BlobStorage
}

func NewService(metadata storage.MetadataRepo, uploads upload.Service, jobs JobPublisher, blobStores ...storage.BlobStorage) Service {
	result := &service{metadata: metadata, uploads: uploads, jobs: jobs}
	if len(blobStores) > 0 {
		result.blobs = blobStores[0]
	}
	return result
}

func (s *service) Init(ctx context.Context, mimeType string, expectedSize *int64, partsCount int, profile domain.ProcessingProfile, ownerKey string) (*domain.MediaAsset, *domain.UploadSession, map[int]string, error) {
	mimeType = strings.ToLower(strings.TrimSpace(mimeType))
	if err := validateProfile(profile, mimeType); err != nil {
		return nil, nil, nil, err
	}
	session, parts, err := s.uploads.InitUpload(ctx, mimeType, expectedSize, partsCount, ownerKey)
	if err != nil {
		return nil, nil, nil, err
	}
	now := time.Now()
	asset := &domain.MediaAsset{
		ID:              uuid.Must(uuid.NewV7()),
		UploadSessionID: &session.ID,
		OwnerKey:        ownerKey,
		ClientNamespace: "legacy",
		OwnerRef:        ownerKey,
		DeclaredKind:    kindForPipeline(profile),
		SourcePolicyID:  "legacy-v1",
		SourceStatus:    domain.SourceUploading,
		Profile:         profile,
		Status:          domain.AssetUploading,
		MimeType:        mimeType,
		Generation:      1,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if err := s.metadata.CreateMediaAsset(ctx, asset); err != nil {
		// The multipart session is not externally useful without its asset.
		_ = s.uploads.CancelUpload(ctx, session.ID, ownerKey)
		return nil, nil, nil, fmt.Errorf("create media asset: %w", err)
	}
	return asset, session, parts, nil
}

func (s *service) BeginUpload(ctx context.Context, namespace, ownerRef string, kind domain.MediaKind, mimeType string, expectedSize *int64, partsCount int, sourcePolicyID string) (*domain.MediaAsset, *domain.UploadSession, map[int]string, error) {
	namespace = strings.TrimSpace(namespace)
	ownerRef = strings.TrimSpace(ownerRef)
	if namespace == "" || ownerRef == "" {
		return nil, nil, nil, fmt.Errorf("client namespace and owner_ref are required")
	}
	if sourcePolicyID != "browser-native-v1" && sourcePolicyID != "browser-capture-v1" {
		return nil, nil, nil, fmt.Errorf("unsupported source policy %q", sourcePolicyID)
	}
	mimeType = strings.ToLower(strings.TrimSpace(strings.Split(mimeType, ";")[0]))
	if err := validateSourcePolicy(sourcePolicyID, kind, mimeType); err != nil {
		return nil, nil, nil, err
	}
	ownerKey := namespace + ":" + ownerRef
	session, parts, err := s.uploads.InitUpload(ctx, mimeType, expectedSize, partsCount, ownerKey)
	if err != nil {
		return nil, nil, nil, err
	}
	now := time.Now()
	asset := &domain.MediaAsset{ID: uuid.Must(uuid.NewV7()), UploadSessionID: &session.ID, OwnerKey: ownerKey,
		ClientNamespace: namespace, OwnerRef: ownerRef, DeclaredKind: kind, SourcePolicyID: sourcePolicyID,
		SourceStatus: domain.SourceUploading, Profile: domain.PipelineUnassigned, Status: domain.AssetUploading,
		MimeType: mimeType, Generation: 0, CreatedAt: now, UpdatedAt: now}
	if err := s.metadata.CreateMediaAsset(ctx, asset); err != nil {
		_ = s.uploads.CancelUpload(ctx, session.ID, ownerKey)
		return nil, nil, nil, fmt.Errorf("create media asset: %w", err)
	}
	return asset, session, parts, nil
}

func (s *service) Complete(ctx context.Context, assetID, sessionID uuid.UUID, parts []domain.UploadPart, ownerKey string) (*domain.MediaAsset, error) {
	asset, err := s.ownedAsset(ctx, assetID, ownerKey)
	if err != nil {
		return nil, err
	}
	if asset.UploadSessionID == nil || *asset.UploadSessionID != sessionID {
		return nil, fmt.Errorf("upload session does not belong to asset")
	}
	if asset.Status != domain.AssetUploading && asset.Status != domain.AssetProcessing {
		return nil, fmt.Errorf("asset cannot be completed in status %s", asset.Status)
	}
	if err := validateCompletedParts(parts); err != nil {
		return nil, err
	}
	if err := s.uploads.CompleteUpload(ctx, sessionID, parts, ownerKey); err != nil {
		return nil, err
	}
	asset.Status = domain.AssetVerifying
	asset.SourceStatus = domain.SourceVerifying
	asset.FailureReason = ""
	asset.FailedAt = nil
	if err := s.metadata.UpdateMediaAsset(ctx, asset); err != nil {
		return nil, fmt.Errorf("mark asset processing: %w", err)
	}
	return asset, nil
}

func validateCompletedParts(parts []domain.UploadPart) error {
	if len(parts) == 0 {
		return fmt.Errorf("at least one uploaded part is required")
	}
	seen := make(map[int]struct{}, len(parts))
	for _, part := range parts {
		if part.PartNumber < 1 {
			return fmt.Errorf("invalid uploaded part number %d", part.PartNumber)
		}
		if strings.TrimSpace(part.ETag) == "" {
			return fmt.Errorf("uploaded part %d is missing ETag", part.PartNumber)
		}
		if _, duplicate := seen[part.PartNumber]; duplicate {
			return fmt.Errorf("uploaded part %d is duplicated", part.PartNumber)
		}
		seen[part.PartNumber] = struct{}{}
	}
	return nil
}

func (s *service) Get(ctx context.Context, assetID uuid.UUID, ownerKey string) (*domain.MediaAsset, error) {
	asset, err := s.ownedAsset(ctx, assetID, ownerKey)
	if err != nil {
		return nil, err
	}
	variants, err := s.metadata.ListMediaAssetVariants(ctx, asset.ID)
	if err != nil {
		return nil, err
	}
	for index := range variants {
		url, err := s.uploads.GetDownloadURL(ctx, variants[index].BlobID, ownerKey)
		if err != nil {
			return nil, fmt.Errorf("create delivery URL for %s: %w", variants[index].Name, err)
		}
		variants[index].URL = url
	}
	asset.Variants = variants
	return asset, nil
}

func (s *service) Retry(ctx context.Context, assetID uuid.UUID, ownerKey string) (*domain.MediaAsset, error) {
	asset, err := s.ownedAsset(ctx, assetID, ownerKey)
	if err != nil {
		return nil, err
	}
	if asset.Status != domain.AssetFailed {
		return nil, fmt.Errorf("only failed assets can be retried")
	}
	if asset.SourceBlobID == nil {
		return nil, fmt.Errorf("source media is no longer available for retry")
	}
	asset.Status = domain.AssetProcessing
	asset.FailureReason = ""
	asset.FailedAt = nil
	asset.Generation++
	if err := s.metadata.UpdateMediaAsset(ctx, asset); err != nil {
		return nil, err
	}
	if err := s.jobs.PublishAsset(ctx, asset.ID, asset.Generation); err != nil {
		return nil, fmt.Errorf("enqueue asset retry: %w", err)
	}
	return asset, nil
}

// BeginProcessingForSession is called by the hash job after its canonical
// source blob exists. It is idempotent because hash jobs may be delivered more
// than once.
func (s *service) BeginProcessingForSession(ctx context.Context, sessionID uuid.UUID) (*domain.MediaAsset, error) {
	asset, err := s.metadata.GetMediaAssetByUploadSession(ctx, sessionID)
	if err != nil || asset == nil {
		return asset, err
	}
	session, err := s.metadata.GetUploadSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if session == nil || session.BlobID == nil {
		return nil, fmt.Errorf("hashed upload session has no source blob")
	}
	if asset.Status == domain.AssetReady || asset.SourceStatus == domain.SourceAvailable {
		return asset, nil
	}
	asset.SourceBlobID = session.BlobID
	if asset.SourcePolicyID == "browser-native-v1" || asset.SourcePolicyID == "browser-capture-v1" {
		blob, blobErr := s.metadata.GetBlob(ctx, *session.BlobID)
		if blobErr != nil {
			return nil, blobErr
		}
		if blob != nil && asset.DeclaredKind == domain.MediaKindAudio && blob.MimeType == "video/mp4" {
			blob.MimeType = "audio/mp4"
		}
		if blob == nil || validateSourcePolicy(asset.SourcePolicyID, asset.DeclaredKind, strings.ToLower(strings.TrimSpace(blob.MimeType))) != nil || s.verifySourceCodec(ctx, asset, blob) != nil {
			asset.Status = domain.AssetRejected
			asset.SourceStatus = domain.SourceRejected
			asset.SourceFailureCode = "UNSUPPORTED_OR_MALFORMED_MEDIA"
			asset.FailureReason = "Uploaded file content does not match " + asset.SourcePolicyID
			now := time.Now()
			asset.FailedAt = &now
			if err := s.metadata.UpdateMediaAsset(ctx, asset); err != nil {
				return nil, err
			}
			_ = s.metadata.AppendAssetLifecycleEvent(ctx, &domain.AssetLifecycleEvent{EventID: fmt.Sprintf("%s:source.rejected", asset.ID), Type: "source.rejected", AssetID: asset.ID, OwnerKey: asset.OwnerKey, ClientNamespace: asset.ClientNamespace, OwnerRef: asset.OwnerRef, FailureCode: asset.SourceFailureCode})
			// A consumer may request processing while source verification is still
			// running. Rejecting the source must terminally resolve that waiting
			// run; otherwise the consumer can remain in PENDING_SOURCE forever.
			if run, runErr := s.metadata.GetLatestProcessingRun(ctx, asset.ID); runErr == nil && run != nil && run.Status == domain.ProcessingWaitingSource {
				run.Status = domain.ProcessingFailed
				run.FailureCode = asset.SourceFailureCode
				if s.metadata.UpdateProcessingRun(ctx, run) == nil {
					_ = s.metadata.AppendAssetLifecycleEvent(ctx, runEvent(run, "processing.failed", run.FailureCode))
				}
			}
			return asset, nil
		}
		asset.MimeType = blob.MimeType
	}
	asset.Status = domain.AssetAvailable
	asset.SourceStatus = domain.SourceAvailable
	asset.FailureReason = ""
	if err := s.metadata.UpdateMediaAsset(ctx, asset); err != nil {
		return nil, err
	}
	_ = s.metadata.AppendAssetLifecycleEvent(ctx, &domain.AssetLifecycleEvent{
		EventID: fmt.Sprintf("%s:source.available", asset.ID), Type: "source.available", AssetID: asset.ID,
		Generation: 0, OwnerKey: asset.OwnerKey, ClientNamespace: asset.ClientNamespace, OwnerRef: asset.OwnerRef,
	})
	if run, runErr := s.metadata.GetLatestProcessingRun(ctx, asset.ID); runErr == nil && run != nil && run.Status == domain.ProcessingWaitingSource {
		run.Status = domain.ProcessingQueued
		if s.metadata.UpdateProcessingRun(ctx, run) == nil {
			_ = s.jobs.PublishAsset(ctx, asset.ID, run.Generation)
		}
	}
	return asset, nil
}

func (s *service) verifySourceCodec(ctx context.Context, asset *domain.MediaAsset, blob *domain.Blob) error {
	if blob.SHA256 == nil || s.blobs == nil {
		return fmt.Errorf("source blob is unavailable for verification")
	}
	dir, err := os.MkdirTemp("", "media-verify-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	input := filepath.Join(dir, "source")
	stream, err := s.blobs.GetBlobStream(ctx, canonicalKey(*blob.SHA256))
	if err != nil {
		return err
	}
	out, err := os.Create(input)
	if err != nil {
		_ = stream.Close()
		return err
	}
	_, copyErr := io.Copy(out, io.LimitReader(stream, 5*1024*1024*1024))
	_ = out.Close()
	_ = stream.Close()
	if copyErr != nil {
		return copyErr
	}
	selector := "a:0"
	if asset.DeclaredKind == domain.MediaKindVideo || asset.DeclaredKind == domain.MediaKindImage {
		selector = "v:0"
	}
	data, err := exec.CommandContext(
		ctx,
		"ffprobe", "-v", "error", "-select_streams", selector,
		"-show_entries", "stream=codec_name,width,height,duration:format=duration",
		"-of", "json", input,
	).Output()
	if err != nil {
		return fmt.Errorf("ffprobe rejected source: %w", err)
	}
	var probe struct {
		Streams []struct {
			CodecName string `json:"codec_name"`
			Width     int    `json:"width"`
			Height    int    `json:"height"`
			Duration  string `json:"duration"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(data, &probe); err != nil || len(probe.Streams) == 0 {
		return fmt.Errorf("ffprobe returned no usable source stream")
	}
	streamInfo := probe.Streams[0]
	codec := strings.ToLower(strings.TrimSpace(streamInfo.CodecName))
	if asset.DeclaredKind == domain.MediaKindImage {
		if streamInfo.Width <= 0 || streamInfo.Height <= 0 {
			return fmt.Errorf("image dimensions are unavailable")
		}
		asset.Width = streamInfo.Width
		asset.Height = streamInfo.Height
	}
	if asset.DeclaredKind == domain.MediaKindVideo {
		allowedVideo := map[string]bool{"h264": true}
		if asset.SourcePolicyID == "browser-capture-v1" {
			allowedVideo["vp8"] = true
			allowedVideo["vp9"] = true
			allowedVideo["av1"] = true
		}
		if !allowedVideo[codec] {
			return fmt.Errorf("video codec %s is not supported", codec)
		}
		if streamInfo.Width <= 0 || streamInfo.Height <= 0 {
			return fmt.Errorf("video dimensions are unavailable")
		}
		asset.Width = streamInfo.Width
		asset.Height = streamInfo.Height
	}
	if asset.DeclaredKind == domain.MediaKindAudio && codec != "aac" && codec != "mp3" {
		return fmt.Errorf("audio codec %s is not supported", codec)
	}
	duration := streamInfo.Duration
	if duration == "" || duration == "N/A" {
		duration = probe.Format.Duration
	}
	if seconds, parseErr := strconv.ParseFloat(duration, 64); parseErr == nil && seconds > 0 {
		asset.DurationMS = int64(math.Round(seconds * 1000))
	}
	return nil
}

func (s *service) ListLifecycleEvents(ctx context.Context, afterSequence int64, limit int) ([]domain.AssetLifecycleEvent, error) {
	if afterSequence < 0 {
		afterSequence = 0
	}
	if limit < 1 {
		limit = 1
	}
	if limit > 500 {
		limit = 500
	}
	return s.metadata.ListAssetLifecycleEvents(ctx, afterSequence, limit)
}

func (s *service) GetSource(ctx context.Context, assetID uuid.UUID, namespace, ownerRef string) (*domain.MediaAsset, *domain.ProcessingRun, error) {
	asset, err := s.ownedAssetV2(ctx, assetID, namespace, ownerRef)
	if err != nil {
		return nil, nil, err
	}
	run, err := s.metadata.GetLatestProcessingRun(ctx, assetID)
	return asset, run, err
}

func (s *service) RequestProcessing(ctx context.Context, assetID uuid.UUID, namespace, ownerRef string, pipeline domain.ProcessingProfile, idempotencyKey string) (*domain.ProcessingRun, error) {
	asset, err := s.ownedAssetV2(ctx, assetID, namespace, ownerRef)
	if err != nil {
		return nil, err
	}
	if !pipelineAcceptsKind(pipeline, asset.DeclaredKind) {
		return nil, fmt.Errorf("pipeline %s does not accept %s", pipeline, asset.DeclaredKind)
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if len(idempotencyKey) < 8 || len(idempotencyKey) > 200 {
		return nil, fmt.Errorf("idempotency_key is invalid")
	}
	if existing, err := s.metadata.GetProcessingRunByIdempotency(ctx, namespace, idempotencyKey); err != nil {
		return nil, err
	} else if existing != nil {
		if existing.AssetID != assetID || existing.PipelineID != pipeline {
			return nil, fmt.Errorf("idempotency_key belongs to another processing request")
		}
		return existing, nil
	}
	latest, err := s.metadata.GetLatestProcessingRun(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if latest != nil && latest.Status == domain.ProcessingReady && latest.PipelineID == pipeline {
		return latest, nil
	}
	generation := int64(1)
	if latest != nil {
		generation = latest.Generation + 1
	}
	status := domain.ProcessingWaitingSource
	if asset.SourceStatus == domain.SourceRejected {
		return nil, fmt.Errorf("asset source was rejected")
	}
	if asset.SourceStatus == domain.SourceAvailable {
		status = domain.ProcessingQueued
	}
	now := time.Now()
	run := &domain.ProcessingRun{ID: uuid.Must(uuid.NewV7()), AssetID: assetID, ClientNamespace: namespace, OwnerRef: ownerRef,
		PipelineID: pipeline, PipelineVersion: "1", Generation: generation, IdempotencyKey: idempotencyKey,
		Status: status, CreatedAt: now, UpdatedAt: now}
	if err := s.metadata.CreateProcessingRun(ctx, run); err != nil {
		return nil, err
	}
	asset.Profile = pipeline
	asset.Generation = generation
	asset.Status = domain.AssetProcessing
	if err := s.metadata.UpdateMediaAsset(ctx, asset); err != nil {
		return nil, err
	}
	if status == domain.ProcessingQueued {
		if err := s.jobs.PublishAsset(ctx, asset.ID, generation); err != nil {
			return nil, fmt.Errorf("enqueue processing: %w", err)
		}
	}
	return run, nil
}

func (s *service) GetProcessingRun(ctx context.Context, runID uuid.UUID, namespace, ownerRef string) (*domain.ProcessingRun, error) {
	run, err := s.metadata.GetProcessingRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run == nil || run.ClientNamespace != namespace || run.OwnerRef != ownerRef {
		return nil, fmt.Errorf("processing run not found")
	}
	return run, nil
}

func (s *service) RetryProcessing(ctx context.Context, runID uuid.UUID, namespace, ownerRef, idempotencyKey string) (*domain.ProcessingRun, error) {
	run, err := s.GetProcessingRun(ctx, runID, namespace, ownerRef)
	if err != nil {
		return nil, err
	}
	if run.Status != domain.ProcessingFailed && run.Status != domain.ProcessingCancelled {
		return nil, fmt.Errorf("only failed or cancelled processing can be retried")
	}
	return s.RequestProcessing(ctx, run.AssetID, namespace, ownerRef, run.PipelineID, idempotencyKey)
}

func (s *service) CancelProcessing(ctx context.Context, runID uuid.UUID, namespace, ownerRef string) (*domain.ProcessingRun, error) {
	run, err := s.GetProcessingRun(ctx, runID, namespace, ownerRef)
	if err != nil {
		return nil, err
	}
	if run.Status == domain.ProcessingReady || run.Status == domain.ProcessingFailed {
		return run, nil
	}
	run.Status = domain.ProcessingCancelled
	if err := s.metadata.UpdateProcessingRun(ctx, run); err != nil {
		return nil, err
	}
	_ = s.metadata.AppendAssetLifecycleEvent(ctx, runEvent(run, "processing.cancelled", ""))
	return run, nil
}

func (s *service) ResolveDelivery(ctx context.Context, assetID uuid.UUID, generation int64, variantName, namespace, ownerRef string) (string, string, error) {
	if _, err := s.ownedAssetV2(ctx, assetID, namespace, ownerRef); err != nil {
		return "", "", err
	}
	variant, err := s.metadata.GetMediaAssetVariant(ctx, assetID, generation, strings.TrimSpace(variantName))
	if err != nil || variant == nil {
		return "", "", fmt.Errorf("delivery variant not found")
	}
	url, err := s.uploads.GetDownloadURL(ctx, variant.BlobID, namespace+":"+ownerRef)
	return url, variant.MimeType, err
}

func (s *service) GetDeliveryManifest(ctx context.Context, assetID uuid.UUID, generation int64, namespace, ownerRef string) ([]domain.MediaAssetVariant, error) {
	if _, err := s.ownedAssetV2(ctx, assetID, namespace, ownerRef); err != nil {
		return nil, err
	}
	return s.metadata.ListMediaAssetVariantsForGeneration(ctx, assetID, generation)
}

func (s *service) ReleaseSource(ctx context.Context, assetID uuid.UUID, generation int64, namespace, ownerRef string) error {
	asset, err := s.ownedAssetV2(ctx, assetID, namespace, ownerRef)
	if err != nil {
		return err
	}
	run, err := s.metadata.GetLatestProcessingRun(ctx, assetID)
	if err != nil {
		return err
	}
	if run == nil || run.Generation != generation || run.Status != domain.ProcessingReady {
		return fmt.Errorf("processing generation is not ready")
	}
	source, err := s.metadata.ReleaseAssetOriginal(ctx, asset.ID)
	if err != nil || source == nil || source.SHA256 == nil || s.blobs == nil {
		return err
	}
	if err := s.blobs.DeleteBlob(ctx, canonicalKey(*source.SHA256)); err != nil {
		return err
	}
	return s.metadata.DeleteBlobRecord(ctx, source.ID)
}

func (s *service) ResolveSource(ctx context.Context, assetID uuid.UUID, namespace, ownerRef string) (string, string, error) {
	asset, err := s.ownedAssetV2(ctx, assetID, namespace, ownerRef)
	if err != nil {
		return "", "", err
	}
	if asset.SourceBlobID == nil || asset.SourceStatus != domain.SourceAvailable {
		return "", "", fmt.Errorf("asset source is not available")
	}
	url, err := s.uploads.GetDownloadURL(ctx, *asset.SourceBlobID, namespace+":"+ownerRef)
	return url, asset.MimeType, err
}

func (s *service) ListLifecycleEventsForNamespace(ctx context.Context, namespace string, afterSequence int64, limit int) ([]domain.AssetLifecycleEvent, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 500 {
		limit = 500
	}
	return s.metadata.ListAssetLifecycleEventsForNamespace(ctx, namespace, afterSequence, limit)
}

func (s *service) ownedAssetV2(ctx context.Context, assetID uuid.UUID, namespace, ownerRef string) (*domain.MediaAsset, error) {
	asset, err := s.metadata.GetMediaAsset(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if asset == nil || asset.ClientNamespace != namespace || asset.OwnerRef != ownerRef {
		return nil, fmt.Errorf("media asset not found")
	}
	return asset, nil
}

func validateSourcePolicy(policy string, kind domain.MediaKind, mime string) error {
	allowed := map[domain.MediaKind]map[string]bool{
		domain.MediaKindImage: {"image/jpeg": true, "image/png": true, "image/webp": true},
		domain.MediaKindVideo: {"video/mp4": true},
		domain.MediaKindAudio: {"audio/mpeg": true, "audio/mp4": true, "audio/aac": true, "audio/x-m4a": true},
	}
	if policy == "browser-capture-v1" {
		allowed[domain.MediaKindVideo]["video/webm"] = true
	}
	if !allowed[kind][mime] {
		return fmt.Errorf("%s is not supported by %s for %s", mime, policy, kind)
	}
	return nil
}

func validateBrowserNative(kind domain.MediaKind, mime string) error {
	return validateSourcePolicy("browser-native-v1", kind, mime)
}

func pipelineAcceptsKind(p domain.ProcessingProfile, kind domain.MediaKind) bool {
	return (p == domain.PipelineImageResponsiveWebV1 && kind == domain.MediaKindImage) ||
		(p == domain.PipelineVideoWeb1080V1 && kind == domain.MediaKindVideo) ||
		(p == domain.PipelineAudioWebV1 && kind == domain.MediaKindAudio)
}

func kindForPipeline(p domain.ProcessingProfile) domain.MediaKind {
	switch p {
	case domain.ProcessingProfileContentVideo, domain.PipelineVideoWeb1080V1:
		return domain.MediaKindVideo
	case domain.ProcessingProfileContentAudio, domain.PipelineAudioWebV1:
		return domain.MediaKindAudio
	default:
		return domain.MediaKindImage
	}
}

func runEvent(run *domain.ProcessingRun, eventType, failureCode string) *domain.AssetLifecycleEvent {
	return &domain.AssetLifecycleEvent{EventID: fmt.Sprintf("%s:g%d:%s", run.ID, run.Generation, eventType), Type: eventType,
		AssetID: run.AssetID, RunID: &run.ID, Generation: run.Generation, OwnerKey: run.ClientNamespace + ":" + run.OwnerRef,
		ClientNamespace: run.ClientNamespace, OwnerRef: run.OwnerRef, FailureCode: failureCode}
}

func (s *service) ownedAsset(ctx context.Context, assetID uuid.UUID, ownerKey string) (*domain.MediaAsset, error) {
	asset, err := s.metadata.GetMediaAsset(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if asset == nil {
		return nil, fmt.Errorf("media asset not found")
	}
	if asset.OwnerKey != ownerKey {
		return nil, fmt.Errorf("media asset is not owned by caller")
	}
	return asset, nil
}

func validateProfile(profile domain.ProcessingProfile, mimeType string) error {
	switch profile {
	case domain.ProcessingProfileContentImage:
		if strings.HasPrefix(mimeType, "image/") {
			return nil
		}
	case domain.ProcessingProfileContentVideo:
		if strings.HasPrefix(mimeType, "video/") {
			return nil
		}
	case domain.ProcessingProfileContentAudio:
		if strings.HasPrefix(mimeType, "audio/") {
			return nil
		}
	default:
		return fmt.Errorf("unsupported processing profile %q", profile)
	}
	return fmt.Errorf("profile %s does not accept %s", profile, mimeType)
}
