package asset

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/onix-fun/media/internal/application/storage"
	"github.com/onix-fun/media/internal/application/worker"
	"github.com/onix-fun/media/internal/domain"
)

// Pipeline turns a ready source blob into configured Content delivery
// variants. The source is only released after every configured variant has
// been persisted successfully.
type Pipeline struct {
	metadata storage.MetadataRepo
	blobs    storage.BlobStorage
	profiles *worker.ProfileWorker
}

func NewPipeline(metadata storage.MetadataRepo, blobs storage.BlobStorage, profiles *worker.ProfileWorker) *Pipeline {
	return &Pipeline{metadata: metadata, blobs: blobs, profiles: profiles}
}

func (p *Pipeline) Process(ctx context.Context, assetID uuid.UUID, generation int64) (err error) {
	return p.process(ctx, assetID, generation, true)
}

// ProcessQueued gives transient infrastructure failures exactly one automatic
// queue retry. The first attempt keeps the run PROCESSING and emits no terminal
// event; the second attempt records the safe failure for consumers.
func (p *Pipeline) ProcessQueued(ctx context.Context, assetID uuid.UUID, generation, previousAttempts int64) error {
	return p.process(ctx, assetID, generation, previousAttempts >= 1)
}

func (p *Pipeline) process(ctx context.Context, assetID uuid.UUID, generation int64, terminalTransient bool) (err error) {
	asset, err := p.metadata.GetMediaAsset(ctx, assetID)
	if err != nil {
		return err
	}
	if asset == nil {
		return fmt.Errorf("media asset not found")
	}
	if asset.Generation != generation {
		// A retry has superseded this leased job.  Treat it as successfully
		// consumed; the newer generation owns all state transitions.
		return nil
	}
	run, err := p.metadata.GetLatestProcessingRun(ctx, assetID)
	if err != nil {
		return err
	}
	if asset.Status == domain.AssetReady {
		if run != nil {
			run.Status = domain.ProcessingReady
			if err := p.metadata.UpdateProcessingRun(ctx, run); err != nil {
				return err
			}
			return p.metadata.AppendAssetLifecycleEvent(ctx, runEvent(run, "processing.ready", ""))
		}
		return p.metadata.AppendAssetLifecycleEvent(ctx, lifecycleEvent(asset, "asset.ready", ""))
	}
	if run != nil {
		if run.Generation != generation || run.Status == domain.ProcessingCancelled {
			return nil
		}
		run.Status = domain.ProcessingRunning
		run.FailureCode = ""
		if err := p.metadata.UpdateProcessingRun(ctx, run); err != nil {
			return err
		}
	}
	if asset.SourceBlobID == nil {
		return p.fail(ctx, asset, fmt.Errorf("asset source is not ready"), terminalTransient)
	}
	asset.Status = domain.AssetProcessing
	asset.FailureReason = ""
	asset.FailedAt = nil
	if err := p.metadata.UpdateMediaAsset(ctx, asset); err != nil {
		return err
	}

	var primary *worker.ProcessedOutput
	for _, spec := range deliverySpecs(asset.Profile) {
		output, processErr := p.profiles.ProcessDelivery(ctx, *asset.SourceBlobID, string(asset.Profile), spec.name)
		if processErr != nil {
			return p.fail(ctx, asset, processErr, terminalTransient)
		}
		// A source blob can be deduplicated from a different owner. Derivatives
		// inherit the source owner in the generic worker, so grant the asset's
		// actual owner explicitly before exposing its delivery URL.
		if err := p.metadata.GrantBlobAccess(ctx, output.Blob.ID, asset.OwnerKey); err != nil {
			return p.fail(ctx, asset, err, terminalTransient)
		}
		variant := &domain.MediaAssetVariant{
			AssetID:    asset.ID,
			Generation: generation,
			Name:       spec.name,
			BlobID:     output.Blob.ID,
			MimeType:   output.Blob.MimeType,
			Width:      output.Width,
			Height:     output.Height,
			DurationMS: output.DurationMS,
			Bitrate:    output.Bitrate,
		}
		if err := p.metadata.UpsertMediaAssetVariant(ctx, variant); err != nil {
			return p.fail(ctx, asset, err, terminalTransient)
		}
		if spec.primary {
			primary = output
		}
	}
	if primary != nil {
		asset.Width = primary.Width
		asset.Height = primary.Height
		asset.DurationMS = primary.DurationMS
	}
	asset.Status = domain.AssetReady
	asset.FailureReason = ""
	asset.FailedAt = nil
	if err := p.metadata.UpdateMediaAsset(ctx, asset); err != nil {
		return err
	}
	if run != nil {
		run.Status = domain.ProcessingReady
		if err := p.metadata.UpdateProcessingRun(ctx, run); err != nil {
			return err
		}
	}
	event := lifecycleEvent(asset, "asset.ready", "")
	if run != nil {
		event = runEvent(run, "processing.ready", "")
	}
	if err := p.metadata.AppendAssetLifecycleEvent(ctx, event); err != nil {
		return err
	}

	// The source stays available until the consumer acknowledges that its
	// publication transaction committed through the generic ReleaseSource API.
	return nil
}

func (p *Pipeline) fail(ctx context.Context, asset *domain.MediaAsset, cause error, terminalTransient bool) error {
	var permanent interface{ Permanent() bool }
	if !terminalTransient && (!errors.As(cause, &permanent) || !permanent.Permanent()) {
		return cause
	}
	asset.Status = domain.AssetFailed
	asset.FailureReason = truncateFailure(cause.Error())
	now := time.Now()
	asset.FailedAt = &now
	if updateErr := p.metadata.UpdateMediaAsset(ctx, asset); updateErr != nil {
		return fmt.Errorf("%w (record asset failure: %v)", cause, updateErr)
	}
	event := lifecycleEvent(asset, "asset.failed", failureCode(cause))
	if run, runErr := p.metadata.GetLatestProcessingRun(ctx, asset.ID); runErr == nil && run != nil && run.Generation == asset.Generation {
		run.Status = domain.ProcessingFailed
		run.FailureCode = failureCode(cause)
		_ = p.metadata.UpdateProcessingRun(ctx, run)
		event = runEvent(run, "processing.failed", run.FailureCode)
	}
	if eventErr := p.metadata.AppendAssetLifecycleEvent(ctx, event); eventErr != nil {
		return fmt.Errorf("%w (record asset lifecycle event: %v)", cause, eventErr)
	}
	return cause
}

func lifecycleEvent(asset *domain.MediaAsset, eventType, failureCode string) *domain.AssetLifecycleEvent {
	return &domain.AssetLifecycleEvent{
		EventID: fmt.Sprintf("%s:g%d:%s", asset.ID, asset.Generation, eventType),
		Type:    eventType, AssetID: asset.ID, Generation: asset.Generation,
		OwnerKey: asset.OwnerKey, FailureCode: failureCode,
	}
}

func failureCode(cause error) string {
	value := strings.ToLower(cause.Error())
	switch {
	case strings.Contains(value, "invalid"), strings.Contains(value, "unsupported"), strings.Contains(value, "profile command"):
		return "UNSUPPORTED_OR_MALFORMED_MEDIA"
	case strings.Contains(value, "deadline"), strings.Contains(value, "timeout"):
		return "PROCESSING_TIMEOUT"
	default:
		return "PROCESSING_FAILED"
	}
}

type deliverySpec struct {
	name    string
	primary bool
}

func deliverySpecs(profile domain.ProcessingProfile) []deliverySpec {
	switch profile {
	case domain.ProcessingProfileContentImage, domain.PipelineImageResponsiveWebV1:
		return []deliverySpec{
			{name: "image-480"},
			{name: "image-960"},
			{name: "image-1440"},
			{name: "image-2048", primary: true},
		}
	case domain.ProcessingProfileContentVideo, domain.PipelineVideoWeb1080V1:
		return []deliverySpec{
			{name: "video-1080", primary: true},
			{name: "poster"},
		}
	case domain.ProcessingProfileContentAudio, domain.PipelineAudioWebV1:
		return []deliverySpec{
			{name: "audio", primary: true},
			{name: "waveform"},
		}
	default:
		return nil
	}
}

func canonicalKey(hash string) string {
	return fmt.Sprintf("blobs/%s/%s/%s", hash[:2], hash[2:4], hash)
}

func truncateFailure(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 1000 {
		return value
	}
	return value[:1000]
}
