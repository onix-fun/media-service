package asset

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/onix-fun/media/internal/application/storage"
	"github.com/onix-fun/media/internal/application/upload"
	"github.com/onix-fun/media/internal/domain"
)

func TestValidateProfileAcceptsOnlyMatchingMediaFamilies(t *testing.T) {
	tests := []struct {
		name    string
		profile domain.ProcessingProfile
		mime    string
		valid   bool
	}{
		{name: "image", profile: domain.ProcessingProfileContentImage, mime: "image/heic", valid: true},
		{name: "video", profile: domain.ProcessingProfileContentVideo, mime: "video/quicktime", valid: true},
		{name: "audio", profile: domain.ProcessingProfileContentAudio, mime: "audio/flac", valid: true},
		{name: "mismatched", profile: domain.ProcessingProfileContentImage, mime: "video/mp4", valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateProfile(test.profile, test.mime)
			if (err == nil) != test.valid {
				t.Fatalf("validateProfile(%q, %q) error = %v, valid = %t", test.profile, test.mime, err, test.valid)
			}
		})
	}
}

func TestCompletingUploadOnlyStartsSourceVerification(t *testing.T) {
	assetID := uuid.Must(uuid.NewV7())
	sessionID := uuid.Must(uuid.NewV7())
	stored := &domain.MediaAsset{ID: assetID, UploadSessionID: &sessionID, OwnerKey: "content:owner", Status: domain.AssetUploading, SourceStatus: domain.SourceUploading}
	metadata := &completeMetadata{asset: stored}
	queue := &countingJobs{}
	service := NewService(metadata, completeUploads{}, queue)
	result, err := service.Complete(context.Background(), assetID, sessionID, []domain.UploadPart{{PartNumber: 1, ETag: "etag"}}, "content:owner")
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceStatus != domain.SourceVerifying || result.Status != domain.AssetVerifying {
		t.Fatalf("unexpected state: %s/%s", result.SourceStatus, result.Status)
	}
	if queue.published != 0 {
		t.Fatalf("conversion was queued while saving a draft")
	}
}

func TestRejectedSourceFailsWaitingProcessingRun(t *testing.T) {
	assetID := uuid.Must(uuid.NewV7())
	sessionID := uuid.Must(uuid.NewV7())
	blobID := uuid.Must(uuid.NewV7())
	runID := uuid.Must(uuid.NewV7())
	metadata := &rejectedSourceMetadata{
		asset: &domain.MediaAsset{
			ID: assetID, UploadSessionID: &sessionID, OwnerKey: "content:owner",
			ClientNamespace: "content", OwnerRef: "owner", DeclaredKind: domain.MediaKindImage,
			SourcePolicyID: "browser-native-v1", SourceStatus: domain.SourceVerifying, Status: domain.AssetVerifying,
		},
		session: &domain.UploadSession{ID: sessionID, BlobID: &blobID},
		blob:    &domain.Blob{ID: blobID, MimeType: "image/gif"},
		run: &domain.ProcessingRun{
			ID: runID, AssetID: assetID, ClientNamespace: "content", OwnerRef: "owner",
			PipelineID: domain.PipelineImageResponsiveWebV1, Generation: 1, Status: domain.ProcessingWaitingSource,
		},
	}
	result, err := NewService(metadata, completeUploads{}, &countingJobs{}).BeginProcessingForSession(context.Background(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceStatus != domain.SourceRejected || metadata.run.Status != domain.ProcessingFailed {
		t.Fatalf("rejected source left processing unresolved: source=%s run=%s", result.SourceStatus, metadata.run.Status)
	}
	if metadata.run.FailureCode != "UNSUPPORTED_OR_MALFORMED_MEDIA" {
		t.Fatalf("unexpected processing failure code %q", metadata.run.FailureCode)
	}
	if len(metadata.events) != 2 || metadata.events[0].Type != "source.rejected" || metadata.events[1].Type != "processing.failed" {
		t.Fatalf("unexpected lifecycle events: %#v", metadata.events)
	}
}

type completeMetadata struct {
	storage.MetadataRepo
	asset *domain.MediaAsset
}

type rejectedSourceMetadata struct {
	storage.MetadataRepo
	asset   *domain.MediaAsset
	session *domain.UploadSession
	blob    *domain.Blob
	run     *domain.ProcessingRun
	events  []*domain.AssetLifecycleEvent
}

func (m *rejectedSourceMetadata) GetMediaAssetByUploadSession(context.Context, uuid.UUID) (*domain.MediaAsset, error) {
	return m.asset, nil
}
func (m *rejectedSourceMetadata) GetUploadSession(context.Context, uuid.UUID) (*domain.UploadSession, error) {
	return m.session, nil
}
func (m *rejectedSourceMetadata) GetBlob(context.Context, uuid.UUID) (*domain.Blob, error) {
	return m.blob, nil
}
func (m *rejectedSourceMetadata) UpdateMediaAsset(_ context.Context, asset *domain.MediaAsset) error {
	m.asset = asset
	return nil
}
func (m *rejectedSourceMetadata) GetLatestProcessingRun(context.Context, uuid.UUID) (*domain.ProcessingRun, error) {
	return m.run, nil
}
func (m *rejectedSourceMetadata) UpdateProcessingRun(_ context.Context, run *domain.ProcessingRun) error {
	m.run = run
	return nil
}
func (m *rejectedSourceMetadata) AppendAssetLifecycleEvent(_ context.Context, event *domain.AssetLifecycleEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *completeMetadata) GetMediaAsset(context.Context, uuid.UUID) (*domain.MediaAsset, error) {
	return m.asset, nil
}
func (m *completeMetadata) UpdateMediaAsset(_ context.Context, asset *domain.MediaAsset) error {
	m.asset = asset
	return nil
}

type completeUploads struct{ upload.Service }

func (completeUploads) CompleteUpload(context.Context, uuid.UUID, []domain.UploadPart, string) error {
	return nil
}

type countingJobs struct{ published int }

func (j *countingJobs) PublishAsset(context.Context, uuid.UUID, int64) error {
	j.published++
	return nil
}

func TestDeliverySpecsProvideExpectedContentVariants(t *testing.T) {
	tests := []struct {
		profile domain.ProcessingProfile
		want    []string
	}{
		{domain.ProcessingProfileContentImage, []string{"image-480", "image-960", "image-1440", "image-2048"}},
		{domain.ProcessingProfileContentVideo, []string{"video-1080", "poster"}},
		{domain.ProcessingProfileContentAudio, []string{"audio", "waveform"}},
		{domain.PipelineImageResponsiveWebV1, []string{"image-480", "image-960", "image-1440", "image-2048"}},
		{domain.PipelineVideoWeb1080V1, []string{"video-1080", "poster"}},
		{domain.PipelineAudioWebV1, []string{"audio", "waveform"}},
	}
	for _, test := range tests {
		gotSpecs := deliverySpecs(test.profile)
		if len(gotSpecs) != len(test.want) {
			t.Fatalf("%s variants = %d, want %d", test.profile, len(gotSpecs), len(test.want))
		}
		for index, want := range test.want {
			if gotSpecs[index].name != want {
				t.Fatalf("%s variant %d = %q, want %q", test.profile, index, gotSpecs[index].name, want)
			}
		}
	}
}

func TestBrowserNativePolicyRejectsFormatsThatNeedDraftConversion(t *testing.T) {
	accepted := []struct {
		kind domain.MediaKind
		mime string
	}{
		{domain.MediaKindImage, "image/jpeg"}, {domain.MediaKindImage, "image/png"}, {domain.MediaKindImage, "image/webp"},
		{domain.MediaKindVideo, "video/mp4"}, {domain.MediaKindAudio, "audio/mpeg"}, {domain.MediaKindAudio, "audio/mp4"},
	}
	for _, item := range accepted {
		if err := validateBrowserNative(item.kind, item.mime); err != nil {
			t.Fatalf("%s rejected: %v", item.mime, err)
		}
	}
	for _, item := range []struct {
		kind domain.MediaKind
		mime string
	}{
		{domain.MediaKindImage, "image/heic"}, {domain.MediaKindImage, "image/gif"}, {domain.MediaKindVideo, "video/quicktime"}, {domain.MediaKindAudio, "audio/flac"},
	} {
		if err := validateBrowserNative(item.kind, item.mime); err == nil {
			t.Fatalf("%s must be rejected", item.mime)
		}
	}
}

func TestBrowserCapturePolicyAcceptsRecorderWebM(t *testing.T) {
	if err := validateSourcePolicy("browser-capture-v1", domain.MediaKindVideo, "video/webm"); err != nil {
		t.Fatalf("browser recorder WebM rejected: %v", err)
	}
	if err := validateSourcePolicy("browser-native-v1", domain.MediaKindVideo, "video/webm"); err == nil {
		t.Fatal("post source policy must not silently accept WebM")
	}
}

func TestValidateCompletedPartsRequiresDistinctETags(t *testing.T) {
	valid := []domain.UploadPart{{PartNumber: 1, ETag: "etag-a"}, {PartNumber: 2, ETag: "etag-b"}}
	if err := validateCompletedParts(valid); err != nil {
		t.Fatalf("valid multipart proof rejected: %v", err)
	}
	for _, invalid := range [][]domain.UploadPart{
		nil,
		{{PartNumber: 1, ETag: ""}},
		{{PartNumber: 1, ETag: "a"}, {PartNumber: 1, ETag: "b"}},
		{{PartNumber: 0, ETag: "a"}},
	} {
		if err := validateCompletedParts(invalid); err == nil {
			t.Fatalf("invalid multipart proof was accepted: %#v", invalid)
		}
	}
}
