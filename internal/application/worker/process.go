package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/onix-fun/media/internal/application/storage"
	"github.com/onix-fun/media/internal/domain"
	"github.com/onix-fun/media/internal/platform/config"
)

type ProfileWorker struct {
	metadata storage.MetadataRepo
	blobs    storage.BlobStorage
	profiles map[string]config.Profile
}

// ProcessedOutput describes a delivery blob together with best-effort metadata
// collected from the generated file. A missing ffprobe binary never prevents a
// derivative from becoming available; metadata remains zero in that case.
type ProcessedOutput struct {
	Blob       *domain.Blob
	Width      int
	Height     int
	DurationMS int64
	Bitrate    int64
}

// ConversionError is deliberately small so the queue can distinguish a
// corrupt/unsupported upload from a transient storage or timeout failure.
type ConversionError struct{ cause error }

func (e ConversionError) Error() string   { return e.cause.Error() }
func (e ConversionError) Unwrap() error   { return e.cause }
func (e ConversionError) Permanent() bool { return true }

func NewProfileWorker(metadata storage.MetadataRepo, blobs storage.BlobStorage, profiles map[string]config.Profile) *ProfileWorker {
	return &ProfileWorker{metadata: metadata, blobs: blobs, profiles: profiles}
}

func (w *ProfileWorker) Process(ctx context.Context, sourceID uuid.UUID, name string) error {
	_, err := w.ProcessOutput(ctx, sourceID, name)
	return err
}

// ProcessOutput reuses configured profiles for asset delivery variants while
// returning the resulting blob identity to the asset pipeline.
func (w *ProfileWorker) ProcessOutput(ctx context.Context, sourceID uuid.UUID, name string) (*ProcessedOutput, error) {
	profile, ok := w.profiles[name]
	if !ok {
		return nil, fmt.Errorf("unknown processing profile %q", name)
	}
	return w.processOutput(ctx, sourceID, name, profile)
}

// ProcessContent runs the closed, versioned Content delivery pipeline.  It is
// intentionally not driven by YAML commands: configuration may tune worker
// capacity, but it must not be able to change the safety or codec contract of
// public Content assets.
func (w *ProfileWorker) ProcessContent(ctx context.Context, sourceID uuid.UUID, variant string) (*ProcessedOutput, error) {
	profile, err := contentV1Profile(variant)
	if err != nil {
		return nil, err
	}
	return w.processOutput(ctx, sourceID, "content-v1:"+variant, profile)
}

// ProcessDelivery executes a product-neutral, code-owned pipeline. Consumer
// services select a stable pipeline id; they never provide executable commands.
func (w *ProfileWorker) ProcessDelivery(ctx context.Context, sourceID uuid.UUID, pipelineID, variant string) (*ProcessedOutput, error) {
	switch pipelineID {
	case "CONTENT_IMAGE":
		pipelineID = "image-responsive-web-v1"
	case "CONTENT_VIDEO":
		pipelineID = "video-web-1080-v1"
	case "CONTENT_AUDIO":
		pipelineID = "audio-web-v1"
	}
	profile, err := deliveryProfile(pipelineID, variant)
	if err != nil {
		return nil, err
	}
	return w.processOutput(ctx, sourceID, pipelineID+":"+variant, profile)
}

func (w *ProfileWorker) processOutput(ctx context.Context, sourceID uuid.UUID, name string, profile config.Profile) (*ProcessedOutput, error) {
	source, err := w.metadata.GetBlob(ctx, sourceID)
	if err != nil || source == nil || source.SHA256 == nil {
		return nil, fmt.Errorf("source blob is not ready: %w", err)
	}
	dir, err := os.MkdirTemp("", "media-process-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	input := filepath.Join(dir, "input")
	output := filepath.Join(dir, "output"+profile.OutputExtension)
	key := canonicalKey(*source.SHA256)
	stream, err := w.blobs.GetBlobStream(ctx, key)
	if err != nil {
		return nil, err
	}
	in, err := os.Create(input)
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	_, err = io.Copy(in, stream)
	_ = in.Close()
	_ = stream.Close()
	if err != nil {
		return nil, err
	}
	args := make([]string, len(profile.Command))
	for i, arg := range profile.Command {
		args[i] = strings.ReplaceAll(strings.ReplaceAll(arg, "{input}", input), "{output}", output)
	}
	if len(args) == 0 {
		return nil, fmt.Errorf("profile command is empty")
	}
	if data, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ConversionError{cause: fmt.Errorf("profile command failed: %w: %s", err, data)}
	}
	out, err := os.Open(output)
	if err != nil {
		return nil, err
	}
	defer out.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, out)
	if err != nil {
		return nil, err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if _, err = out.Seek(0, 0); err != nil {
		return nil, err
	}
	if err = w.blobs.PutBlob(ctx, canonicalKey(digest), out, size, profile.OutputMIME); err != nil {
		return nil, err
	}
	target := &domain.Blob{ID: uuid.Must(uuid.NewV7()), SHA256: &digest, SizeBytes: &size, MimeType: profile.OutputMIME, RetentionState: domain.RetentionReferenced, UploadStatus: domain.UploadReady, CreatedByService: source.CreatedByService, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	target, _, err = w.metadata.CreateOrGetBlob(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("resolve processed output blob: %w", err)
	}
	if err = w.metadata.CreateBlobRelation(ctx, sourceID, target.ID, name); err != nil {
		return nil, err
	}
	if err = w.metadata.GrantBlobAccess(ctx, target.ID, source.CreatedByService); err != nil {
		return nil, err
	}
	metadata := probeOutput(ctx, output)
	return &ProcessedOutput{Blob: target, Width: metadata.Width, Height: metadata.Height, DurationMS: metadata.DurationMS, Bitrate: metadata.Bitrate}, nil
}

// contentV1Profile is deliberately code-owned.  In particular vipsthumbnail
// does not support a --strip switch; libvips strips metadata through the WebP
// saver option in the output specification.  The FFmpeg scale filters cap
// dimensions without upscaling and force codec-safe even dimensions.
func contentV1Profile(variant string) (config.Profile, error) {
	for _, pipelineID := range []string{"image-responsive-web-v1", "video-web-1080-v1", "audio-web-v1"} {
		if profile, err := deliveryProfile(pipelineID, variant); err == nil {
			return profile, nil
		}
	}
	return config.Profile{}, fmt.Errorf("unknown delivery variant %q", variant)
}

func deliveryProfile(pipelineID, variant string) (config.Profile, error) {
	webpImage := func(size string) config.Profile {
		return config.Profile{
			Kind: "image", OutputExtension: ".webp", OutputMIME: "image/webp",
			Command: []string{"vipsthumbnail", "{input}", "--size", size + "x" + size + ">", "-o", "{output}[Q=85,strip]"},
		}
	}
	even1080 := "scale=w='min(1920,iw)':h='min(1080,ih)':force_original_aspect_ratio=decrease:force_divisible_by=2"
	evenPoster := "scale=w='min(1440,iw)':h='min(1080,ih)':force_original_aspect_ratio=decrease:force_divisible_by=2"
	switch pipelineID + ":" + variant {
	case "image-responsive-web-v1:image-480":
		return webpImage("480"), nil
	case "image-responsive-web-v1:image-960":
		return webpImage("960"), nil
	case "image-responsive-web-v1:image-1440":
		return webpImage("1440"), nil
	case "image-responsive-web-v1:image-2048":
		return webpImage("2048"), nil
	case "video-web-1080-v1:video-1080":
		return config.Profile{Kind: "video", OutputExtension: ".mp4", OutputMIME: "video/mp4", Command: []string{
			"ffmpeg", "-y", "-i", "{input}", "-map", "0:v:0", "-map", "0:a?", "-vf", even1080,
			"-c:v", "libx264", "-pix_fmt", "yuv420p", "-preset", "medium", "-crf", "23",
			"-c:a", "aac", "-b:a", "192k", "-movflags", "+faststart", "{output}",
		}}, nil
	case "video-web-1080-v1:poster":
		return config.Profile{Kind: "video", OutputExtension: ".webp", OutputMIME: "image/webp", Command: []string{
			"ffmpeg", "-y", "-ss", "0", "-i", "{input}", "-map", "0:v:0", "-frames:v", "1", "-vf", evenPoster,
			"-c:v", "libwebp", "-q:v", "82", "{output}",
		}}, nil
	case "audio-web-v1:audio":
		return config.Profile{Kind: "audio", OutputExtension: ".m4a", OutputMIME: "audio/mp4", Command: []string{
			"ffmpeg", "-y", "-i", "{input}", "-map", "0:a:0", "-vn", "-c:a", "aac", "-b:a", "192k", "{output}",
		}}, nil
	case "audio-web-v1:waveform":
		return config.Profile{Kind: "audio", OutputExtension: ".png", OutputMIME: "image/png", Command: []string{
			"ffmpeg", "-y", "-i", "{input}", "-map", "0:a:0", "-filter_complex", "aformat=channel_layouts=mono,showwavespic=s=1200x240:colors=111111", "-frames:v", "1", "-c:v", "png", "{output}",
		}}, nil
	default:
		return config.Profile{}, fmt.Errorf("unknown delivery variant %s:%s", pipelineID, variant)
	}
}

type outputMetadata struct {
	Width      int
	Height     int
	DurationMS int64
	Bitrate    int64
}

func probeOutput(ctx context.Context, output string) outputMetadata {
	// ffprobe supports images, video and audio. Processing remains functional
	// in intentionally minimal worker images without it.
	data, err := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries", "stream=width,height,bit_rate:format=duration,bit_rate", "-of", "default=noprint_wrappers=1:nokey=0", output).Output()
	if err != nil {
		return outputMetadata{}
	}
	values := make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			values[key] = value
		}
	}
	metadata := outputMetadata{}
	if value, err := strconv.Atoi(values["width"]); err == nil {
		metadata.Width = value
	}
	if value, err := strconv.Atoi(values["height"]); err == nil {
		metadata.Height = value
	}
	if value, err := strconv.ParseFloat(values["duration"], 64); err == nil {
		metadata.DurationMS = int64(value * 1000)
	}
	if value, err := strconv.ParseInt(values["bit_rate"], 10, 64); err == nil {
		metadata.Bitrate = value
	}
	return metadata
}

func canonicalKey(hash string) string {
	return fmt.Sprintf("blobs/%s/%s/%s", hash[:2], hash[2:4], hash)
}
