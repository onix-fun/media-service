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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/onix-fun/media-service/internal/config"
	"github.com/onix-fun/media-service/internal/domain"
	"github.com/onix-fun/media-service/internal/storage"
)

type ProfileWorker struct {
	metadata storage.MetadataRepo
	blobs    storage.BlobStorage
	profiles map[string]config.Profile
}

func NewProfileWorker(metadata storage.MetadataRepo, blobs storage.BlobStorage, profiles map[string]config.Profile) *ProfileWorker {
	return &ProfileWorker{metadata: metadata, blobs: blobs, profiles: profiles}
}

func (w *ProfileWorker) Process(ctx context.Context, sourceID uuid.UUID, name string) error {
	profile, ok := w.profiles[name]
	if !ok {
		return fmt.Errorf("unknown processing profile %q", name)
	}
	source, err := w.metadata.GetBlob(ctx, sourceID)
	if err != nil || source == nil || source.SHA256 == nil {
		return fmt.Errorf("source blob is not ready: %w", err)
	}
	dir, err := os.MkdirTemp("", "media-process-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	input := filepath.Join(dir, "input")
	output := filepath.Join(dir, "output"+profile.OutputExtension)
	key := canonicalKey(*source.SHA256)
	stream, err := w.blobs.GetBlobStream(ctx, key)
	if err != nil {
		return err
	}
	in, err := os.Create(input)
	if err != nil {
		_ = stream.Close()
		return err
	}
	_, err = io.Copy(in, stream)
	_ = in.Close()
	_ = stream.Close()
	if err != nil {
		return err
	}
	args := make([]string, len(profile.Command))
	for i, arg := range profile.Command {
		args[i] = strings.ReplaceAll(strings.ReplaceAll(arg, "{input}", input), "{output}", output)
	}
	if len(args) == 0 {
		return fmt.Errorf("profile command is empty")
	}
	if data, err := exec.CommandContext(ctx, args[0], args[1:]...).CombinedOutput(); err != nil {
		return fmt.Errorf("profile command failed: %w: %s", err, data)
	}
	out, err := os.Open(output)
	if err != nil {
		return err
	}
	defer out.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, out)
	if err != nil {
		return err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if _, err = out.Seek(0, 0); err != nil {
		return err
	}
	if err = w.blobs.PutBlob(ctx, canonicalKey(digest), out, size, profile.OutputMIME); err != nil {
		return err
	}
	target := &domain.Blob{ID: uuid.Must(uuid.NewV7()), SHA256: &digest, SizeBytes: &size, MimeType: profile.OutputMIME, RetentionState: domain.RetentionReferenced, UploadStatus: domain.UploadReady, CreatedByService: source.CreatedByService, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err = w.metadata.CreateBlob(ctx, target); err != nil {
		if !w.metadata.IsUniqueViolation(err) {
			return err
		}
		target, err = w.metadata.GetBlobBySHA256(ctx, digest)
		if err != nil || target == nil {
			return fmt.Errorf("load deduplicated output: %w", err)
		}
	}
	if err = w.metadata.CreateBlobRelation(ctx, sourceID, target.ID, name); err != nil {
		return err
	}
	return w.metadata.GrantBlobAccess(ctx, target.ID, source.CreatedByService)
}

func canonicalKey(hash string) string {
	return fmt.Sprintf("blobs/%s/%s/%s", hash[:2], hash[2:4], hash)
}
