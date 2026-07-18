package asset

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/onix-fun/media/internal/application/storage"
)

// FailedOriginalGC retains sources of failed conversions long enough for an
// owner retry, then releases them using the same shared-source safety check as
// a successful conversion.
type FailedOriginalGC struct {
	metadata  storage.MetadataRepo
	blobs     storage.BlobStorage
	interval  time.Duration
	retention time.Duration
	log       *slog.Logger
}

func NewFailedOriginalGC(metadata storage.MetadataRepo, blobs storage.BlobStorage, interval, retention time.Duration, log *slog.Logger) *FailedOriginalGC {
	return &FailedOriginalGC{metadata: metadata, blobs: blobs, interval: interval, retention: retention, log: log}
}

func (w *FailedOriginalGC) Start(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		w.collect(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *FailedOriginalGC) collect(ctx context.Context) {
	ids, err := w.metadata.ListExpiredFailedAssetIDs(ctx, w.retention, 100)
	if err != nil {
		w.log.Error("failed media source gc query failed", "error", err)
		return
	}
	for _, id := range ids {
		source, err := w.metadata.ReleaseAssetOriginal(ctx, id)
		if err != nil {
			w.log.Error("failed media source gc release failed", "asset_id", id, "error", err)
			continue
		}
		if source == nil || source.SHA256 == nil {
			continue
		}
		if err := w.blobs.DeleteBlob(ctx, canonicalKey(*source.SHA256)); err != nil {
			w.log.Error("failed media source gc delete failed", "asset_id", id, "error", err)
			continue
		}
		if err := w.metadata.DeleteBlobRecord(ctx, source.ID); err != nil {
			w.log.Error("failed media source gc metadata delete failed", "asset_id", id, "error", err)
		}
	}
}

func (w *FailedOriginalGC) String() string {
	return fmt.Sprintf("failed-original-gc(retention=%s)", w.retention)
}
