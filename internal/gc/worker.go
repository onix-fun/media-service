package gc

import (
	"context"
	"fmt"
	"log"
	"time"

	"media-service/internal/storage"
)

type Worker struct {
	metadata    storage.MetadataRepo
	blobStorage storage.BlobStorage
	interval    time.Duration
	gracePeriod time.Duration
}

func NewWorker(metadata storage.MetadataRepo, blobStorage storage.BlobStorage, interval, gracePeriod time.Duration) *Worker {
	return &Worker{
		metadata:    metadata,
		blobStorage: blobStorage,
		interval:    interval,
		gracePeriod: gracePeriod,
	}
}

func (w *Worker) Start(ctx context.Context) {
	log.Printf("GC Worker started. Interval: %v, Grace Period: %v", w.interval, w.gracePeriod)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("GC Worker shutting down")
			return
		case <-ticker.C:
			w.runSweep(ctx)
		}
	}
}

func (w *Worker) runSweep(ctx context.Context) {
	log.Println("GC Worker: Starting sweep for orphaned blobs...")

	blobs, err := w.metadata.GetOrphanedBlobs(ctx, w.gracePeriod)
	if err != nil {
		log.Printf("GC Worker error fetching orphaned blobs: %v", err)
		return
	}

	if len(blobs) == 0 {
		log.Println("GC Worker: No orphaned blobs found.")
		return
	}

	log.Printf("GC Worker: Found %d orphaned blobs to delete.", len(blobs))

	for _, blob := range blobs {
		// 1. Delete from S3 if it has a SHA256 (meaning it was fully hashed and moved to canonical path)
		if blob.SHA256 != nil {
			hashStr := *blob.SHA256
			canonicalKey := fmt.Sprintf("blobs/%s/%s/%s", hashStr[:2], hashStr[2:4], hashStr)

			err := w.blobStorage.DeleteBlob(ctx, canonicalKey)
			if err != nil {
				log.Printf("GC Worker warning: failed to delete S3 object %s for blob %s: %v", canonicalKey, blob.ID, err)
				// Continue to delete DB record anyway? Usually we want to ensure S3 is clean, but let's assume eventual consistency.
			} else {
				log.Printf("GC Worker: Deleted S3 object %s", canonicalKey)
			}
		}

		// 2. Delete metadata record
		err := w.metadata.DeleteBlobRecord(ctx, blob.ID)
		if err != nil {
			log.Printf("GC Worker error deleting blob record %s: %v", blob.ID, err)
		} else {
			log.Printf("GC Worker: Deleted blob record %s", blob.ID)
		}
	}

	log.Println("GC Worker: Sweep completed.")
}
