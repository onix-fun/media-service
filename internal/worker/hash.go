package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"time"

	"media-service/internal/domain"
	"media-service/internal/storage"

	"github.com/google/uuid"
)

type HashWorker struct {
	metadata storage.MetadataRepo
	blobSvc  storage.BlobStorage
	jobs     <-chan uuid.UUID
}

func NewHashWorker(metadata storage.MetadataRepo, blobSvc storage.BlobStorage, jobs <-chan uuid.UUID) *HashWorker {
	return &HashWorker{
		metadata: metadata,
		blobSvc:  blobSvc,
		jobs:     jobs,
	}
}

func (w *HashWorker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Println("HashWorker shutting down")
			return
		case sessionID := <-w.jobs:
			// Process in a new goroutine to allow concurrent hashing
			go w.processSession(ctx, sessionID)
		}
	}
}

func (w *HashWorker) processSession(ctx context.Context, sessionID uuid.UUID) {
	log.Printf("HashWorker: Processing session %s", sessionID)

	// 1. Fetch Session
	session, err := w.metadata.GetUploadSession(ctx, sessionID)
	if err != nil {
		log.Printf("HashWorker error fetching session %s: %v", sessionID, err)
		return
	}
	if session == nil {
		log.Printf("HashWorker: session %s not found", sessionID)
		return
	}

	// 2. Download stream from S3 using session.ObjectKey
	stream, err := w.blobSvc.GetBlobStream(ctx, session.ObjectKey)
	if err != nil {
		log.Printf("HashWorker error streaming blob %s: %v", session.ObjectKey, err)
		return
	}
	defer stream.Close()

	// 3. Compute SHA256 and exact size
	hasher := sha256.New()
	sizeBytes, err := io.Copy(hasher, stream)
	if err != nil {
		log.Printf("HashWorker error hashing blob %s: %v", session.ObjectKey, err)
		return
	}
	hashStr := hex.EncodeToString(hasher.Sum(nil))

	log.Printf("HashWorker: session %s hashed to %s (%d bytes)", sessionID, hashStr, sizeBytes)

	// 4. Check for existing blob (Deduplication)
	blob, err := w.metadata.GetBlobBySHA256(ctx, hashStr)
	if err != nil {
		log.Printf("HashWorker error querying blob by hash %s: %v", hashStr, err)
		return
	}

	if blob == nil {
		// 5. If new: Move object to CAS path in S3, create Blob record
		canonicalKey := fmt.Sprintf("blobs/%s/%s/%s", hashStr[:2], hashStr[2:4], hashStr)

		err = w.blobSvc.CopyBlob(ctx, session.ObjectKey, canonicalKey)
		if err != nil {
			log.Printf("HashWorker error copying to canonical key %s: %v", canonicalKey, err)
			return
		}

		newBlobID := uuid.New()
		newBlob := &domain.Blob{
			ID:               newBlobID,
			SHA256:           &hashStr,
			SizeBytes:        &sizeBytes,
			MimeType:         session.MimeType,
			RetentionState:   domain.RetentionPendingReference,
			UploadStatus:     domain.UploadReady,
			CreatedByService: session.CreatedByService,
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
		}

		err = w.metadata.CreateBlob(ctx, newBlob)
		if err != nil {
			if w.metadata.IsUniqueViolation(err) {
				// Race condition: another worker inserted it first
				log.Printf("HashWorker: unique violation, fetching existing blob for hash %s", hashStr)
				blob, err = w.metadata.GetBlobBySHA256(ctx, hashStr)
				if err != nil || blob == nil {
					log.Printf("HashWorker critical error recovering from race for %s: %v", hashStr, err)
					return
				}
			} else {
				log.Printf("HashWorker error creating blob record for %s: %v", hashStr, err)
				return
			}
		} else {
			blob = newBlob
		}
	} else {
		log.Printf("HashWorker: duplicate blob found for hash %s", hashStr)
	}

	// 6. Finalize session and cleanup S3 temp object
	session.BlobID = &blob.ID
	err = w.metadata.UpdateUploadSession(ctx, session)
	if err != nil {
		log.Printf("HashWorker error updating session blob link %s: %v", sessionID, err)
		return
	}

	err = w.blobSvc.DeleteBlob(ctx, session.ObjectKey)
	if err != nil {
		// Log but don't fail, S3 lifecycle rules will eventually clean abandoned temp keys
		log.Printf("HashWorker warning: failed to delete temp object %s: %v", session.ObjectKey, err)
	}

	log.Printf("HashWorker: successfully processed session %s -> blob %s", sessionID, blob.ID)
}
