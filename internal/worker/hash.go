package worker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/onix-fun/media-service/internal/config"
	"github.com/onix-fun/media-service/internal/domain"
	"github.com/onix-fun/media-service/internal/storage"

	"github.com/google/uuid"
)

type HashWorker struct {
	metadata storage.MetadataRepo
	blobSvc  storage.BlobStorage
	scanning config.Scanning
}

func NewHashWorker(metadata storage.MetadataRepo, blobSvc storage.BlobStorage, scanning config.Scanning) *HashWorker {
	return &HashWorker{
		metadata: metadata,
		blobSvc:  blobSvc,
		scanning: scanning,
	}
}

func (w *HashWorker) ProcessSession(ctx context.Context, sessionID uuid.UUID) error {
	log.Printf("HashWorker: Processing session %s", sessionID)

	// 1. Fetch Session
	session, err := w.metadata.GetUploadSession(ctx, sessionID)
	if err != nil {
		log.Printf("HashWorker error fetching session %s: %v", sessionID, err)
		return err
	}
	if session == nil {
		log.Printf("HashWorker: session %s not found", sessionID)
		return fmt.Errorf("session not found")
	}
	if w.scanning.Enabled {
		if err := w.scan(ctx, session.ObjectKey); err != nil {
			return fmt.Errorf("malware scan failed: %w", err)
		}
	}

	// 2. Download stream from S3 using session.ObjectKey
	stream, err := w.blobSvc.GetBlobStream(ctx, session.ObjectKey)
	if err != nil {
		log.Printf("HashWorker error streaming blob %s: %v", session.ObjectKey, err)
		return err
	}
	defer stream.Close()

	// 3. Compute SHA256 and exact size
	hasher := sha256.New()
	sizeBytes, err := io.Copy(hasher, stream)
	if err != nil {
		log.Printf("HashWorker error hashing blob %s: %v", session.ObjectKey, err)
		return err
	}
	hashStr := hex.EncodeToString(hasher.Sum(nil))

	log.Printf("HashWorker: session %s hashed to %s (%d bytes)", sessionID, hashStr, sizeBytes)

	// 4. Check for existing blob (Deduplication)
	blob, err := w.metadata.GetBlobBySHA256(ctx, hashStr)
	if err != nil {
		log.Printf("HashWorker error querying blob by hash %s: %v", hashStr, err)
		return err
	}

	if blob == nil {
		// 5. If new: Move object to CAS path in S3, create Blob record
		canonicalKey := fmt.Sprintf("blobs/%s/%s/%s", hashStr[:2], hashStr[2:4], hashStr)

		err = w.blobSvc.CopyBlob(ctx, session.ObjectKey, canonicalKey)
		if err != nil {
			log.Printf("HashWorker error copying to canonical key %s: %v", canonicalKey, err)
			return err
		}

		newBlobID := uuid.Must(uuid.NewV7())
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
					return err
				}
			} else {
				log.Printf("HashWorker error creating blob record for %s: %v", hashStr, err)
				return err
			}
		} else {
			blob = newBlob
		}
	} else {
		log.Printf("HashWorker: duplicate blob found for hash %s", hashStr)
	}

	if err := w.metadata.GrantBlobAccess(ctx, blob.ID, session.CreatedByService); err != nil {
		log.Printf("HashWorker error granting blob access %s: %v", blob.ID, err)
		return err
	}

	// 6. Finalize session and cleanup S3 temp object
	session.BlobID = &blob.ID
	err = w.metadata.UpdateUploadSession(ctx, session)
	if err != nil {
		log.Printf("HashWorker error updating session blob link %s: %v", sessionID, err)
		return err
	}

	err = w.blobSvc.DeleteBlob(ctx, session.ObjectKey)
	if err != nil {
		// Log but don't fail, S3 lifecycle rules will eventually clean abandoned temp keys
		log.Printf("HashWorker warning: failed to delete temp object %s: %v", session.ObjectKey, err)
	}

	log.Printf("HashWorker: successfully processed session %s -> blob %s", sessionID, blob.ID)
	return nil
}

func (w *HashWorker) scan(ctx context.Context, key string) error {
	stream, err := w.blobSvc.GetBlobStream(ctx, key)
	if err != nil {
		return err
	}
	defer stream.Close()
	dialer := net.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", w.scanning.ClamAVAddress)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.Write([]byte("zINSTREAM\x00")); err != nil {
		return err
	}
	buffer := make([]byte, 32*1024)
	for {
		n, readErr := stream.Read(buffer)
		if n > 0 {
			var size [4]byte
			binary.BigEndian.PutUint32(size[:], uint32(n))
			if _, err = conn.Write(size[:]); err != nil {
				return err
			}
			if _, err = conn.Write(buffer[:n]); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	if _, err = conn.Write([]byte{0, 0, 0, 0}); err != nil {
		return err
	}
	response, err := io.ReadAll(io.LimitReader(conn, 4096))
	if err != nil {
		return err
	}
	if !strings.Contains(string(response), "OK") {
		return fmt.Errorf("clamav rejected object: %s", strings.TrimSpace(string(response)))
	}
	return nil
}
