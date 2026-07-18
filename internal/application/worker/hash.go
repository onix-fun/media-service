package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/onix-fun/media/internal/application/storage"
	"github.com/onix-fun/media/internal/domain"
	"github.com/onix-fun/media/internal/platform/config"

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
	prefix := make([]byte, 512)
	prefixSize, readErr := io.ReadFull(stream, prefix)
	if readErr != nil && readErr != io.ErrUnexpectedEOF {
		return readErr
	}
	prefix = prefix[:prefixSize]
	detectedMIME := detectMediaMIME(prefix)
	hasher := sha256.New()
	sizeBytes, err := io.Copy(hasher, io.MultiReader(bytes.NewReader(prefix), stream))
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

		newBlob := &domain.Blob{
			ID:               uuid.Must(uuid.NewV7()),
			SHA256:           &hashStr,
			SizeBytes:        &sizeBytes,
			MimeType:         detectedMIME,
			RetentionState:   domain.RetentionPendingReference,
			UploadStatus:     domain.UploadReady,
			CreatedByService: session.CreatedByService,
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
		}

		var inserted bool
		blob, inserted, err = w.metadata.CreateOrGetBlob(ctx, newBlob)
		if err != nil {
			log.Printf("HashWorker error resolving blob record for %s: %v", hashStr, err)
			return err
		}
		if !inserted {
			log.Printf("HashWorker: concurrent duplicate blob reused for hash %s", hashStr)
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

// net/http intentionally returns application/octet-stream for some EBML
// containers. Browser MediaRecorder emits WebM, so recognize its DocType while
// keeping the final codec/container verification in the asset service.
func detectMediaMIME(prefix []byte) string {
	detected := strings.ToLower(strings.TrimSpace(strings.Split(http.DetectContentType(prefix), ";")[0]))
	if detected == "application/octet-stream" && len(prefix) >= 4 &&
		prefix[0] == 0x1a && prefix[1] == 0x45 && prefix[2] == 0xdf && prefix[3] == 0xa3 &&
		bytes.Contains(bytes.ToLower(prefix), []byte("webm")) {
		return "video/webm"
	}
	return detected
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
