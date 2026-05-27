package postgres

import (
	"context"
	"errors"
	"time"

	"media-service/internal/domain"
	"media-service/internal/storage"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type metadataRepo struct {
	pool *pgxpool.Pool
}

// NewMetadataRepo creates a new PostgreSQL backed MetadataRepo
func NewMetadataRepo(pool *pgxpool.Pool) storage.MetadataRepo {
	return &metadataRepo{pool: pool}
}

func (r *metadataRepo) IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "23505"
	}
	return false
}

func (r *metadataRepo) CreateBlob(ctx context.Context, blob *domain.Blob) error {
	query := `
		INSERT INTO blobs (id, sha256, size_bytes, mime_type, retention_state, upload_status, created_by_service, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := r.pool.Exec(ctx, query, blob.ID, blob.SHA256, blob.SizeBytes, blob.MimeType, blob.RetentionState, blob.UploadStatus, blob.CreatedByService, blob.CreatedAt, blob.UpdatedAt)
	return err
}

func (r *metadataRepo) GetBlob(ctx context.Context, id uuid.UUID) (*domain.Blob, error) {
	query := `
		SELECT id, sha256, size_bytes, mime_type, retention_state, upload_status, created_by_service, created_at, updated_at
		FROM blobs WHERE id = $1
	`
	b := &domain.Blob{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&b.ID, &b.SHA256, &b.SizeBytes, &b.MimeType, &b.RetentionState, &b.UploadStatus, &b.CreatedByService, &b.CreatedAt, &b.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Or a custom Not Found error
		}
		return nil, err
	}
	return b, nil
}

func (r *metadataRepo) GetBlobBySHA256(ctx context.Context, sha256 string) (*domain.Blob, error) {
	query := `
		SELECT id, sha256, size_bytes, mime_type, retention_state, upload_status, created_by_service, created_at, updated_at
		FROM blobs WHERE sha256 = $1
	`
	b := &domain.Blob{}
	err := r.pool.QueryRow(ctx, query, sha256).Scan(
		&b.ID, &b.SHA256, &b.SizeBytes, &b.MimeType, &b.RetentionState, &b.UploadStatus, &b.CreatedByService, &b.CreatedAt, &b.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return b, nil
}

func (r *metadataRepo) UpdateBlob(ctx context.Context, blob *domain.Blob) error {
	query := `
		UPDATE blobs
		SET sha256 = $1, size_bytes = $2, retention_state = $3, upload_status = $4, updated_at = NOW()
		WHERE id = $5
	`
	_, err := r.pool.Exec(ctx, query, blob.SHA256, blob.SizeBytes, blob.RetentionState, blob.UploadStatus, blob.ID)
	return err
}

func (r *metadataRepo) CreateUploadSession(ctx context.Context, session *domain.UploadSession) error {
	query := `
		INSERT INTO upload_sessions (id, multipart_upload_id, object_key, blob_id, expected_size, mime_type, status, created_by_service, created_at, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := r.pool.Exec(ctx, query, session.ID, session.MultipartUploadID, session.ObjectKey, session.BlobID, session.ExpectedSize, session.MimeType, session.Status, session.CreatedByService, session.CreatedAt, session.ExpiresAt)
	return err
}

func (r *metadataRepo) GetUploadSession(ctx context.Context, id uuid.UUID) (*domain.UploadSession, error) {
	query := `
		SELECT id, multipart_upload_id, object_key, blob_id, expected_size, mime_type, status, created_by_service, created_at, expires_at
		FROM upload_sessions WHERE id = $1
	`
	s := &domain.UploadSession{}
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.MultipartUploadID, &s.ObjectKey, &s.BlobID, &s.ExpectedSize, &s.MimeType, &s.Status, &s.CreatedByService, &s.CreatedAt, &s.ExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return s, nil
}

func (r *metadataRepo) UpdateUploadSession(ctx context.Context, session *domain.UploadSession) error {
	query := `
		UPDATE upload_sessions
		SET status = $1, blob_id = $2
		WHERE id = $3
	`
	_, err := r.pool.Exec(ctx, query, session.Status, session.BlobID, session.ID)
	return err
}

func (r *metadataRepo) SaveUploadPart(ctx context.Context, part *domain.UploadPart) error {
	query := `
		INSERT INTO upload_parts (upload_session_id, part_number, etag, size_bytes, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (upload_session_id, part_number) DO UPDATE
		SET etag = EXCLUDED.etag, size_bytes = EXCLUDED.size_bytes, created_at = EXCLUDED.created_at
	`
	_, err := r.pool.Exec(ctx, query, part.UploadSessionID, part.PartNumber, part.ETag, part.SizeBytes, part.CreatedAt)
	return err
}

func (r *metadataRepo) GetUploadParts(ctx context.Context, sessionID uuid.UUID) ([]*domain.UploadPart, error) {
	query := `
		SELECT upload_session_id, part_number, etag, size_bytes, created_at
		FROM upload_parts
		WHERE upload_session_id = $1
		ORDER BY part_number ASC
	`
	rows, err := r.pool.Query(ctx, query, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parts []*domain.UploadPart
	for rows.Next() {
		p := &domain.UploadPart{}
		if err := rows.Scan(&p.UploadSessionID, &p.PartNumber, &p.ETag, &p.SizeBytes, &p.CreatedAt); err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	return parts, rows.Err()
}

func (r *metadataRepo) GetOrphanedBlobs(ctx context.Context, gracePeriod time.Duration) ([]*domain.Blob, error) {
	query := `
		SELECT id, sha256, size_bytes, mime_type, retention_state, upload_status, created_by_service, created_at, updated_at
		FROM blobs
		WHERE retention_state = 'PENDING_REFERENCE'
		  AND created_at < NOW() - $1::interval
	`
	// Convert duration to Postgres interval string format
	intervalStr := gracePeriod.String()

	rows, err := r.pool.Query(ctx, query, intervalStr)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blobs []*domain.Blob
	for rows.Next() {
		b := &domain.Blob{}
		if err := rows.Scan(&b.ID, &b.SHA256, &b.SizeBytes, &b.MimeType, &b.RetentionState, &b.UploadStatus, &b.CreatedByService, &b.CreatedAt, &b.UpdatedAt); err != nil {
			return nil, err
		}
		blobs = append(blobs, b)
	}
	return blobs, rows.Err()
}

func (r *metadataRepo) DeleteBlobRecord(ctx context.Context, id uuid.UUID) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Remove foreign key constraints safely for manual deletion
	_, err = tx.Exec(ctx, `UPDATE upload_sessions SET blob_id = NULL WHERE blob_id = $1`, id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM blob_relations WHERE source_blob_id = $1 OR target_blob_id = $1`, id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM blob_references WHERE blob_id = $1`, id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `DELETE FROM blobs WHERE id = $1`, id)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (r *metadataRepo) CreateBlobRelation(ctx context.Context, sourceID, targetID uuid.UUID, relationType string) error {
	query := `
		INSERT INTO blob_relations (source_blob_id, target_blob_id, relation_type)
		VALUES ($1, $2, $3)
		ON CONFLICT (source_blob_id, target_blob_id, relation_type) DO NOTHING
	`
	_, err := r.pool.Exec(ctx, query, sourceID, targetID, relationType)
	return err
}
