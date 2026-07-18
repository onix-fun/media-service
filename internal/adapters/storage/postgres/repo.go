package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/onix-fun/media/internal/application/storage"
	"github.com/onix-fun/media/internal/domain"

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

func (r *metadataRepo) CreateOrGetBlob(ctx context.Context, blob *domain.Blob) (*domain.Blob, bool, error) {
	query := `
		INSERT INTO blobs (id, sha256, size_bytes, mime_type, retention_state, upload_status, created_by_service, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (sha256) DO UPDATE SET sha256 = EXCLUDED.sha256
		RETURNING id, sha256, size_bytes, mime_type, retention_state, upload_status, created_by_service, created_at, updated_at,
		          (xmax = 0) AS inserted
	`
	resolved := &domain.Blob{}
	var inserted bool
	err := r.pool.QueryRow(ctx, query, blob.ID, blob.SHA256, blob.SizeBytes, blob.MimeType, blob.RetentionState, blob.UploadStatus, blob.CreatedByService, blob.CreatedAt, blob.UpdatedAt).Scan(
		&resolved.ID, &resolved.SHA256, &resolved.SizeBytes, &resolved.MimeType, &resolved.RetentionState, &resolved.UploadStatus, &resolved.CreatedByService, &resolved.CreatedAt, &resolved.UpdatedAt, &inserted,
	)
	if err != nil {
		return nil, false, err
	}
	return resolved, inserted, nil
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

func (r *metadataRepo) GrantBlobAccess(ctx context.Context, blobID uuid.UUID, ownerKey string) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO blob_access (blob_id, owner_key)
		VALUES ($1, $2)
		ON CONFLICT (blob_id, owner_key) DO NOTHING
	`, blobID, ownerKey)
	return err
}

func (r *metadataRepo) GrantServiceAliasAccess(ctx context.Context, ownerKey string, aliases []string) error {
	if ownerKey == "" || len(aliases) == 0 {
		return nil
	}
	_, err := r.pool.Exec(ctx, `
		INSERT INTO blob_access (blob_id, owner_key)
		SELECT id, $1
		FROM blobs
		WHERE created_by_service = ANY($2)
		ON CONFLICT (blob_id, owner_key) DO NOTHING
	`, ownerKey, aliases)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO blob_access (blob_id, owner_key)
		SELECT blob_id, $1
		FROM blob_references
		WHERE service_name = ANY($2)
		ON CONFLICT (blob_id, owner_key) DO NOTHING
	`, ownerKey, aliases)
	return err
}

func (r *metadataRepo) HasBlobAccess(ctx context.Context, blobID uuid.UUID, ownerKey string) (bool, error) {
	var allowed bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM blob_access WHERE blob_id = $1 AND owner_key = $2)
	`, blobID, ownerKey).Scan(&allowed)
	return allowed, err
}

func (r *metadataRepo) CreateReference(ctx context.Context, blobID uuid.UUID, ownerKey, referenceType, referenceID string) error {
	_, err := r.pool.Exec(ctx, `WITH inserted AS (
		INSERT INTO blob_references(id,blob_id,service_name,entity_type,entity_id)
		VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING
	) UPDATE blobs SET retention_state='REFERENCED',updated_at=NOW() WHERE id=$2`,
		uuid.Must(uuid.NewV7()), blobID, ownerKey, referenceType, referenceID)
	return err
}

func (r *metadataRepo) DeleteReference(ctx context.Context, blobID uuid.UUID, ownerKey, referenceType, referenceID string) error {
	_, err := r.pool.Exec(ctx, `DELETE FROM blob_references WHERE blob_id=$1 AND service_name=$2 AND entity_type=$3 AND entity_id=$4`, blobID, ownerKey, referenceType, referenceID)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx, `UPDATE blobs SET retention_state='PENDING_REFERENCE',updated_at=NOW()
		WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM blob_references WHERE blob_id=$1)`, blobID)
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

	_, err = tx.Exec(ctx, `DELETE FROM blob_access WHERE blob_id = $1`, id)
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

func (r *metadataRepo) GetExpiredSessions(ctx context.Context) ([]*domain.UploadSession, error) {
	query := `
		SELECT id, multipart_upload_id, object_key, blob_id, expected_size, mime_type, status, created_by_service, created_at, expires_at
		FROM upload_sessions
		WHERE status = 'UPLOADING'
		  AND expires_at < NOW()
	`
	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sessions []*domain.UploadSession
	for rows.Next() {
		s := &domain.UploadSession{}
		if err := rows.Scan(&s.ID, &s.MultipartUploadID, &s.ObjectKey, &s.BlobID, &s.ExpectedSize, &s.MimeType, &s.Status, &s.CreatedByService, &s.CreatedAt, &s.ExpiresAt); err != nil {
			return nil, err
		}
		sessions = append(sessions, s)
	}
	return sessions, rows.Err()
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

func (r *metadataRepo) CreateMediaAsset(ctx context.Context, asset *domain.MediaAsset) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO media_assets (
			id, upload_session_id, source_blob_id, owner_key, profile, status, mime_type,
			width, height, duration_ms, failure_reason, failed_at, original_removed, generation, created_at, updated_at,
			client_namespace, owner_ref, declared_kind, source_policy_id, source_status, source_failure_code
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
	`, asset.ID, asset.UploadSessionID, asset.SourceBlobID, asset.OwnerKey, asset.Profile, asset.Status, asset.MimeType,
		asset.Width, asset.Height, asset.DurationMS, asset.FailureReason, asset.FailedAt, asset.OriginalRemoved, asset.Generation, asset.CreatedAt, asset.UpdatedAt,
		asset.ClientNamespace, asset.OwnerRef, asset.DeclaredKind, asset.SourcePolicyID, asset.SourceStatus, asset.SourceFailureCode)
	return err
}

func (r *metadataRepo) GetMediaAsset(ctx context.Context, id uuid.UUID) (*domain.MediaAsset, error) {
	asset := &domain.MediaAsset{}
	err := r.pool.QueryRow(ctx, mediaAssetSelect+` WHERE id = $1`, id).Scan(mediaAssetScanTargets(asset)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return asset, nil
}

func (r *metadataRepo) GetMediaAssetByUploadSession(ctx context.Context, sessionID uuid.UUID) (*domain.MediaAsset, error) {
	asset := &domain.MediaAsset{}
	err := r.pool.QueryRow(ctx, mediaAssetSelect+` WHERE upload_session_id = $1`, sessionID).Scan(mediaAssetScanTargets(asset)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return asset, nil
}

func (r *metadataRepo) UpdateMediaAsset(ctx context.Context, asset *domain.MediaAsset) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE media_assets
		SET upload_session_id = $1,
			source_blob_id = $2,
			status = $3,
			width = $4,
			height = $5,
			duration_ms = $6,
			failure_reason = $7,
			failed_at = $8,
			original_removed = $9,
			generation = $10,
			profile = $11,
			source_status = $12,
			source_failure_code = $13,
			updated_at = NOW()
		WHERE id = $14
	`, asset.UploadSessionID, asset.SourceBlobID, asset.Status, asset.Width, asset.Height, asset.DurationMS,
		asset.FailureReason, asset.FailedAt, asset.OriginalRemoved, asset.Generation, asset.Profile,
		asset.SourceStatus, asset.SourceFailureCode, asset.ID)
	return err
}

func (r *metadataRepo) UpsertMediaAssetVariant(ctx context.Context, variant *domain.MediaAssetVariant) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO media_asset_variants (asset_id, generation, name, blob_id, mime_type, width, height, duration_ms, bitrate)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (asset_id, generation, name) DO UPDATE SET
			blob_id = EXCLUDED.blob_id,
			mime_type = EXCLUDED.mime_type,
			width = EXCLUDED.width,
			height = EXCLUDED.height,
			duration_ms = EXCLUDED.duration_ms,
			bitrate = EXCLUDED.bitrate
	`, variant.AssetID, variant.Generation, variant.Name, variant.BlobID, variant.MimeType, variant.Width, variant.Height, variant.DurationMS, variant.Bitrate)
	return err
}

func (r *metadataRepo) ListMediaAssetVariants(ctx context.Context, assetID uuid.UUID) ([]domain.MediaAssetVariant, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT asset_id, generation, name, blob_id, mime_type, width, height, duration_ms, bitrate
		FROM media_asset_variants
		WHERE asset_id = $1 AND generation = (SELECT MAX(generation) FROM media_asset_variants WHERE asset_id = $1)
		ORDER BY name
	`, assetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	variants := make([]domain.MediaAssetVariant, 0)
	for rows.Next() {
		variant := domain.MediaAssetVariant{}
		if err := rows.Scan(&variant.AssetID, &variant.Generation, &variant.Name, &variant.BlobID, &variant.MimeType, &variant.Width, &variant.Height, &variant.DurationMS, &variant.Bitrate); err != nil {
			return nil, err
		}
		variants = append(variants, variant)
	}
	return variants, rows.Err()
}

func (r *metadataRepo) ListMediaAssetVariantsForGeneration(ctx context.Context, assetID uuid.UUID, generation int64) ([]domain.MediaAssetVariant, error) {
	rows, err := r.pool.Query(ctx, `SELECT asset_id, generation, name, blob_id, mime_type, width, height, duration_ms, bitrate
		FROM media_asset_variants WHERE asset_id = $1 AND generation = $2 ORDER BY name`, assetID, generation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	variants := make([]domain.MediaAssetVariant, 0)
	for rows.Next() {
		var variant domain.MediaAssetVariant
		if err := rows.Scan(&variant.AssetID, &variant.Generation, &variant.Name, &variant.BlobID, &variant.MimeType, &variant.Width, &variant.Height, &variant.DurationMS, &variant.Bitrate); err != nil {
			return nil, err
		}
		variants = append(variants, variant)
	}
	return variants, rows.Err()
}

func (r *metadataRepo) GetMediaAssetVariant(ctx context.Context, assetID uuid.UUID, generation int64, name string) (*domain.MediaAssetVariant, error) {
	var variant domain.MediaAssetVariant
	err := r.pool.QueryRow(ctx, `SELECT asset_id, generation, name, blob_id, mime_type, width, height, duration_ms, bitrate
		FROM media_asset_variants WHERE asset_id = $1 AND generation = $2 AND name = $3`, assetID, generation, name).
		Scan(&variant.AssetID, &variant.Generation, &variant.Name, &variant.BlobID, &variant.MimeType, &variant.Width, &variant.Height, &variant.DurationMS, &variant.Bitrate)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &variant, err
}

func (r *metadataRepo) CreateProcessingRun(ctx context.Context, run *domain.ProcessingRun) error {
	_, err := r.pool.Exec(ctx, `INSERT INTO media_processing_runs
		(id, asset_id, client_namespace, owner_ref, pipeline_id, pipeline_version, generation, idempotency_key, status, failure_code, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, run.ID, run.AssetID, run.ClientNamespace, run.OwnerRef,
		run.PipelineID, run.PipelineVersion, run.Generation, run.IdempotencyKey, run.Status, run.FailureCode, run.CreatedAt, run.UpdatedAt)
	return err
}

func (r *metadataRepo) GetProcessingRun(ctx context.Context, id uuid.UUID) (*domain.ProcessingRun, error) {
	return r.processingRun(ctx, `WHERE id = $1`, id)
}

func (r *metadataRepo) GetProcessingRunByIdempotency(ctx context.Context, namespace, key string) (*domain.ProcessingRun, error) {
	return r.processingRun(ctx, `WHERE client_namespace = $1 AND idempotency_key = $2`, namespace, key)
}

func (r *metadataRepo) GetLatestProcessingRun(ctx context.Context, assetID uuid.UUID) (*domain.ProcessingRun, error) {
	return r.processingRun(ctx, `WHERE asset_id = $1 ORDER BY generation DESC LIMIT 1`, assetID)
}

func (r *metadataRepo) processingRun(ctx context.Context, suffix string, args ...any) (*domain.ProcessingRun, error) {
	var run domain.ProcessingRun
	err := r.pool.QueryRow(ctx, `SELECT id, asset_id, client_namespace, owner_ref, pipeline_id, pipeline_version,
		generation, idempotency_key, status, failure_code, created_at, updated_at FROM media_processing_runs `+suffix, args...).
		Scan(&run.ID, &run.AssetID, &run.ClientNamespace, &run.OwnerRef, &run.PipelineID, &run.PipelineVersion,
			&run.Generation, &run.IdempotencyKey, &run.Status, &run.FailureCode, &run.CreatedAt, &run.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return &run, err
}

func (r *metadataRepo) UpdateProcessingRun(ctx context.Context, run *domain.ProcessingRun) error {
	_, err := r.pool.Exec(ctx, `UPDATE media_processing_runs SET status=$1, failure_code=$2, updated_at=NOW() WHERE id=$3`, run.Status, run.FailureCode, run.ID)
	return err
}

// ReleaseAssetOriginal only unlinks a source when every asset sharing it is
// ready and has released it, and no legacy blob reference or delivery variant
// still points to it. The caller removes the returned object and blob record.
func (r *metadataRepo) ReleaseAssetOriginal(ctx context.Context, assetID uuid.UUID) (*domain.Blob, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var sourceID *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT source_blob_id FROM media_assets WHERE id = $1 FOR UPDATE`, assetID).Scan(&sourceID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if sourceID == nil {
		if _, err := tx.Exec(ctx, `UPDATE media_assets SET original_removed = TRUE, updated_at = NOW() WHERE id = $1`, assetID); err != nil {
			return nil, err
		}
		return nil, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `UPDATE media_assets SET original_removed = TRUE, updated_at = NOW() WHERE id = $1`, assetID); err != nil {
		return nil, err
	}

	var retained bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM media_assets WHERE source_blob_id = $1 AND original_removed = FALSE
			UNION ALL SELECT 1 FROM blob_references WHERE blob_id = $1
			UNION ALL SELECT 1 FROM media_asset_variants WHERE blob_id = $1
		)
	`, *sourceID).Scan(&retained); err != nil {
		return nil, err
	}
	if retained {
		return nil, tx.Commit(ctx)
	}

	blob := &domain.Blob{}
	if err := tx.QueryRow(ctx, `
		SELECT id, sha256, size_bytes, mime_type, retention_state, upload_status, created_by_service, created_at, updated_at
		FROM blobs WHERE id = $1 FOR UPDATE
	`, *sourceID).Scan(&blob.ID, &blob.SHA256, &blob.SizeBytes, &blob.MimeType, &blob.RetentionState, &blob.UploadStatus, &blob.CreatedByService, &blob.CreatedAt, &blob.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, tx.Commit(ctx)
		}
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE media_assets SET source_blob_id = NULL, updated_at = NOW() WHERE source_blob_id = $1`, *sourceID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return blob, nil
}

func (r *metadataRepo) AppendAssetLifecycleEvent(ctx context.Context, event *domain.AssetLifecycleEvent) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO media_outbox (event_id, event_type, asset_id, generation, owner_key, failure_code, run_id, client_namespace, owner_ref)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (event_id) DO NOTHING
	`, event.EventID, event.Type, event.AssetID, event.Generation, event.OwnerKey, event.FailureCode, event.RunID, event.ClientNamespace, event.OwnerRef)
	return err
}

func (r *metadataRepo) ListAssetLifecycleEventsForNamespace(ctx context.Context, namespace string, afterSequence int64, limit int) ([]domain.AssetLifecycleEvent, error) {
	rows, err := r.pool.Query(ctx, `SELECT sequence, event_id, event_type, asset_id, run_id, generation, owner_key,
		failure_code, client_namespace, owner_ref, created_at FROM media_outbox
		WHERE client_namespace = $1 AND sequence > $2 ORDER BY sequence ASC LIMIT $3`, namespace, afterSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]domain.AssetLifecycleEvent, 0)
	for rows.Next() {
		var event domain.AssetLifecycleEvent
		if err := rows.Scan(&event.Sequence, &event.EventID, &event.Type, &event.AssetID, &event.RunID, &event.Generation,
			&event.OwnerKey, &event.FailureCode, &event.ClientNamespace, &event.OwnerRef, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *metadataRepo) ListAssetLifecycleEvents(ctx context.Context, afterSequence int64, limit int) ([]domain.AssetLifecycleEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT sequence, event_id, event_type, asset_id, generation, owner_key, failure_code, created_at
		FROM media_outbox WHERE sequence > $1 ORDER BY sequence ASC LIMIT $2
	`, afterSequence, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]domain.AssetLifecycleEvent, 0)
	for rows.Next() {
		var event domain.AssetLifecycleEvent
		if err := rows.Scan(&event.Sequence, &event.EventID, &event.Type, &event.AssetID, &event.Generation, &event.OwnerKey, &event.FailureCode, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *metadataRepo) ListExpiredFailedAssetIDs(ctx context.Context, retention time.Duration, limit int) ([]uuid.UUID, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id FROM media_assets
		WHERE status IN ('FAILED', 'REJECTED') AND source_blob_id IS NOT NULL AND failed_at <= NOW() - ($1 * INTERVAL '1 second')
		ORDER BY failed_at ASC LIMIT $2
	`, int64(retention.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]uuid.UUID, 0)
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

const mediaAssetSelect = `
	SELECT id, upload_session_id, source_blob_id, owner_key, profile, status, mime_type,
		width, height, duration_ms, failure_reason, failed_at, original_removed, generation, created_at, updated_at,
		client_namespace, owner_ref, declared_kind, source_policy_id, source_status, source_failure_code
	FROM media_assets`

func mediaAssetScanTargets(asset *domain.MediaAsset) []any {
	return []any{
		&asset.ID,
		&asset.UploadSessionID,
		&asset.SourceBlobID,
		&asset.OwnerKey,
		&asset.Profile,
		&asset.Status,
		&asset.MimeType,
		&asset.Width,
		&asset.Height,
		&asset.DurationMS,
		&asset.FailureReason,
		&asset.FailedAt,
		&asset.OriginalRemoved,
		&asset.Generation,
		&asset.CreatedAt,
		&asset.UpdatedAt,
		&asset.ClientNamespace,
		&asset.OwnerRef,
		&asset.DeclaredKind,
		&asset.SourcePolicyID,
		&asset.SourceStatus,
		&asset.SourceFailureCode,
	}
}
