CREATE TABLE blobs (
    id UUID PRIMARY KEY,
    sha256 TEXT UNIQUE, -- Nullable initially because hashing is async
    size_bytes BIGINT,
    mime_type TEXT NOT NULL,
    retention_state TEXT NOT NULL, -- PENDING_REFERENCE, REFERENCED, ORPHANED, MARKED_FOR_DELETION
    upload_status TEXT NOT NULL,   -- HASH_PENDING, READY, FAILED
    created_by_service TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for deduplication lookups
CREATE INDEX idx_blobs_sha256 ON blobs(sha256) WHERE sha256 IS NOT NULL;
CREATE INDEX idx_blobs_retention_state ON blobs(retention_state);

CREATE TABLE upload_sessions (
    id UUID PRIMARY KEY,
    multipart_upload_id TEXT NOT NULL,
    object_key TEXT NOT NULL, -- The final destination key in S3
    blob_id UUID REFERENCES blobs(id),
    expected_size BIGINT,
    mime_type TEXT NOT NULL,
    status TEXT NOT NULL, -- UPLOADING, FINALIZING, ABANDONED, COMPLETED
    created_by_service TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE upload_parts (
    upload_session_id UUID NOT NULL REFERENCES upload_sessions(id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    etag TEXT NOT NULL,
    size_bytes BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY(upload_session_id, part_number)
);

CREATE TABLE blob_references (
    id UUID PRIMARY KEY,
    blob_id UUID NOT NULL REFERENCES blobs(id),
    service_name TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_blob_references_blob_id ON blob_references(blob_id);

CREATE TABLE blob_relations (
    source_blob_id UUID NOT NULL REFERENCES blobs(id),
    target_blob_id UUID NOT NULL REFERENCES blobs(id),
    relation_type TEXT NOT NULL, -- THUMBNAIL, TRANSCODE, PREVIEW, WAVEFORM, DERIVED_FROM
    PRIMARY KEY(source_blob_id, target_blob_id, relation_type)
);

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY,
    service_name TEXT NOT NULL,
    action TEXT NOT NULL,
    blob_id UUID REFERENCES blobs(id),
    upload_session_id UUID REFERENCES upload_sessions(id),
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
