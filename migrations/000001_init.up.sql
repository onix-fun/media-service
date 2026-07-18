-- Generated from the final pre-production schema for media.
-- Data is intentionally reset; historical compatibility DDL is forbidden here.

CREATE SCHEMA IF NOT EXISTS media;

CREATE SEQUENCE media.media_outbox_sequence_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

CREATE TABLE media.blobs (
    id uuid NOT NULL,
    sha256 text,
    size_bytes bigint,
    mime_type text NOT NULL,
    retention_state text NOT NULL,
    upload_status text NOT NULL,
    created_by_service text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT blobs_created_at_not_null NOT NULL created_at,
    CONSTRAINT blobs_created_by_service_not_null NOT NULL created_by_service,
    CONSTRAINT blobs_id_not_null NOT NULL id,
    CONSTRAINT blobs_mime_type_not_null NOT NULL mime_type,
    CONSTRAINT blobs_pkey PRIMARY KEY (id),
    CONSTRAINT blobs_retention_state_not_null NOT NULL retention_state,
    CONSTRAINT blobs_sha256_key UNIQUE (sha256),
    CONSTRAINT blobs_updated_at_not_null NOT NULL updated_at,
    CONSTRAINT blobs_upload_status_not_null NOT NULL upload_status
);

CREATE TABLE media.blob_access (
    blob_id uuid NOT NULL,
    owner_key text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT blob_access_blob_id_fkey FOREIGN KEY (blob_id) REFERENCES media.blobs(id) ON DELETE CASCADE,
    CONSTRAINT blob_access_blob_id_not_null NOT NULL blob_id,
    CONSTRAINT blob_access_created_at_not_null NOT NULL created_at,
    CONSTRAINT blob_access_owner_key_not_null NOT NULL owner_key,
    CONSTRAINT blob_access_pkey PRIMARY KEY (blob_id, owner_key)
);

CREATE TABLE media.blob_references (
    id uuid NOT NULL,
    blob_id uuid NOT NULL,
    service_name text NOT NULL,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT blob_references_blob_id_fkey FOREIGN KEY (blob_id) REFERENCES media.blobs(id),
    CONSTRAINT blob_references_blob_id_not_null NOT NULL blob_id,
    CONSTRAINT blob_references_created_at_not_null NOT NULL created_at,
    CONSTRAINT blob_references_entity_id_not_null NOT NULL entity_id,
    CONSTRAINT blob_references_entity_type_not_null NOT NULL entity_type,
    CONSTRAINT blob_references_id_not_null NOT NULL id,
    CONSTRAINT blob_references_pkey PRIMARY KEY (id),
    CONSTRAINT blob_references_service_name_not_null NOT NULL service_name
);

CREATE TABLE media.blob_relations (
    source_blob_id uuid NOT NULL,
    target_blob_id uuid NOT NULL,
    relation_type text NOT NULL,
    CONSTRAINT blob_relations_pkey PRIMARY KEY (source_blob_id, target_blob_id, relation_type),
    CONSTRAINT blob_relations_relation_type_not_null NOT NULL relation_type,
    CONSTRAINT blob_relations_source_blob_id_fkey FOREIGN KEY (source_blob_id) REFERENCES media.blobs(id),
    CONSTRAINT blob_relations_source_blob_id_not_null NOT NULL source_blob_id,
    CONSTRAINT blob_relations_target_blob_id_fkey FOREIGN KEY (target_blob_id) REFERENCES media.blobs(id),
    CONSTRAINT blob_relations_target_blob_id_not_null NOT NULL target_blob_id
);

CREATE TABLE media.upload_sessions (
    id uuid NOT NULL,
    multipart_upload_id text NOT NULL,
    object_key text NOT NULL,
    blob_id uuid,
    expected_size bigint,
    mime_type text NOT NULL,
    status text NOT NULL,
    created_by_service text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    CONSTRAINT upload_sessions_blob_id_fkey FOREIGN KEY (blob_id) REFERENCES media.blobs(id),
    CONSTRAINT upload_sessions_created_at_not_null NOT NULL created_at,
    CONSTRAINT upload_sessions_created_by_service_not_null NOT NULL created_by_service,
    CONSTRAINT upload_sessions_expires_at_not_null NOT NULL expires_at,
    CONSTRAINT upload_sessions_id_not_null NOT NULL id,
    CONSTRAINT upload_sessions_mime_type_not_null NOT NULL mime_type,
    CONSTRAINT upload_sessions_multipart_upload_id_not_null NOT NULL multipart_upload_id,
    CONSTRAINT upload_sessions_object_key_not_null NOT NULL object_key,
    CONSTRAINT upload_sessions_pkey PRIMARY KEY (id),
    CONSTRAINT upload_sessions_status_not_null NOT NULL status
);

CREATE TABLE media.audit_logs (
    id uuid NOT NULL,
    service_name text NOT NULL,
    action text NOT NULL,
    blob_id uuid,
    upload_session_id uuid,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT audit_logs_action_not_null NOT NULL action,
    CONSTRAINT audit_logs_blob_id_fkey FOREIGN KEY (blob_id) REFERENCES media.blobs(id),
    CONSTRAINT audit_logs_created_at_not_null NOT NULL created_at,
    CONSTRAINT audit_logs_id_not_null NOT NULL id,
    CONSTRAINT audit_logs_pkey PRIMARY KEY (id),
    CONSTRAINT audit_logs_service_name_not_null NOT NULL service_name,
    CONSTRAINT audit_logs_upload_session_id_fkey FOREIGN KEY (upload_session_id) REFERENCES media.upload_sessions(id)
);

CREATE TABLE media.media_assets (
    id uuid NOT NULL,
    upload_session_id uuid,
    source_blob_id uuid,
    owner_key text NOT NULL,
    profile text NOT NULL,
    status text NOT NULL,
    mime_type text NOT NULL,
    width integer DEFAULT 0 NOT NULL,
    height integer DEFAULT 0 NOT NULL,
    duration_ms bigint DEFAULT 0 NOT NULL,
    failure_reason text DEFAULT ''::text NOT NULL,
    original_removed boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    failed_at timestamp with time zone,
    client_namespace text DEFAULT 'legacy'::text NOT NULL,
    owner_ref text DEFAULT ''::text NOT NULL,
    declared_kind text DEFAULT 'IMAGE'::text NOT NULL,
    source_policy_id text DEFAULT 'legacy-v1'::text NOT NULL,
    source_status text DEFAULT 'AVAILABLE'::text NOT NULL,
    source_failure_code text DEFAULT ''::text NOT NULL,
    CONSTRAINT media_assets_client_namespace_not_null NOT NULL client_namespace,
    CONSTRAINT media_assets_created_at_not_null NOT NULL created_at,
    CONSTRAINT media_assets_declared_kind_check CHECK (declared_kind = ANY (ARRAY['IMAGE'::text, 'VIDEO'::text, 'AUDIO'::text])),
    CONSTRAINT media_assets_declared_kind_not_null NOT NULL declared_kind,
    CONSTRAINT media_assets_duration_ms_not_null NOT NULL duration_ms,
    CONSTRAINT media_assets_failure_reason_not_null NOT NULL failure_reason,
    CONSTRAINT media_assets_generation_not_null NOT NULL generation,
    CONSTRAINT media_assets_height_not_null NOT NULL height,
    CONSTRAINT media_assets_id_not_null NOT NULL id,
    CONSTRAINT media_assets_mime_type_not_null NOT NULL mime_type,
    CONSTRAINT media_assets_original_removed_not_null NOT NULL original_removed,
    CONSTRAINT media_assets_owner_key_not_null NOT NULL owner_key,
    CONSTRAINT media_assets_owner_ref_not_null NOT NULL owner_ref,
    CONSTRAINT media_assets_pkey PRIMARY KEY (id),
    CONSTRAINT media_assets_profile_not_null NOT NULL profile,
    CONSTRAINT media_assets_source_blob_id_fkey FOREIGN KEY (source_blob_id) REFERENCES media.blobs(id) ON DELETE SET NULL,
    CONSTRAINT media_assets_source_failure_code_not_null NOT NULL source_failure_code,
    CONSTRAINT media_assets_source_policy_id_not_null NOT NULL source_policy_id,
    CONSTRAINT media_assets_source_status_check CHECK (source_status = ANY (ARRAY['UPLOADING'::text, 'VERIFYING'::text, 'AVAILABLE'::text, 'REJECTED'::text])),
    CONSTRAINT media_assets_source_status_not_null NOT NULL source_status,
    CONSTRAINT media_assets_status_not_null NOT NULL status,
    CONSTRAINT media_assets_updated_at_not_null NOT NULL updated_at,
    CONSTRAINT media_assets_upload_session_id_fkey FOREIGN KEY (upload_session_id) REFERENCES media.upload_sessions(id) ON DELETE SET NULL,
    CONSTRAINT media_assets_upload_session_id_key UNIQUE (upload_session_id),
    CONSTRAINT media_assets_width_not_null NOT NULL width
);

CREATE TABLE media.media_asset_variants (
    asset_id uuid NOT NULL,
    name text NOT NULL,
    blob_id uuid NOT NULL,
    mime_type text NOT NULL,
    width integer DEFAULT 0 NOT NULL,
    height integer DEFAULT 0 NOT NULL,
    duration_ms bigint DEFAULT 0 NOT NULL,
    bitrate bigint DEFAULT 0 NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    CONSTRAINT media_asset_variants_asset_id_fkey FOREIGN KEY (asset_id) REFERENCES media.media_assets(id) ON DELETE CASCADE,
    CONSTRAINT media_asset_variants_asset_id_not_null NOT NULL asset_id,
    CONSTRAINT media_asset_variants_bitrate_not_null NOT NULL bitrate,
    CONSTRAINT media_asset_variants_blob_id_fkey FOREIGN KEY (blob_id) REFERENCES media.blobs(id),
    CONSTRAINT media_asset_variants_blob_id_not_null NOT NULL blob_id,
    CONSTRAINT media_asset_variants_duration_ms_not_null NOT NULL duration_ms,
    CONSTRAINT media_asset_variants_generation_not_null NOT NULL generation,
    CONSTRAINT media_asset_variants_height_not_null NOT NULL height,
    CONSTRAINT media_asset_variants_mime_type_not_null NOT NULL mime_type,
    CONSTRAINT media_asset_variants_name_not_null NOT NULL name,
    CONSTRAINT media_asset_variants_pkey PRIMARY KEY (asset_id, generation, name),
    CONSTRAINT media_asset_variants_width_not_null NOT NULL width
);

CREATE TABLE media.media_jobs (
    id uuid NOT NULL,
    job_key text NOT NULL,
    type text NOT NULL,
    session_id uuid,
    blob_id uuid,
    profile text,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts bigint DEFAULT 0 NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    leased_until timestamp with time zone,
    last_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    asset_id uuid,
    generation bigint DEFAULT 0 NOT NULL,
    CONSTRAINT media_jobs_asset_id_fkey FOREIGN KEY (asset_id) REFERENCES media.media_assets(id) ON DELETE CASCADE,
    CONSTRAINT media_jobs_attempts_not_null NOT NULL attempts,
    CONSTRAINT media_jobs_created_at_not_null NOT NULL created_at,
    CONSTRAINT media_jobs_generation_not_null NOT NULL generation,
    CONSTRAINT media_jobs_id_not_null NOT NULL id,
    CONSTRAINT media_jobs_job_key_key UNIQUE (job_key),
    CONSTRAINT media_jobs_job_key_not_null NOT NULL job_key,
    CONSTRAINT media_jobs_next_attempt_at_not_null NOT NULL next_attempt_at,
    CONSTRAINT media_jobs_pkey PRIMARY KEY (id),
    CONSTRAINT media_jobs_status_check CHECK (status = ANY (ARRAY['pending'::text, 'leased'::text, 'retry'::text, 'done'::text, 'dead'::text])),
    CONSTRAINT media_jobs_status_not_null NOT NULL status,
    CONSTRAINT media_jobs_type_check CHECK (type = ANY (ARRAY['hash'::text, 'process'::text, 'asset'::text])),
    CONSTRAINT media_jobs_type_not_null NOT NULL type,
    CONSTRAINT media_jobs_updated_at_not_null NOT NULL updated_at
);

CREATE TABLE media.media_processing_runs (
    id uuid NOT NULL,
    asset_id uuid NOT NULL,
    client_namespace text NOT NULL,
    owner_ref text NOT NULL,
    pipeline_id text NOT NULL,
    pipeline_version text NOT NULL,
    generation bigint NOT NULL,
    idempotency_key text NOT NULL,
    status text NOT NULL,
    failure_code text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT media_processing_runs_asset_id_fkey FOREIGN KEY (asset_id) REFERENCES media.media_assets(id) ON DELETE CASCADE,
    CONSTRAINT media_processing_runs_asset_id_generation_key UNIQUE (asset_id, generation),
    CONSTRAINT media_processing_runs_asset_id_not_null NOT NULL asset_id,
    CONSTRAINT media_processing_runs_client_namespace_idempotency_key_key UNIQUE (client_namespace, idempotency_key),
    CONSTRAINT media_processing_runs_client_namespace_not_null NOT NULL client_namespace,
    CONSTRAINT media_processing_runs_created_at_not_null NOT NULL created_at,
    CONSTRAINT media_processing_runs_failure_code_not_null NOT NULL failure_code,
    CONSTRAINT media_processing_runs_generation_not_null NOT NULL generation,
    CONSTRAINT media_processing_runs_id_not_null NOT NULL id,
    CONSTRAINT media_processing_runs_idempotency_key_not_null NOT NULL idempotency_key,
    CONSTRAINT media_processing_runs_owner_ref_not_null NOT NULL owner_ref,
    CONSTRAINT media_processing_runs_pipeline_id_not_null NOT NULL pipeline_id,
    CONSTRAINT media_processing_runs_pipeline_version_not_null NOT NULL pipeline_version,
    CONSTRAINT media_processing_runs_pkey PRIMARY KEY (id),
    CONSTRAINT media_processing_runs_status_check CHECK (status = ANY (ARRAY['WAITING_SOURCE'::text, 'QUEUED'::text, 'PROCESSING'::text, 'READY'::text, 'FAILED'::text, 'CANCELLED'::text])),
    CONSTRAINT media_processing_runs_status_not_null NOT NULL status,
    CONSTRAINT media_processing_runs_updated_at_not_null NOT NULL updated_at
);

CREATE TABLE media.media_outbox (
    sequence bigint DEFAULT nextval('media.media_outbox_sequence_seq'::regclass) NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    asset_id uuid NOT NULL,
    generation bigint NOT NULL,
    owner_key text NOT NULL,
    failure_code text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    run_id uuid,
    client_namespace text DEFAULT 'legacy'::text NOT NULL,
    owner_ref text DEFAULT ''::text NOT NULL,
    CONSTRAINT media_outbox_asset_id_fkey FOREIGN KEY (asset_id) REFERENCES media.media_assets(id) ON DELETE CASCADE,
    CONSTRAINT media_outbox_asset_id_not_null NOT NULL asset_id,
    CONSTRAINT media_outbox_client_namespace_not_null NOT NULL client_namespace,
    CONSTRAINT media_outbox_created_at_not_null NOT NULL created_at,
    CONSTRAINT media_outbox_event_id_key UNIQUE (event_id),
    CONSTRAINT media_outbox_event_id_not_null NOT NULL event_id,
    CONSTRAINT media_outbox_event_type_not_null NOT NULL event_type,
    CONSTRAINT media_outbox_failure_code_not_null NOT NULL failure_code,
    CONSTRAINT media_outbox_generation_not_null NOT NULL generation,
    CONSTRAINT media_outbox_owner_key_not_null NOT NULL owner_key,
    CONSTRAINT media_outbox_owner_ref_not_null NOT NULL owner_ref,
    CONSTRAINT media_outbox_pkey PRIMARY KEY (sequence),
    CONSTRAINT media_outbox_run_id_fkey FOREIGN KEY (run_id) REFERENCES media.media_processing_runs(id) ON DELETE SET NULL,
    CONSTRAINT media_outbox_sequence_not_null NOT NULL sequence
);

CREATE TABLE media.upload_parts (
    upload_session_id uuid NOT NULL,
    part_number integer NOT NULL,
    etag text NOT NULL,
    size_bytes bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT upload_parts_created_at_not_null NOT NULL created_at,
    CONSTRAINT upload_parts_etag_not_null NOT NULL etag,
    CONSTRAINT upload_parts_part_number_not_null NOT NULL part_number,
    CONSTRAINT upload_parts_pkey PRIMARY KEY (upload_session_id, part_number),
    CONSTRAINT upload_parts_size_bytes_not_null NOT NULL size_bytes,
    CONSTRAINT upload_parts_upload_session_id_fkey FOREIGN KEY (upload_session_id) REFERENCES media.upload_sessions(id) ON DELETE CASCADE,
    CONSTRAINT upload_parts_upload_session_id_not_null NOT NULL upload_session_id
);

CREATE INDEX idx_blob_references_blob_id ON media.blob_references USING btree (blob_id);

CREATE UNIQUE INDEX idx_blob_references_unique ON media.blob_references USING btree (blob_id, service_name, entity_type, entity_id);

CREATE INDEX idx_blobs_retention_state ON media.blobs USING btree (retention_state);

CREATE INDEX idx_blobs_sha256 ON media.blobs USING btree (sha256) WHERE (sha256 IS NOT NULL);

CREATE INDEX idx_media_asset_variants_blob ON media.media_asset_variants USING btree (blob_id);

CREATE INDEX idx_media_assets_failed_original ON media.media_assets USING btree (failed_at) WHERE ((status = 'FAILED'::text) AND (source_blob_id IS NOT NULL));

CREATE INDEX idx_media_assets_owner_status ON media.media_assets USING btree (owner_key, status, created_at DESC);

CREATE INDEX idx_media_assets_rejected_original ON media.media_assets USING btree (failed_at) WHERE ((status = 'REJECTED'::text) AND (source_blob_id IS NOT NULL));

CREATE INDEX idx_media_assets_source_blob ON media.media_assets USING btree (source_blob_id) WHERE (source_blob_id IS NOT NULL);

CREATE INDEX idx_media_assets_upload_session ON media.media_assets USING btree (upload_session_id) WHERE (upload_session_id IS NOT NULL);

CREATE INDEX idx_media_jobs_asset_generation ON media.media_jobs USING btree (asset_id, generation, created_at DESC) WHERE (asset_id IS NOT NULL);

CREATE INDEX idx_media_jobs_asset_id ON media.media_jobs USING btree (asset_id) WHERE (asset_id IS NOT NULL);

CREATE INDEX idx_media_jobs_leased_until ON media.media_jobs USING btree (leased_until) WHERE (status = 'leased'::text);

CREATE INDEX idx_media_jobs_ready ON media.media_jobs USING btree (status, next_attempt_at, created_at) WHERE (status = ANY (ARRAY['pending'::text, 'retry'::text]));

CREATE INDEX idx_media_outbox_namespace_sequence ON media.media_outbox USING btree (client_namespace, sequence);

CREATE INDEX idx_media_outbox_sequence ON media.media_outbox USING btree (sequence);

CREATE INDEX idx_media_processing_runs_asset ON media.media_processing_runs USING btree (asset_id, generation DESC);

CREATE INDEX idx_media_processing_runs_status ON media.media_processing_runs USING btree (status, created_at);
