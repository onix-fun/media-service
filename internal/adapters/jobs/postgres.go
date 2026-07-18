package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/onix-fun/media/internal/platform/config"
)

type Job struct {
	ID         string
	Type       string
	SessionID  string
	BlobID     string
	AssetID    string
	Profile    string
	Generation int64
	Attempts   int64
}

type Handler interface {
	HandleJob(context.Context, Job) error
}

// PermanentError identifies malformed input and configuration failures. They
// are persisted as a failed asset but are not pointlessly retried five times;
// the owner can explicitly retry after replacing the source or a deployment
// fix, which creates a new asset generation.
type PermanentError interface {
	error
	Permanent() bool
}

type Store struct {
	pool *pgxpool.Pool
	cfg  config.Jobs
	log  *slog.Logger
}

func New(pool *pgxpool.Pool, cfg config.Jobs, log *slog.Logger) *Store {
	return &Store{pool: pool, cfg: cfg, log: log}
}

func (s *Store) PublishHash(ctx context.Context, sessionID uuid.UUID) error {
	return s.enqueue(ctx, Job{
		ID:        uuid.Must(uuid.NewV7()).String(),
		Type:      "hash",
		SessionID: sessionID.String(),
	})
}

func (s *Store) PublishProcess(ctx context.Context, blobID uuid.UUID, profile string) error {
	return s.enqueue(ctx, Job{
		ID:      uuid.Must(uuid.NewV7()).String(),
		Type:    "process",
		BlobID:  blobID.String(),
		Profile: profile,
	})
}

// PublishAsset schedules the content delivery pipeline for exactly one asset.
// The key is asset-scoped so duplicate hash completions stay idempotent.
func (s *Store) PublishAsset(ctx context.Context, assetID uuid.UUID, generation int64) error {
	return s.enqueue(ctx, Job{
		ID:         uuid.Must(uuid.NewV7()).String(),
		Type:       "asset",
		AssetID:    assetID.String(),
		Generation: generation,
	})
}

func (s *Store) enqueue(ctx context.Context, job Job) error {
	key, err := jobKey(job)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO media_jobs (id, job_key, type, session_id, blob_id, asset_id, profile, generation, status, next_attempt_at)
		VALUES ($1, $2, $3, NULLIF($4, '')::uuid, NULLIF($5, '')::uuid, NULLIF($6, '')::uuid, NULLIF($7, ''), $8, 'pending', NOW())
		ON CONFLICT (job_key) DO NOTHING
	`, job.ID, key, job.Type, job.SessionID, job.BlobID, job.AssetID, job.Profile, job.Generation)
	return err
}

func (s *Store) Run(ctx context.Context, handler Handler) error {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		processed, err := s.processOnce(ctx, handler)
		if err != nil {
			s.log.Error("media job poll failed", "error", err)
		}
		if processed {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Store) processOnce(ctx context.Context, handler Handler) (bool, error) {
	jobs, err := s.lease(ctx)
	if err != nil {
		return false, err
	}
	if len(jobs) == 0 {
		return false, nil
	}
	for _, job := range jobs {
		if ctx.Err() != nil {
			return true, nil
		}
		if err := handler.HandleJob(ctx, job); err != nil {
			s.log.Error("media job failed", "id", job.ID, "type", job.Type, "attempts", job.Attempts+1, "error", err)
			if markErr := s.markFailed(ctx, job, err); markErr != nil {
				return true, markErr
			}
			continue
		}
		if err := s.markDone(ctx, job.ID); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (s *Store) lease(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM media_jobs
			WHERE (status IN ('pending', 'retry') AND next_attempt_at <= NOW())
			   OR (status = 'leased' AND leased_until <= NOW())
			ORDER BY created_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE media_jobs j
		SET status = 'leased',
		    leased_until = NOW() + ($2 * INTERVAL '1 second'),
		    updated_at = NOW()
		FROM picked
		WHERE j.id = picked.id
		RETURNING j.id, j.type, COALESCE(j.session_id::text, ''), COALESCE(j.blob_id::text, ''), COALESCE(j.asset_id::text, ''), COALESCE(j.profile, ''), j.generation, j.attempts
	`, s.cfg.BatchSize, int64(s.cfg.LeaseDuration.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Type, &job.SessionID, &job.BlobID, &job.AssetID, &job.Profile, &job.Generation, &job.Attempts); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) markDone(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE media_jobs
		SET status = 'done', leased_until = NULL, last_error = NULL, updated_at = NOW()
		WHERE id = $1
	`, id)
	return err
}

func (s *Store) markFailed(ctx context.Context, job Job, cause error) error {
	attempts := job.Attempts + 1
	status := "retry"
	nextAttempt := time.Now().Add(backoff(attempts))
	var permanent PermanentError
	if (errors.As(cause, &permanent) && permanent.Permanent()) || attempts >= s.cfg.MaxRetries {
		status = "dead"
		nextAttempt = time.Now()
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE media_jobs
		SET status = $2,
		    attempts = $3,
		    next_attempt_at = $4,
		    leased_until = NULL,
		    last_error = $5,
		    updated_at = NOW()
		WHERE id = $1
	`, job.ID, status, attempts, nextAttempt, cause.Error())
	return err
}

func jobKey(job Job) (string, error) {
	switch job.Type {
	case "hash":
		if job.SessionID == "" {
			return "", fmt.Errorf("hash job requires session id")
		}
		return "hash:" + job.SessionID, nil
	case "process":
		if job.BlobID == "" || job.Profile == "" {
			return "", fmt.Errorf("process job requires blob id and profile")
		}
		return "process:" + job.BlobID + ":" + job.Profile, nil
	case "asset":
		if job.AssetID == "" || job.Generation < 1 {
			return "", fmt.Errorf("asset job requires asset id")
		}
		return fmt.Sprintf("asset:%s:g%d", job.AssetID, job.Generation), nil
	default:
		return "", fmt.Errorf("unknown job type %q", job.Type)
	}
}

func backoff(attempts int64) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	seconds := int64(1) << min(attempts-1, 5)
	return time.Duration(seconds) * time.Second
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
