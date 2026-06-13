package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	minioClient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/onix-fun/media-service/internal/api"
	"github.com/onix-fun/media-service/internal/config"
	"github.com/onix-fun/media-service/internal/gc"
	"github.com/onix-fun/media-service/internal/jobs"
	"github.com/onix-fun/media-service/internal/storage"
	"github.com/onix-fun/media-service/internal/storage/minio"
	"github.com/onix-fun/media-service/internal/storage/postgres"
	"github.com/onix-fun/media-service/internal/upload"
	"github.com/onix-fun/media-service/internal/worker"
)

type jobHandler struct {
	hash       *worker.HashWorker
	profiles   *worker.ProfileWorker
	metadata   storage.MetadataRepo
	queue      *jobs.Rabbit
	configured map[string]config.Profile
}

func (h jobHandler) HandleJob(ctx context.Context, job jobs.Job) error {
	switch job.Type {
	case "hash":
		id, err := uuid.Parse(job.SessionID)
		if err != nil {
			return err
		}
		if err := h.hash.ProcessSession(ctx, id); err != nil {
			return err
		}
		session, err := h.metadata.GetUploadSession(ctx, id)
		if err != nil || session == nil || session.BlobID == nil {
			return err
		}
		for name, profile := range h.configured {
			if profile.Automatic && (len(profile.MIME) == 0 || contains(profile.MIME, session.MimeType)) {
				if err := h.queue.PublishProcess(ctx, *session.BlobID, name); err != nil {
					return err
				}
			}
		}
		return nil
	case "process":
		id, err := uuid.Parse(job.BlobID)
		if err != nil {
			return err
		}
		return h.profiles.Process(ctx, id, job.Profile)
	default:
		return fmt.Errorf("unknown job type %q", job.Type)
	}
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) < 2 {
		log.Error("usage: media-service <serve|config>")
		os.Exit(2)
	}
	if os.Args[1] == "config" && (len(os.Args) < 3 || os.Args[2] != "validate") {
		log.Error("usage: media-service config validate --config=<path>")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	path := fs.String("config", "config/config.example.yaml", "YAML config")
	role := fs.String("role", "all", "api, worker or all")
	flagArgs := os.Args[2:]
	if os.Args[1] == "config" {
		flagArgs = os.Args[3:]
	}
	_ = fs.Parse(flagArgs)
	cfg, err := config.Load(*path)
	if err != nil {
		log.Error("invalid config", "error", err)
		os.Exit(1)
	}
	if os.Args[1] == "config" {
		log.Info("config is valid")
		return
	}
	if os.Args[1] != "serve" {
		log.Error("unknown command")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if cfg.Database.AutoMigrate {
		m, e := migrate.New(cfg.Database.MigrationPath, cfg.Database.URL)
		if e != nil {
			log.Error("migrator failed", "error", e)
			os.Exit(1)
		}
		if e = m.Up(); e != nil && !errors.Is(e, migrate.ErrNoChange) {
			log.Error("migration failed", "error", e)
			os.Exit(1)
		}
	}
	pool, err := pgxpool.New(ctx, cfg.Database.URL)
	if err != nil {
		log.Error("database failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	repo := postgres.NewMetadataRepo(pool)
	private, err := minioClient.New(cfg.S3.Endpoint, &minioClient.Options{Creds: credentials.NewStaticV4(cfg.S3.AccessKey, cfg.S3.SecretKey, ""), Secure: cfg.S3.UseSSL, Region: cfg.S3.Region})
	if err != nil {
		log.Error("s3 failed", "error", err)
		os.Exit(1)
	}
	publicEndpoint := cfg.S3.PublicEndpoint
	if publicEndpoint == "" {
		publicEndpoint = cfg.S3.Endpoint
	}
	public, err := minioClient.New(publicEndpoint, &minioClient.Options{Creds: credentials.NewStaticV4(cfg.S3.AccessKey, cfg.S3.SecretKey, ""), Secure: cfg.S3.PublicUseSSL, Region: cfg.S3.Region})
	if err != nil {
		log.Error("public s3 failed", "error", err)
		os.Exit(1)
	}
	blobs := minio.NewBlobStorage(private, public, cfg.S3.Bucket)
	queue := jobs.New(cfg.RabbitMQ, log)
	hash := worker.NewHashWorker(repo, blobs, cfg.Scanning)
	profiles := worker.NewProfileWorker(repo, blobs, cfg.Profiles)
	errs := make(chan error, 3)
	if *role == "all" || *role == "worker" {
		go func() {
			errs <- queue.Run(ctx, jobHandler{hash: hash, profiles: profiles, metadata: repo, queue: queue, configured: cfg.Profiles})
		}()
		go gc.NewWorker(repo, blobs, cfg.GC.Interval, cfg.GC.GracePeriod).Start(ctx)
	}
	var server *http.Server
	if *role == "all" || *role == "api" {
		svc := upload.NewService(repo, blobs, queue, cfg.Uploads.UploadExpiry, cfg.Uploads.DownloadExpiry)
		mux := http.NewServeMux()
		mux.Handle("/v1/", http.StripPrefix("/v1", api.NewRouter(api.NewHandlers(svc), cfg.Service.APIKey)))
		mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
			if pool.Ping(r.Context()) != nil {
				http.Error(w, "not ready", 503)
				return
			}
			w.WriteHeader(200)
		})
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("media_service_up 1\n")) })
		server = &http.Server{Addr: cfg.Service.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			e := server.ListenAndServe()
			if !errors.Is(e, http.ErrServerClosed) {
				errs <- e
			}
		}()
	}
	select {
	case <-ctx.Done():
	case err := <-errs:
		log.Error("service stopped", "error", err)
	}
	if server != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}
}
