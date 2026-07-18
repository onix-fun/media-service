package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	minioClient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/onix-fun/media/internal/adapters/grpcapi"
	"github.com/onix-fun/media/internal/adapters/jobs"
	"github.com/onix-fun/media/internal/adapters/storage/minio"
	"github.com/onix-fun/media/internal/adapters/storage/postgres"
	"github.com/onix-fun/media/internal/application/asset"
	"github.com/onix-fun/media/internal/application/gc"
	"github.com/onix-fun/media/internal/application/storage"
	"github.com/onix-fun/media/internal/application/upload"
	"github.com/onix-fun/media/internal/application/worker"
	mediapb "github.com/onix-fun/media/internal/gen/media"
	"github.com/onix-fun/media/internal/platform/config"
	"google.golang.org/grpc"
	grpccredentials "google.golang.org/grpc/credentials"
)

type jobHandler struct {
	hash       *worker.HashWorker
	profiles   *worker.ProfileWorker
	metadata   storage.MetadataRepo
	queue      *jobs.Store
	configured map[string]config.Profile
	assetSvc   asset.Service
	assetPipe  *asset.Pipeline
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
		assetUpload, assetLookupErr := h.metadata.GetMediaAssetByUploadSession(ctx, id)
		if assetLookupErr != nil {
			return assetLookupErr
		}
		// Legacy blob uploads may still opt into automatic YAML profiles. Asset
		// uploads are source-only until a consumer requests a typed pipeline.
		if assetUpload == nil {
			for name, profile := range h.configured {
				if profile.Automatic && (len(profile.MIME) == 0 || contains(profile.MIME, session.MimeType)) {
					if err := h.queue.PublishProcess(ctx, *session.BlobID, name); err != nil {
						return err
					}
				}
			}
		}
		_, err = h.assetSvc.BeginProcessingForSession(ctx, id)
		if err != nil {
			return err
		}
		return nil
	case "process":
		id, err := uuid.Parse(job.BlobID)
		if err != nil {
			return err
		}
		return h.profiles.Process(ctx, id, job.Profile)
	case "asset":
		id, err := uuid.Parse(job.AssetID)
		if err != nil {
			return err
		}
		run, lookupErr := h.metadata.GetLatestProcessingRun(ctx, id)
		if lookupErr != nil {
			return lookupErr
		}
		attempts := job.Attempts
		if run != nil && strings.HasPrefix(run.IdempotencyKey, "auto-retry-once:") {
			attempts = 1
		}
		err = h.assetPipe.ProcessQueued(ctx, id, job.Generation, attempts)
		if err == nil || job.Attempts > 0 || isPermanentProcessingError(err) {
			return err
		}
		// A transient first failure is retried as a new immutable generation.
		// The idempotency prefix is durable and prevents another automatic
		// generation if that retry also fails.
		if run == nil {
			return err
		}
		if strings.HasPrefix(run.IdempotencyKey, "auto-retry-once:") {
			return terminalProcessingError{err}
		}
		if _, cancelErr := h.assetSvc.CancelProcessing(ctx, run.ID, run.ClientNamespace, run.OwnerRef); cancelErr != nil {
			return err
		}
		_, retryErr := h.assetSvc.RetryProcessing(ctx, run.ID, run.ClientNamespace, run.OwnerRef, "auto-retry-once:"+run.ID.String())
		return retryErr
	default:
		return fmt.Errorf("unknown job type %q", job.Type)
	}
}

func isPermanentProcessingError(err error) bool {
	var permanent interface{ Permanent() bool }
	return errors.As(err, &permanent) && permanent.Permanent()
}

type terminalProcessingError struct{ error }

func (terminalProcessingError) Permanent() bool { return true }

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// @title Media Service API
// @version 1.0
// @description Microservice for handling media uploads, processing (thumbnails/previews), and storage (S3).
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.url http://www.swagger.io/support
// @contact.email support@swagger.io

// @license.name MIT
// @license.url https://opensource.org/licenses/MIT

// @host localhost:8080
// @BasePath /v1

// @securityDefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) < 2 {
		log.Error("usage: media <serve|config>")
		os.Exit(2)
	}
	if os.Args[1] == "config" && (len(os.Args) < 3 || os.Args[2] != "validate") {
		log.Error("usage: media config validate --config=<path>")
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
		migrationURL, e := databaseMigrationURL(cfg.Database.URL)
		if e != nil {
			log.Error("migration config failed", "error", e)
			os.Exit(1)
		}
		m, e := migrate.New(cfg.Database.MigrationPath, migrationURL)
		if e != nil {
			log.Error("migrator failed", "error", e)
			os.Exit(1)
		}
		if e = m.Up(); e != nil && !errors.Is(e, migrate.ErrNoChange) {
			log.Error("migration failed", "error", e)
			os.Exit(1)
		}
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.Database.URL)
	if err != nil {
		log.Error("database config failed", "error", err)
		os.Exit(1)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = "media,public"
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Error("database failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()
	repo := postgres.NewMetadataRepo(pool)
	for service, aliases := range cfg.Aliases {
		if len(aliases) == 0 {
			continue
		}
		if err := repo.GrantServiceAliasAccess(ctx, service, aliases); err != nil {
			log.Error("service alias repair failed", "service", service, "aliases", aliases, "error", err)
			os.Exit(1)
		}
		log.Info("service alias access repaired", "service", service, "aliases", aliases)
	}
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
	queue := jobs.New(pool, cfg.Jobs, log)
	hash := worker.NewHashWorker(repo, blobs, cfg.Scanning)
	profiles := worker.NewProfileWorker(repo, blobs, cfg.Profiles)
	errs := make(chan error, 3)
	svc := upload.NewService(repo, blobs, queue, cfg.Uploads.UploadExpiry, cfg.Uploads.DownloadExpiry, cfg.Aliases)
	assetSvc := asset.NewService(repo, svc, queue, blobs)
	assetPipe := asset.NewPipeline(repo, blobs, profiles)
	if *role == "all" || *role == "worker" {
		go func() {
			errs <- queue.Run(ctx, jobHandler{hash: hash, profiles: profiles, metadata: repo, queue: queue, configured: cfg.Profiles, assetSvc: assetSvc, assetPipe: assetPipe})
		}()
		go gc.NewWorker(repo, blobs, cfg.GC.Interval, cfg.GC.GracePeriod).Start(ctx)
		go asset.NewFailedOriginalGC(repo, blobs, 24*time.Hour, 7*24*time.Hour, log).Start(ctx)
	}
	var server *http.Server
	var grpcServer *grpc.Server
	if *role == "all" || *role == "api" {
		mux := http.NewServeMux()
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
		created, e := newGRPCServer(cfg)
		if e != nil {
			log.Error("grpc server failed", "error", e)
			os.Exit(1)
		}
		grpcServer = created
		mediapb.RegisterMediaServiceServer(grpcServer, grpcapi.New(assetSvc, cfg.Service.APIKey))
		listener, e := net.Listen("tcp", cfg.Service.GRPCAddr)
		if e != nil {
			log.Error("grpc listen failed", "error", e)
			os.Exit(1)
		}
		go func() {
			log.Info("gRPC server started", "addr", cfg.Service.GRPCAddr, "tls", cfg.Service.GRPCTLS)
			if e := grpcServer.Serve(listener); e != nil {
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
	if grpcServer != nil {
		grpcServer.GracefulStop()
	}
}

// PostgreSQL resolves the default "$user" schema before public. The media
// schema does not exist on the first run but does on every restart, so an
// implicit search_path makes golang-migrate switch metadata tables and replay
// V1. Pin migration metadata to public; application connections still use
// media,public below.
func databaseMigrationURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse database URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", fmt.Errorf("database URL must use postgres or postgresql scheme")
	}
	query := parsed.Query()
	query.Set("search_path", "public")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func newGRPCServer(cfg config.Config) (*grpc.Server, error) {
	if !cfg.Service.GRPCTLS {
		return grpc.NewServer(), nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.Service.GRPCCertFile, cfg.Service.GRPCKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load grpc cert: %w", err)
	}
	ca, err := os.ReadFile(cfg.Service.GRPCClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read grpc client ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid grpc client ca")
	}
	return grpc.NewServer(grpc.Creds(grpccredentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}))), nil
}
