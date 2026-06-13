//go:generate go run github.com/swaggo/swag/cmd/swag@latest init -g cmd/media-service/main.go --parseDependency --parseInternal -o docs

package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"media-service/internal/api"
	"media-service/internal/gc"
	"media-service/internal/processing"
	"media-service/internal/storage/minio"
	"media-service/internal/storage/postgres"
	"media-service/internal/upload"
	"media-service/internal/worker"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	minioClient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// @title Media Service Control Plane API
// @version 1.0
// @description Internal platform service for handling blob lifecycles, multipart uploads, and processing orchestration.
// @termsOfService http://swagger.io/terms/

// @contact.name API Support
// @contact.email support@swagger.io

// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html

// @host localhost:8080
// @BasePath /
func parseDuration(key string, defaultVal time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(val)
	if err != nil {
		log.Fatalf("Invalid %s: %q — expected duration (e.g. 10m, 24h): %v", key, val, err)
	}
	return d
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// App settings
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// 1. Initialize PostgreSQL
	dbUrl := os.Getenv("DATABASE_URL")
	if dbUrl == "" {
		dbUrl = "postgres://postgres:password@localhost:5432/media?sslmode=disable"
	}

	// 0. Run Migrations automatically if enabled
	autoMigrate := os.Getenv("AUTO_MIGRATE")
	if autoMigrate == "" || autoMigrate == "true" {
		migrationPath := os.Getenv("MIGRATION_PATH")
		if migrationPath == "" {
			migrationPath = "file://migrations"
		}
		m, err := migrate.New(migrationPath, dbUrl)
		if err != nil {
			log.Fatalf("Failed to initialize migrator: %v", err)
		}
		if err := m.Up(); err != nil && err != migrate.ErrNoChange {
			log.Fatalf("Failed to apply migrations: %v", err)
		}
		log.Println("Migrations applied successfully!")
	}

	pool, err := pgxpool.New(ctx, dbUrl)
	if err != nil {
		log.Fatalf("Failed to connect to postgres: %v", err)
	}
	defer pool.Close()
	metadataRepo := postgres.NewMetadataRepo(pool)

	// 2. Initialize MinIO / S3 Storage
	s3Endpoint := os.Getenv("S3_ENDPOINT")
	if s3Endpoint == "" {
		s3Endpoint = "localhost:9010"
	}
	s3AccessKey := os.Getenv("S3_ACCESS_KEY")
	if s3AccessKey == "" {
		s3AccessKey = "minioadmin"
	}
	s3SecretKey := os.Getenv("S3_SECRET_KEY")
	if s3SecretKey == "" {
		s3SecretKey = "minioadmin"
	}
	s3Bucket := os.Getenv("S3_BUCKET")
	if s3Bucket == "" {
		s3Bucket = "media-blobs"
	}
	s3Region := os.Getenv("S3_REGION")
	if s3Region == "" {
		s3Region = "us-east-1"
	}
	s3UseSSL := os.Getenv("S3_USE_SSL") == "true"

	mc, err := minioClient.New(s3Endpoint, &minioClient.Options{
		Creds:  credentials.NewStaticV4(s3AccessKey, s3SecretKey, ""),
		Secure: s3UseSSL,
		Region: s3Region,
	})
	if err != nil {
		log.Fatalf("Failed to initialize S3 client: %v", err)
	}
	s3PublicEndpoint := os.Getenv("S3_PUBLIC_ENDPOINT")
	if s3PublicEndpoint == "" {
		s3PublicEndpoint = s3Endpoint
	}
	publicClient, err := minioClient.New(s3PublicEndpoint, &minioClient.Options{
		Creds:  credentials.NewStaticV4(s3AccessKey, s3SecretKey, ""),
		Secure: os.Getenv("S3_PUBLIC_USE_SSL") == "true",
		Region: s3Region,
	})
	if err != nil {
		log.Fatalf("Failed to initialize public S3 signing client: %v", err)
	}
	blobStorage := minio.NewBlobStorage(mc, publicClient, s3Bucket)

	// 3. Setup Async Hashing Worker
	hashBufferSize := 100
	gcInterval := parseDuration("GC_SWEEP_INTERVAL", 10*time.Minute)
	gcGracePeriod := parseDuration("GC_GRACE_PERIOD", 24*time.Hour)
	presignedUploadExpiry := parseDuration("PRESIGNED_UPLOAD_EXPIRY", 24*time.Hour)
	presignedDownloadExpiry := parseDuration("PRESIGNED_DOWNLOAD_EXPIRY", 1*time.Hour)

	hashChan := make(chan uuid.UUID, hashBufferSize)
	hashWorker := worker.NewHashWorker(metadataRepo, blobStorage, hashChan)
	go hashWorker.Start(ctx)

	// 4. Setup GC Worker
	gcWorker := gc.NewWorker(metadataRepo, blobStorage, gcInterval, gcGracePeriod)
	go gcWorker.Start(ctx)

	// 5. Setup Processing Service (Skeleton)
	processingSvc := processing.NewService(metadataRepo)
	_ = processingSvc // Ready to be injected into future handlers or event listeners

	// 6. Setup Upload Service & API
	uploadSvc := upload.NewService(metadataRepo, blobStorage, hashChan, presignedUploadExpiry, presignedDownloadExpiry)

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Get("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := api.NewHandlers(uploadSvc)
	internalAuthSecret := os.Getenv("INTERNAL_AUTH_SECRET")
	if internalAuthSecret == "" {
		log.Fatal("INTERNAL_AUTH_SECRET is required")
	}
	apiRouter := api.NewRouter(h, internalAuthSecret)

	r.Mount("/", apiRouter)

	// Swagger documentation endpoint (only if not in PROD)
	if os.Getenv("ENV") != "production" {
		r.Get("/openapi.json", func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, "docs/openapi.json")
		})
		r.Get("/swagger/*", httpSwagger.Handler(
			httpSwagger.URL("/openapi.json"),
		))
		log.Printf("Swagger docs available at http://localhost:%s/swagger/index.html\n", port)
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("Media Service starting on :%s...\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown failed: %v", err)
	}
}
