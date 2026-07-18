package minio

import (
	"context"
	"net/url"
	"testing"
	"time"

	minioClient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestBrowserPresignedUploadAndDownloadUsePublicEndpoint(t *testing.T) {
	internal, err := minioClient.New("internal-minio:9000", &minioClient.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	public, err := minioClient.New("media.onix.localhost:9010", &minioClient.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	storage := NewBlobStorage(internal, public, "media-blobs")

	uploadURL, err := storage.GeneratePresignedPartURL(context.Background(), "uploads/session", "upload-id", 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	downloadURL, err := storage.GetPresignedDownloadURL(context.Background(), "blobs/e2/e8/hash", time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	parsedUpload, err := url.Parse(uploadURL)
	if err != nil {
		t.Fatal(err)
	}
	parsedDownload, err := url.Parse(downloadURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsedUpload.Host != "media.onix.localhost:9010" {
		t.Fatalf("expected public upload host, got %s", parsedUpload.Host)
	}
	if parsedDownload.Host != "media.onix.localhost:9010" {
		t.Fatalf("expected public download host, got %s", parsedDownload.Host)
	}
}
