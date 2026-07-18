package minio

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	minioClient "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// This is deliberately opt-in because it talks to the local Docker topology.
// It protects the browser-critical path: public MinIO host -> signed PUT ->
// multipart ETag. Run with ONIX_TEST_LOCAL_MEDIA=1.
func TestLocalPublicPresignedMultipartUpload(t *testing.T) {
	if os.Getenv("ONIX_TEST_LOCAL_MEDIA") != "1" {
		t.Skip("set ONIX_TEST_LOCAL_MEDIA=1 to test the local media topology")
	}
	endpoint := os.Getenv("ONIX_MEDIA_PUBLIC_ENDPOINT")
	if endpoint == "" {
		endpoint = "media.onix.localhost:9010"
	}
	accessKey := os.Getenv("MEDIA_S3_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "media"
	}
	secretKey := os.Getenv("MEDIA_S3_SECRET_KEY")
	if secretKey == "" {
		secretKey = "media-secret"
	}
	client, err := minioClient.New(endpoint, &minioClient.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
		Region: "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	key := path.Join("uploads", "public-endpoint-integration-"+time.Now().UTC().Format("20060102150405.000000000"))
	core := &minioClient.Core{Client: client}
	uploadID, err := core.NewMultipartUpload(ctx, "media-blobs", key, minioClient.PutObjectOptions{ContentType: "image/png"})
	if err != nil {
		t.Fatalf("create multipart upload through public endpoint: %v", err)
	}
	defer func() { _ = core.AbortMultipartUpload(ctx, "media-blobs", key, uploadID) }()

	query := make(url.Values)
	query.Set("uploadId", uploadID)
	query.Set("partNumber", "1")
	u, err := client.Presign(ctx, http.MethodPut, "media-blobs", key, time.Hour, query)
	if err != nil {
		t.Fatalf("presign public multipart part: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), bytes.NewReader([]byte("onix-media")))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "image/png")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("PUT signed part through public endpoint: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		t.Fatalf("PUT signed part returned %s", response.Status)
	}
	if response.Header.Get("ETag") == "" {
		t.Fatal("PUT signed part did not return ETag")
	}
}
