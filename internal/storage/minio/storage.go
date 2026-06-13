package minio

import (
	"context"
	"io"
	"net/url"
	"strconv"
	"time"

	"media-service/internal/domain"
	"media-service/internal/storage"

	minioClient "github.com/minio/minio-go/v7"
)

type blobStorage struct {
	client        *minioClient.Client
	presignClient *minioClient.Client
	core          *minioClient.Core
	bucket        string
}

// NewBlobStorage creates a new MinIO backed BlobStorage
func NewBlobStorage(client *minioClient.Client, presignClient *minioClient.Client, bucket string) storage.BlobStorage {
	core := &minioClient.Core{Client: client}
	return &blobStorage{
		client:        client,
		presignClient: presignClient,
		core:          core,
		bucket:        bucket,
	}
}

func (s *blobStorage) CreateMultipartUpload(ctx context.Context, key string, contentType string) (string, error) {
	opts := minioClient.PutObjectOptions{ContentType: contentType}
	return s.core.NewMultipartUpload(ctx, s.bucket, key, opts)
}

func (s *blobStorage) GeneratePresignedPartURL(ctx context.Context, key string, uploadID string, partNumber int, expiry time.Duration) (string, error) {
	reqParams := make(url.Values)
	reqParams.Set("uploadId", uploadID)
	reqParams.Set("partNumber", strconv.Itoa(partNumber))

	u, err := s.presignClient.Presign(ctx, "PUT", s.bucket, key, expiry, reqParams)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}

func (s *blobStorage) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []domain.UploadPart) error {
	minioParts := make([]minioClient.CompletePart, len(parts))
	for i, p := range parts {
		minioParts[i] = minioClient.CompletePart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}

	_, err := s.core.CompleteMultipartUpload(ctx, s.bucket, key, uploadID, minioParts, minioClient.PutObjectOptions{})
	return err
}

func (s *blobStorage) AbortMultipartUpload(ctx context.Context, key string, uploadID string) error {
	return s.core.AbortMultipartUpload(ctx, s.bucket, key, uploadID)
}

func (s *blobStorage) CopyBlob(ctx context.Context, srcKey, dstKey string) error {
	src := minioClient.CopySrcOptions{
		Bucket: s.bucket,
		Object: srcKey,
	}
	dst := minioClient.CopyDestOptions{
		Bucket: s.bucket,
		Object: dstKey,
	}
	_, err := s.client.CopyObject(ctx, dst, src)
	return err
}

func (s *blobStorage) GetBlobStream(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.client.GetObject(ctx, s.bucket, key, minioClient.GetObjectOptions{})
}

func (s *blobStorage) DeleteBlob(ctx context.Context, key string) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minioClient.RemoveObjectOptions{})
}

func (s *blobStorage) GetPresignedDownloadURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	reqParams := make(url.Values)
	// Force Content-Disposition inline for browser viewing (images, videos)
	reqParams.Set("response-content-disposition", "inline")
	u, err := s.presignClient.Presign(ctx, "GET", s.bucket, key, expiry, reqParams)
	if err != nil {
		return "", err
	}
	return u.String(), nil
}
