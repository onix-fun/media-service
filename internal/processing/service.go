package processing

import (
	"context"
	"fmt"

	"github.com/onix-fun/media-service/internal/storage"

	"github.com/google/uuid"
)

// Service defines the interface for media processing pipelines
// Note: Actual transcoding/image resizing would typically be offloaded to
// specialized workers (e.g. using FFMPEG). This service orchestrates the DAG relationships.
type Service interface {
	RegisterArtifact(ctx context.Context, sourceBlobID, artifactBlobID uuid.UUID, relationType string) error
	// other methods like SubmitJob(blobID) could go here
}

type service struct {
	metadata storage.MetadataRepo
}

func NewService(metadata storage.MetadataRepo) Service {
	return &service{
		metadata: metadata,
	}
}

func (s *service) RegisterArtifact(ctx context.Context, sourceBlobID, artifactBlobID uuid.UUID, relationType string) error {
	// Register the relationship in the DAG
	err := s.metadata.CreateBlobRelation(ctx, sourceBlobID, artifactBlobID, relationType)
	if err != nil {
		return fmt.Errorf("failed to create blob relation (DAG link): %w", err)
	}
	return nil
}
