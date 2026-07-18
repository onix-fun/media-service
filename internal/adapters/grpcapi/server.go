package grpcapi

import (
	"context"
	"crypto/subtle"
	"strings"
	"time"

	"github.com/google/uuid"
	assetservice "github.com/onix-fun/media/internal/application/asset"
	"github.com/onix-fun/media/internal/domain"
	mediapb "github.com/onix-fun/media/internal/gen/media"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Server struct {
	mediapb.UnimplementedMediaServiceServer
	assets assetservice.Service
	apiKey string
}

func New(assets assetservice.Service, apiKey string) *Server {
	return &Server{assets: assets, apiKey: apiKey}
}

func (s *Server) BeginAssetUpload(ctx context.Context, req *mediapb.BeginAssetUploadRequest) (*mediapb.BeginAssetUploadResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	kind, err := kindFromProto(req.GetDeclaredKind())
	if err != nil {
		return nil, err
	}
	size := req.GetExpectedSize()
	var expected *int64
	if size > 0 {
		expected = &size
	}
	asset, session, parts, err := s.assets.BeginUpload(ctx, ns, req.GetOwnerRef(), kind, req.GetMimeType(), expected, int(req.GetPartsCount()), req.GetSourcePolicyId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &mediapb.BeginAssetUploadResponse{Source: sourceToProto(asset), SessionId: session.ID.String(), Parts: intMap(parts), ExpiresAt: session.ExpiresAt.Format(time.RFC3339)}, nil
}

func (s *Server) CompleteAssetUpload(ctx context.Context, req *mediapb.CompleteAssetUploadRequest) (*mediapb.AssetSourceResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	assetID, err := uuid.Parse(req.GetAssetId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid asset_id")
	}
	sessionID, err := uuid.Parse(req.GetSessionId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid session_id")
	}
	parts := make([]domain.UploadPart, 0, len(req.GetParts()))
	for _, part := range req.GetParts() {
		parts = append(parts, domain.UploadPart{PartNumber: int(part.GetPartNumber()), ETag: part.GetEtag()})
	}
	asset, err := s.assets.Complete(ctx, assetID, sessionID, parts, ns+":"+strings.TrimSpace(req.GetOwnerRef()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &mediapb.AssetSourceResponse{Source: sourceToProto(asset)}, nil
}

func (s *Server) GetAssetSource(ctx context.Context, req *mediapb.GetAssetSourceRequest) (*mediapb.AssetSourceResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAssetId(), "asset_id")
	if err != nil {
		return nil, err
	}
	asset, run, err := s.assets.GetSource(ctx, id, ns, req.GetOwnerRef())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &mediapb.AssetSourceResponse{Source: sourceToProto(asset), LatestRun: runToProto(run)}, nil
}

func (s *Server) BatchGetAssetSources(ctx context.Context, req *mediapb.BatchGetAssetSourcesRequest) (*mediapb.BatchGetAssetSourcesResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	if len(req.GetAssetIds()) > 50 {
		return nil, status.Error(codes.InvalidArgument, "at most 50 asset_ids are allowed")
	}
	response := &mediapb.BatchGetAssetSourcesResponse{}
	seen := make(map[string]struct{}, len(req.GetAssetIds()))
	for _, raw := range req.GetAssetIds() {
		raw = strings.TrimSpace(raw)
		if _, duplicate := seen[raw]; duplicate {
			continue
		}
		seen[raw] = struct{}{}
		id, parseErr := parseID(raw, "asset_id")
		if parseErr != nil {
			response.MissingAssetIds = append(response.MissingAssetIds, raw)
			continue
		}
		asset, run, getErr := s.assets.GetSource(ctx, id, ns, req.GetOwnerRef())
		if getErr != nil {
			// Missing and unauthorized assets intentionally share one result so
			// callers cannot use this owner-scoped batch as an enumeration API.
			response.MissingAssetIds = append(response.MissingAssetIds, raw)
			continue
		}
		response.Sources = append(response.Sources, &mediapb.AssetSourceResponse{Source: sourceToProto(asset), LatestRun: runToProto(run)})
	}
	return response, nil
}

func (s *Server) RequestProcessing(ctx context.Context, req *mediapb.RequestProcessingRequest) (*mediapb.ProcessingRunResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAssetId(), "asset_id")
	if err != nil {
		return nil, err
	}
	pipeline, err := pipelineFromProto(req.GetPipelineId())
	if err != nil {
		return nil, err
	}
	run, err := s.assets.RequestProcessing(ctx, id, ns, req.GetOwnerRef(), pipeline, req.GetIdempotencyKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &mediapb.ProcessingRunResponse{Run: runToProto(run)}, nil
}

func (s *Server) GetProcessingRun(ctx context.Context, req *mediapb.GetProcessingRunRequest) (*mediapb.ProcessingRunResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetRunId(), "run_id")
	if err != nil {
		return nil, err
	}
	run, err := s.assets.GetProcessingRun(ctx, id, ns, req.GetOwnerRef())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &mediapb.ProcessingRunResponse{Run: runToProto(run)}, nil
}

func (s *Server) BatchGetProcessingRuns(ctx context.Context, req *mediapb.BatchGetProcessingRunsRequest) (*mediapb.BatchGetProcessingRunsResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	response := &mediapb.BatchGetProcessingRunsResponse{}
	for _, raw := range req.GetRunIds() {
		id, parseErr := parseID(raw, "run_id")
		if parseErr != nil {
			return nil, parseErr
		}
		run, getErr := s.assets.GetProcessingRun(ctx, id, ns, req.GetOwnerRef())
		if getErr == nil {
			response.Runs = append(response.Runs, runToProto(run))
		}
	}
	return response, nil
}

func (s *Server) RetryProcessing(ctx context.Context, req *mediapb.RetryProcessingRequest) (*mediapb.ProcessingRunResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetRunId(), "run_id")
	if err != nil {
		return nil, err
	}
	run, err := s.assets.RetryProcessing(ctx, id, ns, req.GetOwnerRef(), req.GetIdempotencyKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &mediapb.ProcessingRunResponse{Run: runToProto(run)}, nil
}

func (s *Server) CancelProcessing(ctx context.Context, req *mediapb.CancelProcessingRequest) (*mediapb.ProcessingRunResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetRunId(), "run_id")
	if err != nil {
		return nil, err
	}
	run, err := s.assets.CancelProcessing(ctx, id, ns, req.GetOwnerRef())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &mediapb.ProcessingRunResponse{Run: runToProto(run)}, nil
}

func (s *Server) GetDeliveryManifest(ctx context.Context, req *mediapb.GetDeliveryManifestRequest) (*mediapb.DeliveryManifestResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAssetId(), "asset_id")
	if err != nil {
		return nil, err
	}
	variants, err := s.assets.GetDeliveryManifest(ctx, id, req.GetGeneration(), ns, req.GetOwnerRef())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	manifest := &mediapb.DeliveryManifest{AssetId: id.String(), Generation: req.GetGeneration()}
	for _, v := range variants {
		manifest.Variants = append(manifest.Variants, variantToProto(v))
	}
	return &mediapb.DeliveryManifestResponse{Manifest: manifest}, nil
}

func (s *Server) ResolveDelivery(ctx context.Context, req *mediapb.ResolveDeliveryRequest) (*mediapb.ResolveDeliveryResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAssetId(), "asset_id")
	if err != nil {
		return nil, err
	}
	url, mime, err := s.assets.ResolveDelivery(ctx, id, req.GetGeneration(), req.GetVariantName(), ns, req.GetOwnerRef())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &mediapb.ResolveDeliveryResponse{Url: url, MimeType: mime}, nil
}

func (s *Server) ResolveSource(ctx context.Context, req *mediapb.ResolveSourceRequest) (*mediapb.ResolveDeliveryResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAssetId(), "asset_id")
	if err != nil {
		return nil, err
	}
	url, mime, err := s.assets.ResolveSource(ctx, id, ns, req.GetOwnerRef())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &mediapb.ResolveDeliveryResponse{Url: url, MimeType: mime}, nil
}

func (s *Server) ReleaseSource(ctx context.Context, req *mediapb.ReleaseSourceRequest) (*mediapb.ReleaseSourceResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAssetId(), "asset_id")
	if err != nil {
		return nil, err
	}
	if err := s.assets.ReleaseSource(ctx, id, req.GetGeneration(), ns, req.GetOwnerRef()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &mediapb.ReleaseSourceResponse{Released: true}, nil
}

func (s *Server) ListLifecycleEvents(ctx context.Context, req *mediapb.ListLifecycleEventsRequest) (*mediapb.ListLifecycleEventsResponse, error) {
	ns, err := s.namespace(ctx)
	if err != nil {
		return nil, err
	}
	events, err := s.assets.ListLifecycleEventsForNamespace(ctx, ns, req.GetAfterSequence(), int(req.GetLimit()))
	if err != nil {
		return nil, status.Error(codes.Internal, "list lifecycle events")
	}
	response := &mediapb.ListLifecycleEventsResponse{}
	for _, e := range events {
		item := &mediapb.LifecycleEvent{Sequence: e.Sequence, EventId: e.EventID, Type: e.Type, AssetId: e.AssetID.String(), Generation: e.Generation, OwnerRef: e.OwnerRef, FailureCode: e.FailureCode, CreatedAt: e.CreatedAt.Format(time.RFC3339)}
		if e.RunID != nil {
			item.RunId = e.RunID.String()
		}
		response.Events = append(response.Events, item)
	}
	return response, nil
}

func (s *Server) namespace(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "metadata is required")
	}
	token := strings.TrimSpace(strings.TrimPrefix(first(md, "authorization"), "Bearer "))
	if s.apiKey == "" || subtle.ConstantTimeCompare([]byte(token), []byte(s.apiKey)) != 1 {
		return "", status.Error(codes.PermissionDenied, "forbidden")
	}
	ns := ""
	if remote, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := remote.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
			ns = strings.TrimSpace(tlsInfo.State.PeerCertificates[0].Subject.CommonName)
			if ns == "" && len(tlsInfo.State.PeerCertificates[0].DNSNames) > 0 {
				ns = strings.TrimSpace(tlsInfo.State.PeerCertificates[0].DNSNames[0])
			}
		}
	}
	if ns == "" {
		ns = strings.TrimSpace(first(md, "x-onix-service"))
	}
	if ns == "" {
		ns = strings.TrimSpace(first(md, "x-service-name"))
	}
	if ns == "" || strings.Contains(ns, ":") {
		return "", status.Error(codes.InvalidArgument, "service namespace is required")
	}
	return ns, nil
}

func parseID(raw, field string) (uuid.UUID, error) {
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "invalid "+field)
	}
	return id, nil
}

func intMap(values map[int]string) map[int32]string {
	result := make(map[int32]string, len(values))
	for key, value := range values {
		result[int32(key)] = value
	}
	return result
}

func first(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func kindFromProto(kind mediapb.MediaKind) (domain.MediaKind, error) {
	switch kind {
	case mediapb.MediaKind_MEDIA_KIND_IMAGE:
		return domain.MediaKindImage, nil
	case mediapb.MediaKind_MEDIA_KIND_VIDEO:
		return domain.MediaKindVideo, nil
	case mediapb.MediaKind_MEDIA_KIND_AUDIO:
		return domain.MediaKindAudio, nil
	default:
		return "", status.Error(codes.InvalidArgument, "declared_kind is required")
	}
}
func pipelineFromProto(raw string) (domain.ProcessingProfile, error) {
	p := domain.ProcessingProfile(strings.TrimSpace(raw))
	switch p {
	case domain.PipelineImageResponsiveWebV1, domain.PipelineVideoWeb1080V1, domain.PipelineAudioWebV1:
		return p, nil
	default:
		return "", status.Error(codes.InvalidArgument, "unknown pipeline_id")
	}
}
func sourceToProto(a *domain.MediaAsset) *mediapb.AssetSource {
	if a == nil {
		return nil
	}
	return &mediapb.AssetSource{
		AssetId: a.ID.String(), OwnerRef: a.OwnerRef, Kind: kindToProto(a.DeclaredKind),
		Status: sourceStatusToProto(a.SourceStatus), MimeType: a.MimeType,
		SourcePolicyId: a.SourcePolicyID, FailureCode: a.SourceFailureCode,
		Width: int32(a.Width), Height: int32(a.Height), DurationMs: a.DurationMS,
	}
}
func runToProto(r *domain.ProcessingRun) *mediapb.ProcessingRun {
	if r == nil {
		return nil
	}
	return &mediapb.ProcessingRun{RunId: r.ID.String(), AssetId: r.AssetID.String(), PipelineId: string(r.PipelineID), PipelineVersion: r.PipelineVersion, Generation: r.Generation, Status: processingStatusToProto(r.Status), FailureCode: r.FailureCode}
}
func variantToProto(v domain.MediaAssetVariant) *mediapb.DeliveryVariant {
	return &mediapb.DeliveryVariant{Name: v.Name, MimeType: v.MimeType, Width: int32(v.Width), Height: int32(v.Height), DurationMs: v.DurationMS, Bitrate: v.Bitrate, Ready: true}
}
func kindToProto(k domain.MediaKind) mediapb.MediaKind {
	switch k {
	case domain.MediaKindVideo:
		return mediapb.MediaKind_MEDIA_KIND_VIDEO
	case domain.MediaKindAudio:
		return mediapb.MediaKind_MEDIA_KIND_AUDIO
	default:
		return mediapb.MediaKind_MEDIA_KIND_IMAGE
	}
}
func sourceStatusToProto(v domain.SourceStatus) mediapb.SourceStatus {
	switch v {
	case domain.SourceVerifying:
		return mediapb.SourceStatus_SOURCE_STATUS_VERIFYING
	case domain.SourceAvailable:
		return mediapb.SourceStatus_SOURCE_STATUS_AVAILABLE
	case domain.SourceRejected:
		return mediapb.SourceStatus_SOURCE_STATUS_REJECTED
	default:
		return mediapb.SourceStatus_SOURCE_STATUS_UPLOADING
	}
}
func processingStatusToProto(v domain.ProcessingStatus) mediapb.ProcessingStatus {
	switch v {
	case domain.ProcessingWaitingSource:
		return mediapb.ProcessingStatus_PROCESSING_STATUS_WAITING_SOURCE
	case domain.ProcessingQueued:
		return mediapb.ProcessingStatus_PROCESSING_STATUS_QUEUED
	case domain.ProcessingRunning:
		return mediapb.ProcessingStatus_PROCESSING_STATUS_PROCESSING
	case domain.ProcessingReady:
		return mediapb.ProcessingStatus_PROCESSING_STATUS_READY
	case domain.ProcessingFailed:
		return mediapb.ProcessingStatus_PROCESSING_STATUS_FAILED
	case domain.ProcessingCancelled:
		return mediapb.ProcessingStatus_PROCESSING_STATUS_CANCELLED
	default:
		return mediapb.ProcessingStatus_PROCESSING_STATUS_UNSPECIFIED
	}
}
