package api

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/onix-fun/media-service/internal/domain"
	"github.com/onix-fun/media-service/internal/upload"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// InitUploadRequest represents the payload for starting an upload
type InitUploadRequest struct {
	MimeType     string `json:"mimeType"`
	ExpectedSize *int64 `json:"expectedSize,omitempty"`
	PartsCount   int    `json:"partsCount"`
}

// InitUploadResponse represents the response containing presigned URLs
type InitUploadResponse struct {
	SessionID uuid.UUID      `json:"sessionId"`
	Parts     map[int]string `json:"parts"` // map of PartNumber to Presigned URL
	ExpiresAt string         `json:"expiresAt"`
}

// UploadSessionResponse represents the polling response
type UploadSessionResponse struct {
	SessionID uuid.UUID            `json:"sessionId"`
	Status    domain.SessionStatus `json:"status"`
	BlobID    *uuid.UUID           `json:"blobId,omitempty"`
}

// CompleteUploadRequest represents the payload from client to finalize upload
type CompleteUploadRequest struct {
	Parts []UploadPartReq `json:"parts"`
}

type UploadPartReq struct {
	PartNumber int    `json:"partNumber"`
	ETag       string `json:"etag"`
}

// Handlers struct holds dependencies
type Handlers struct {
	uploadService upload.Service
}

// NewRouter sets up the chi router and routes
func NewRouter(h *Handlers, internalAuthSecret string) *chi.Mux {
	r := chi.NewRouter()
	r.Use(requireInternalAuth(internalAuthSecret))

	r.Route("/uploads", func(r chi.Router) {
		r.Post("/init", h.InitUpload)
		r.Post("/{sessionId}/complete", h.CompleteUpload)
		r.Post("/{sessionId}/cancel", h.CancelUpload)
		r.Get("/{sessionId}", h.GetUploadSession)
	})

	r.Route("/blobs", func(r chi.Router) {
		r.Get("/{blobId}/download-url", h.GetDownloadURL)
		r.Delete("/{blobId}", h.DeleteBlob)
		r.Post("/{blobId}/references", h.CreateReference)
		r.Delete("/{blobId}/references/{referenceType}/{referenceId}", h.DeleteReference)
		r.Post("/{blobId}/processing/{profile}", h.StartProcessing)
	})

	return r
}

// StartProcessing godoc
// @Summary Manually triggers a processing profile for a blob
// @Description Queues a processing task (e.g., thumbnail generation) for an existing blob.
// @Tags blobs
// @Param blobId path string true "Blob ID"
// @Param profile path string true "Profile Name (e.g., thumbnail, preview)"
// @Success 202 "Accepted"
// @Security ApiKeyAuth
// @Router /blobs/{blobId}/processing/{profile} [post]
func (h *Handlers) StartProcessing(w http.ResponseWriter, r *http.Request) {
	blobID, err := uuid.Parse(chi.URLParam(r, "blobId"))
	if err != nil {
		http.Error(w, "invalid blob ID", 400)
		return
	}
	if err := h.uploadService.StartProcessing(r.Context(), blobID, callerIdentity(r), chi.URLParam(r, "profile")); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

type ReferenceRequest struct {
	ReferenceType string `json:"reference_type"`
	ReferenceID   string `json:"reference_id"`
}

// CreateReference godoc
// @Summary Links a blob to an external entity
// @Description Registers a reference (e.g., "post:123") to prevent the blob from being garbage collected.
// @Tags blobs
// @Accept json
// @Param blobId path string true "Blob ID"
// @Param request body ReferenceRequest true "Reference Details"
// @Success 204 "Created"
// @Security ApiKeyAuth
// @Router /blobs/{blobId}/references [post]
func (h *Handlers) CreateReference(w http.ResponseWriter, r *http.Request) {
	blobID, err := uuid.Parse(chi.URLParam(r, "blobId"))
	if err != nil {
		http.Error(w, "invalid blob ID", 400)
		return
	}
	var req ReferenceRequest
	if json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "invalid JSON", 400)
		return
	}
	if err := h.uploadService.CreateReference(r.Context(), blobID, callerIdentity(r), req.ReferenceType, req.ReferenceID); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DeleteReference godoc
// @Summary Removes a link between a blob and an external entity
// @Description Unregisters a reference. If no references remain, the blob may be eventually garbage collected.
// @Tags blobs
// @Param blobId path string true "Blob ID"
// @Param referenceType path string true "Reference Type"
// @Param referenceId path string true "Reference ID"
// @Success 204 "Deleted"
// @Security ApiKeyAuth
// @Router /blobs/{blobId}/references/{referenceType}/{referenceId} [delete]
func (h *Handlers) DeleteReference(w http.ResponseWriter, r *http.Request) {
	blobID, err := uuid.Parse(chi.URLParam(r, "blobId"))
	if err != nil {
		http.Error(w, "invalid blob ID", 400)
		return
	}
	if err := h.uploadService.DeleteReference(r.Context(), blobID, callerIdentity(r), chi.URLParam(r, "referenceType"), chi.URLParam(r, "referenceId")); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func requireInternalAuth(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if secret == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(secret)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// NewHandlers creates a new Handlers instance
func NewHandlers(uploadService upload.Service) *Handlers {
	return &Handlers{
		uploadService: uploadService,
	}
}

// InitUpload godoc
// @Summary Initializes a multipart upload
// @Description Creates an S3 multipart upload session and returns presigned URLs for direct client-to-S3 upload
// @Tags uploads
// @Accept json
// @Produce json
// @Param request body InitUploadRequest true "Upload Details"
// @Success 200 {object} InitUploadResponse
// @Router /uploads/init [post]
func (h *Handlers) InitUpload(w http.ResponseWriter, r *http.Request) {
	var req InitUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.PartsCount <= 0 {
		http.Error(w, "partsCount must be greater than 0", http.StatusBadRequest)
		return
	}
	if req.PartsCount > 10_000 {
		http.Error(w, "partsCount exceeds limit", http.StatusBadRequest)
		return
	}
	if req.ExpectedSize != nil && (*req.ExpectedSize <= 0 || *req.ExpectedSize > 10*1024*1024*1024) {
		http.Error(w, "expectedSize must be between 1 byte and 10 GiB", http.StatusBadRequest)
		return
	}
	if req.MimeType == "" {
		http.Error(w, "mimeType is required", http.StatusBadRequest)
		return
	}

	// In a real system, the caller identity is injected by API Gateway via headers (e.g., X-Service-Name)
	callerService := callerIdentity(r)
	if callerService == "" {
		http.Error(w, "X-Service-Name is required", http.StatusBadRequest)
		return
	}

	session, urls, err := h.uploadService.InitUpload(r.Context(), req.MimeType, req.ExpectedSize, req.PartsCount, callerService)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := InitUploadResponse{
		SessionID: session.ID,
		Parts:     urls,
		ExpiresAt: session.ExpiresAt.Format(http.TimeFormat),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// CompleteUpload godoc
// @Summary Completes a multipart upload
// @Description Validates parts, completes the S3 multipart upload, and triggers the async hashing pipeline
// @Tags uploads
// @Accept json
// @Produce json
// @Param sessionId path string true "Upload Session ID"
// @Param request body CompleteUploadRequest true "Uploaded Parts with ETags"
// @Success 202 "Accepted - Processing started"
// @Router /uploads/{sessionId}/complete [post]
func (h *Handlers) CompleteUpload(w http.ResponseWriter, r *http.Request) {
	sessionIDStr := chi.URLParam(r, "sessionId")
	sessionID, err := uuid.Parse(sessionIDStr)
	if err != nil {
		http.Error(w, "invalid session ID", http.StatusBadRequest)
		return
	}

	var req CompleteUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	parts := make([]domain.UploadPart, len(req.Parts))
	for i, p := range req.Parts {
		parts[i] = domain.UploadPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		}
	}

	if err := h.uploadService.CompleteUpload(r.Context(), sessionID, parts, callerIdentity(r)); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message": "Upload completed, async hashing started"}`))
}

// CancelUpload godoc
// @Summary Cancels an upload session
// @Description Aborts the S3 multipart upload and marks the session as ABANDONED
// @Tags uploads
// @Param sessionId path string true "Upload Session ID"
// @Success 200 "Cancelled"
// @Router /uploads/{sessionId}/cancel [post]
func (h *Handlers) CancelUpload(w http.ResponseWriter, r *http.Request) {
	sessionIDStr := chi.URLParam(r, "sessionId")
	sessionID, err := uuid.Parse(sessionIDStr)
	if err != nil {
		http.Error(w, "invalid session ID", http.StatusBadRequest)
		return
	}

	if err := h.uploadService.CancelUpload(r.Context(), sessionID, callerIdentity(r)); err != nil {
		if err.Error() == "session not found" {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"message": "Upload cancelled"}`))
}

// GetDownloadURL godoc
// @Summary Gets a presigned download URL
// @Description Returns a direct S3 presigned URL for downloading the blob (Control Plane approach)
// @Tags blobs
// @Produce json
// @Param blobId path string true "Blob ID"
// @Success 200 {object} map[string]string "{"url": "https://..."}"
// @Router /blobs/{blobId}/download-url [get]
func (h *Handlers) GetDownloadURL(w http.ResponseWriter, r *http.Request) {
	blobIDStr := chi.URLParam(r, "blobId")
	blobID, err := uuid.Parse(blobIDStr)
	if err != nil {
		http.Error(w, "invalid blob ID", http.StatusBadRequest)
		return
	}

	url, err := h.uploadService.GetDownloadURL(r.Context(), blobID, callerIdentity(r))
	if err != nil {
		// Distinguish between not found and internal errors in a real app
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := map[string]string{"url": url}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// GetUploadSession godoc
// @Summary Gets the status of an upload session
// @Description Returns the current status of the upload session and the resulting blobId if completed.
// @Tags uploads
// @Produce json
// @Param sessionId path string true "Upload Session ID"
// @Success 200 {object} UploadSessionResponse
// @Router /uploads/{sessionId} [get]
func (h *Handlers) GetUploadSession(w http.ResponseWriter, r *http.Request) {
	sessionIDStr := chi.URLParam(r, "sessionId")
	sessionID, err := uuid.Parse(sessionIDStr)
	if err != nil {
		http.Error(w, "invalid session ID", http.StatusBadRequest)
		return
	}

	// For simplicity in this handler, we will access the metadata repo through a new method on UploadService
	// Wait, the handler only has UploadService. We should add GetSession to UploadService.
	// But since this is a quick implementation, let's assume we have it. I'll need to add it to upload.Service.
	// For now, I'll return a placeholder.

	session, err := h.uploadService.GetSession(r.Context(), sessionID, callerIdentity(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if session == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	res := UploadSessionResponse{
		SessionID: session.ID,
		Status:    session.Status,
		BlobID:    session.BlobID,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

// DeleteBlob godoc
// @Summary Force deletes a blob
// @Description Synchronously deletes a blob from S3 and its metadata from the database. Typically used for administrative actions, DMCA, or GDPR purges.
// @Tags blobs
// @Param blobId path string true "Blob ID"
// @Success 204 "Accepted / Deleted"
// @Router /blobs/{blobId} [delete]
func (h *Handlers) DeleteBlob(w http.ResponseWriter, r *http.Request) {
	blobIDStr := chi.URLParam(r, "blobId")
	blobID, err := uuid.Parse(blobIDStr)
	if err != nil {
		http.Error(w, "invalid blob ID", http.StatusBadRequest)
		return
	}

	if err := h.uploadService.DeleteBlob(r.Context(), blobID, callerIdentity(r)); err != nil {
		if err.Error() == "blob not found" {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func callerIdentity(r *http.Request) string {
	service := strings.TrimSpace(r.Header.Get("X-Service-Name"))
	user := strings.TrimSpace(r.Header.Get("X-User-ID"))
	if service == "" {
		return ""
	}
	if user == "" {
		return service
	}
	return service + ":" + user
}
