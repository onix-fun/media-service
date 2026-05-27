package api

import (
	"encoding/json"
	"net/http"

	"media-service/internal/domain"
	"media-service/internal/upload"

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
func NewRouter(h *Handlers) *chi.Mux {
	r := chi.NewRouter()

	r.Route("/uploads", func(r chi.Router) {
		r.Post("/init", h.InitUpload)
		r.Post("/{sessionId}/complete", h.CompleteUpload)
		r.Get("/{sessionId}", h.GetUploadSession)
	})

	r.Route("/blobs", func(r chi.Router) {
		r.Get("/{blobId}/download-url", h.GetDownloadURL)
		r.Delete("/{blobId}", h.DeleteBlob)
	})

	return r
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

	// In a real system, the caller identity is injected by API Gateway via headers (e.g., X-Service-Name)
	callerService := r.Header.Get("X-Service-Name")
	if callerService == "" {
		callerService = "unknown-service" // default for prototype
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

	if err := h.uploadService.CompleteUpload(r.Context(), sessionID, parts); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"message": "Upload completed, async hashing started"}`))
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

	url, err := h.uploadService.GetDownloadURL(r.Context(), blobID)
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

	session, err := h.uploadService.GetSession(r.Context(), sessionID)
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

	if err := h.uploadService.DeleteBlob(r.Context(), blobID); err != nil {
		if err.Error() == "blob not found" {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
