package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const recipientDeviceBodyLimit = 8 * 1024

type registerRecipientDeviceRequest struct {
	Platform        string `json:"platform"`
	BundleID        string `json:"bundle_id"`
	PushEnvironment string `json:"push_environment"`
	DeviceToken     string `json:"device_token"`
}

type recipientDeviceResponse struct {
	ID              string  `json:"id"`
	InstallationID  string  `json:"installation_id"`
	UserID          string  `json:"user_id"`
	Platform        string  `json:"platform"`
	BundleID        string  `json:"bundle_id"`
	PushEnvironment string  `json:"push_environment"`
	Enabled         bool    `json:"enabled"`
	BoundAt         string  `json:"bound_at"`
	RevokedAt       *string `json:"revoked_at"`
	InvalidatedAt   *string `json:"invalidated_at"`
	LastSeenAt      string  `json:"last_seen_at"`
	CreatedAt       string  `json:"created_at"`
	UpdatedAt       string  `json:"updated_at"`
}

func newRecipientDeviceResponse(device db.RecipientDevice) recipientDeviceResponse {
	return recipientDeviceResponse{
		ID:              uuidToString(device.ID),
		InstallationID:  uuidToString(device.InstallationID),
		UserID:          uuidToString(device.UserID),
		Platform:        device.Platform,
		BundleID:        device.BundleID,
		PushEnvironment: device.PushEnvironment,
		Enabled:         device.Enabled,
		BoundAt:         timestampToString(device.BoundAt),
		RevokedAt:       timestampToPtr(device.RevokedAt),
		InvalidatedAt:   timestampToPtr(device.InvalidatedAt),
		LastSeenAt:      timestampToString(device.LastSeenAt),
		CreatedAt:       timestampToString(device.CreatedAt),
		UpdatedAt:       timestampToString(device.UpdatedAt),
	}
}

func decodeRecipientDeviceRegistration(
	w http.ResponseWriter,
	r *http.Request,
) (registerRecipientDeviceRequest, bool) {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, recipientDeviceBodyLimit))
	decoder.DisallowUnknownFields()

	var req registerRecipientDeviceRequest
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return registerRecipientDeviceRequest{}, false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return registerRecipientDeviceRequest{}, false
	}

	req.Platform = strings.ToLower(strings.TrimSpace(req.Platform))
	req.BundleID = strings.TrimSpace(req.BundleID)
	req.PushEnvironment = strings.ToLower(strings.TrimSpace(req.PushEnvironment))
	req.DeviceToken = strings.TrimSpace(req.DeviceToken)

	if req.Platform != "ios" {
		writeError(w, http.StatusBadRequest, "platform must be ios")
		return registerRecipientDeviceRequest{}, false
	}
	if req.BundleID == "" || len(req.BundleID) > 255 || strings.ContainsAny(req.BundleID, " \t\r\n") {
		writeError(w, http.StatusBadRequest, "invalid bundle_id")
		return registerRecipientDeviceRequest{}, false
	}
	if req.PushEnvironment != "sandbox" && req.PushEnvironment != "production" {
		writeError(w, http.StatusBadRequest, "push_environment must be sandbox or production")
		return registerRecipientDeviceRequest{}, false
	}
	if req.DeviceToken == "" || len(req.DeviceToken) > 4096 {
		writeError(w, http.StatusBadRequest, "invalid device_token")
		return registerRecipientDeviceRequest{}, false
	}

	return req, true
}

// RegisterRecipientDevice creates or refreshes the current user's binding for
// one app installation. The native token is accepted as an opaque secret and
// intentionally omitted from the response and logs.
func (h *Handler) RegisterRecipientDevice(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	userUUID, ok := parseUUIDOrBadRequest(w, userID, "user id")
	if !ok {
		return
	}
	installationID, ok := parseUUIDOrBadRequest(
		w,
		chi.URLParam(r, "installationId"),
		"installation id",
	)
	if !ok {
		return
	}
	req, ok := decodeRecipientDeviceRegistration(w, r)
	if !ok {
		return
	}

	tx, err := h.TxStarter.Begin(r.Context())
	if err != nil {
		slog.Error("failed to begin recipient device registration")
		writeError(w, http.StatusInternalServerError, "failed to register recipient device")
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	queries := h.Queries.WithTx(tx)
	lockParams := db.LockRecipientDeviceTokenParams{
		BundleID:        req.BundleID,
		PushEnvironment: req.PushEnvironment,
		DeviceToken:     req.DeviceToken,
	}
	if err := queries.LockRecipientDeviceToken(r.Context(), lockParams); err != nil {
		slog.Error("failed to lock recipient device registration")
		writeError(w, http.StatusInternalServerError, "failed to register recipient device")
		return
	}
	if err := queries.DisplaceRecipientDeviceToken(r.Context(), db.DisplaceRecipientDeviceTokenParams{
		DeviceToken:     req.DeviceToken,
		BundleID:        req.BundleID,
		PushEnvironment: req.PushEnvironment,
		InstallationID:  installationID,
	}); err != nil {
		slog.Error("failed to displace recipient device registration")
		writeError(w, http.StatusInternalServerError, "failed to register recipient device")
		return
	}
	device, err := queries.UpsertRecipientDevice(r.Context(), db.UpsertRecipientDeviceParams{
		InstallationID:  installationID,
		UserID:          userUUID,
		Platform:        req.Platform,
		BundleID:        req.BundleID,
		PushEnvironment: req.PushEnvironment,
		DeviceToken:     req.DeviceToken,
	})
	if err != nil {
		slog.Error("failed to register recipient device")
		writeError(w, http.StatusInternalServerError, "failed to register recipient device")
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit recipient device registration")
		writeError(w, http.StatusInternalServerError, "failed to register recipient device")
		return
	}

	writeJSON(w, http.StatusOK, newRecipientDeviceResponse(device))
}

// RevokeRecipientDevice disables only a binding currently owned by the
// authenticated user. A previous account cannot revoke an installation after
// it has been rebound to somebody else.
func (h *Handler) RevokeRecipientDevice(w http.ResponseWriter, r *http.Request) {
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	userUUID, ok := parseUUIDOrBadRequest(w, userID, "user id")
	if !ok {
		return
	}
	installationID, ok := parseUUIDOrBadRequest(
		w,
		chi.URLParam(r, "installationId"),
		"installation id",
	)
	if !ok {
		return
	}

	device, err := h.Queries.RevokeRecipientDevice(r.Context(), db.RevokeRecipientDeviceParams{
		InstallationID: installationID,
		UserID:         userUUID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "recipient device not found")
			return
		}
		slog.Error("failed to revoke recipient device", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to revoke recipient device")
		return
	}

	writeJSON(w, http.StatusOK, newRecipientDeviceResponse(device))
}
