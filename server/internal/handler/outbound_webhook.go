package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/integrations/outwebhook"
)

type createOutboundWebhookRequest struct {
	Name        string   `json:"name"`
	Destination string   `json:"destination"`
	Events      []string `json:"events"`
	ScopeMode   string   `json:"scope_mode"`
}

type updateOutboundWebhookEventsRequest struct {
	Events []string `json:"events"`
}

func (h *Handler) outboundWebhooksAvailable(w http.ResponseWriter) bool {
	if h.OutboundWebhooks == nil || !h.OutboundWebhooks.Available() {
		writeError(w, http.StatusServiceUnavailable, "outbound webhooks are not configured")
		return false
	}
	return true
}

func (h *Handler) ListOutboundWebhooks(w http.ResponseWriter, r *http.Request) {
	if !h.outboundWebhooksAvailable(w) {
		return
	}
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceMember(w, r, chi.URLParam(r, "id"), "workspace not found"); !ok {
		return
	}
	items, err := h.OutboundWebhooks.List(r.Context(), workspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list outbound webhooks")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": items})
}

func (h *Handler) GetOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	if !h.outboundWebhooksAvailable(w) {
		return
	}
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceMember(w, r, chi.URLParam(r, "id"), "workspace not found"); !ok {
		return
	}
	subscriptionID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "subscriptionId"), "subscription_id")
	if !ok {
		return
	}
	item, err := h.OutboundWebhooks.Get(r.Context(), workspaceID, subscriptionID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get outbound webhook")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) CreateOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	if !h.outboundWebhooksAvailable(w) {
		return
	}
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceRole(w, r, chi.URLParam(r, "id"), "workspace not found", "owner", "admin"); !ok {
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	createdBy, ok := parseUUIDOrBadRequest(w, userID, "user_id")
	if !ok {
		return
	}
	var request createOutboundWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if request.ScopeMode != outwebhook.ScopeWorkspace {
		writeError(w, http.StatusBadRequest, "scope_mode must be workspace")
		return
	}
	result, err := h.OutboundWebhooks.Create(r.Context(), outwebhook.CreateInput{WorkspaceID: workspaceID, CreatedBy: createdBy, Name: request.Name, Destination: request.Destination, Events: request.Events})
	if errors.Is(err, outwebhook.ErrInvalidInput) {
		writeError(w, http.StatusBadRequest, "invalid outbound webhook subscription")
		return
	}
	if errors.Is(err, outwebhook.ErrUnavailable) {
		writeError(w, http.StatusServiceUnavailable, "outbound webhooks are not configured")
		return
	}
	if err != nil {
		slog.Error("create outbound webhook failed", "workspace_id", chi.URLParam(r, "id"), "error", err)
		writeError(w, http.StatusInternalServerError, "failed to create outbound webhook")
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (h *Handler) UpdateOutboundWebhookEvents(w http.ResponseWriter, r *http.Request) {
	if !h.outboundWebhooksAvailable(w) {
		return
	}
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceRole(w, r, chi.URLParam(r, "id"), "workspace not found", "owner", "admin"); !ok {
		return
	}
	subscriptionID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "subscriptionId"), "subscription_id")
	if !ok {
		return
	}
	var request updateOutboundWebhookEventsRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	item, err := h.OutboundWebhooks.UpdateEvents(r.Context(), outwebhook.UpdateEventsInput{
		WorkspaceID: workspaceID, SubscriptionID: subscriptionID, Events: request.Events,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if errors.Is(err, outwebhook.ErrInvalidInput) {
		writeError(w, http.StatusBadRequest, "invalid outbound webhook event selection")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update outbound webhook")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) DeleteOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	if !h.outboundWebhooksAvailable(w) {
		return
	}
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceRole(w, r, chi.URLParam(r, "id"), "workspace not found", "owner", "admin"); !ok {
		return
	}
	subscriptionID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "subscriptionId"), "subscription_id")
	if !ok {
		return
	}
	deleted, err := h.OutboundWebhooks.Delete(r.Context(), workspaceID, subscriptionID)
	if errors.Is(err, pgx.ErrNoRows) || !deleted {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete outbound webhook")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
