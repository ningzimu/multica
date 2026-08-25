package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/integrations/outwebhook"
)

type createOutboundWebhookRequest struct {
	Name        string   `json:"name"`
	Destination string   `json:"destination"`
	Events      []string `json:"events"`
	ScopeMode   string   `json:"scope_mode"`
	ProjectIDs  []string `json:"project_ids"`
}

type updateOutboundWebhookRequest struct {
	Name        string   `json:"name"`
	Destination *string  `json:"destination"`
	Events      []string `json:"events"`
	ScopeMode   string   `json:"scope_mode"`
	ProjectIDs  []string `json:"project_ids"`
}

type updateOutboundWebhookScopeRequest struct {
	ScopeMode  string   `json:"scope_mode"`
	ProjectIDs []string `json:"project_ids"`
}

func parseOutboundWebhookProjectIDs(w http.ResponseWriter, values []string) ([]pgtype.UUID, bool) {
	if values == nil {
		return nil, true
	}
	projectIDs := make([]pgtype.UUID, 0, len(values))
	for _, value := range values {
		projectID, ok := parseUUIDOrBadRequest(w, value, "project_id")
		if !ok {
			return nil, false
		}
		projectIDs = append(projectIDs, projectID)
	}
	return projectIDs, true
}

func (h *Handler) outboundWebhooksAvailable(w http.ResponseWriter) bool {
	if h.OutboundWebhooks == nil || !h.OutboundWebhooks.Available() {
		writeError(w, http.StatusServiceUnavailable, "outbound webhooks are not configured")
		return false
	}
	return true
}

func (h *Handler) ListOutboundWebhooks(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceMember(w, r, chi.URLParam(r, "id"), "workspace not found"); !ok {
		return
	}
	if h.OutboundWebhooks == nil || !h.OutboundWebhooks.Available() {
		writeJSON(w, http.StatusOK, map[string]any{"subscriptions": []outwebhook.Subscription{}, "capability_available": false})
		return
	}
	items, err := h.OutboundWebhooks.List(r.Context(), workspaceID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list outbound webhooks")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"subscriptions": items, "capability_available": true})
}

func (h *Handler) GetOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceMember(w, r, chi.URLParam(r, "id"), "workspace not found"); !ok {
		return
	}
	if !h.outboundWebhooksAvailable(w) {
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
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceRole(w, r, chi.URLParam(r, "id"), "workspace not found", "owner", "admin"); !ok {
		return
	}
	if !h.outboundWebhooksAvailable(w) {
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
	projectIDs, ok := parseOutboundWebhookProjectIDs(w, request.ProjectIDs)
	if !ok {
		return
	}
	result, err := h.OutboundWebhooks.Create(r.Context(), outwebhook.CreateInput{WorkspaceID: workspaceID, CreatedBy: createdBy, Name: request.Name, Destination: request.Destination, Events: request.Events, ScopeMode: request.ScopeMode, ProjectIDs: projectIDs})
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

func (h *Handler) UpdateOutboundWebhookScope(w http.ResponseWriter, r *http.Request) {
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
	var request updateOutboundWebhookScopeRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	projectIDs, ok := parseOutboundWebhookProjectIDs(w, request.ProjectIDs)
	if !ok {
		return
	}
	item, err := h.OutboundWebhooks.UpdateScope(r.Context(), outwebhook.UpdateScopeInput{
		WorkspaceID: workspaceID, SubscriptionID: subscriptionID, ScopeMode: request.ScopeMode, ProjectIDs: projectIDs,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if errors.Is(err, outwebhook.ErrInvalidInput) {
		writeError(w, http.StatusBadRequest, "invalid outbound webhook scope")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to update outbound webhook scope")
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (h *Handler) DeleteOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return
	}
	if _, ok := h.requireWorkspaceRole(w, r, chi.URLParam(r, "id"), "workspace not found", "owner", "admin"); !ok {
		return
	}
	if !h.outboundWebhooksAvailable(w) {
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

func (h *Handler) UpdateOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	var request updateOutboundWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	projectIDs, ok := parseOutboundWebhookProjectIDs(w, request.ProjectIDs)
	if !ok {
		return
	}
	item, err := h.OutboundWebhooks.Update(r.Context(), outwebhook.UpdateInput{
		WorkspaceID: workspaceID, ID: subscriptionID, Name: request.Name,
		Destination: request.Destination, Events: request.Events, ScopeMode: request.ScopeMode,
		ProjectIDs: projectIDs,
	})
	h.writeOutboundWebhookMutationResult(w, item, err, "update")
}

func (h *Handler) PauseOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	item, err := h.OutboundWebhooks.Pause(r.Context(), workspaceID, subscriptionID)
	h.writeOutboundWebhookMutationResult(w, item, err, "pause")
}

func (h *Handler) ResumeOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	item, err := h.OutboundWebhooks.Resume(r.Context(), workspaceID, subscriptionID)
	h.writeOutboundWebhookMutationResult(w, item, err, "resume")
}

func (h *Handler) TestOutboundWebhook(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	actorID, ok := parseUUIDOrBadRequest(w, userID, "user_id")
	if !ok {
		return
	}
	result, err := h.OutboundWebhooks.Test(r.Context(), outwebhook.TestInput{
		WorkspaceID: workspaceID, ID: subscriptionID, ActorID: actorID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if errors.Is(err, outwebhook.ErrInvalidInput) {
		writeError(w, http.StatusBadRequest, "outbound webhook test is unavailable")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to test outbound webhook")
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (h *Handler) RotateOutboundWebhookSecret(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	result, err := h.OutboundWebhooks.RotateSecret(r.Context(), workspaceID, subscriptionID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to rotate outbound webhook secret")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) ListOutboundWebhookDeliveries(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	limit, offset := int32(0), int32(0)
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid delivery history pagination")
			return
		}
		limit = int32(value)
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid delivery history pagination")
			return
		}
		offset = int32(value)
	}
	page, err := h.OutboundWebhooks.ListDeliveries(r.Context(), workspaceID, subscriptionID, limit, offset)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if errors.Is(err, outwebhook.ErrInvalidInput) {
		writeError(w, http.StatusBadRequest, "invalid delivery history pagination")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list webhook deliveries")
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (h *Handler) GetOutboundWebhookDelivery(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	deliveryID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "deliveryId"), "delivery_id")
	if !ok {
		return
	}
	delivery, err := h.OutboundWebhooks.GetDelivery(r.Context(), workspaceID, subscriptionID, deliveryID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "webhook delivery not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get webhook delivery")
		return
	}
	writeJSON(w, http.StatusOK, delivery)
}

func (h *Handler) RedeliverOutboundWebhookDelivery(w http.ResponseWriter, r *http.Request) {
	workspaceID, subscriptionID, ok := h.requireOutboundWebhookMutation(w, r)
	if !ok {
		return
	}
	deliveryID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "deliveryId"), "delivery_id")
	if !ok {
		return
	}
	delivery, err := h.OutboundWebhooks.Redeliver(r.Context(), workspaceID, subscriptionID, deliveryID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "webhook delivery not found")
		return
	}
	if errors.Is(err, outwebhook.ErrPaused) {
		writeError(w, http.StatusConflict, "paused webhook subscriptions cannot redeliver")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to redeliver webhook delivery")
		return
	}
	writeJSON(w, http.StatusAccepted, delivery)
}

func (h *Handler) requireOutboundWebhookMutation(w http.ResponseWriter, r *http.Request) (pgtype.UUID, pgtype.UUID, bool) {
	workspaceID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "id"), "workspace_id")
	if !ok {
		return pgtype.UUID{}, pgtype.UUID{}, false
	}
	if _, ok := h.requireWorkspaceRole(w, r, chi.URLParam(r, "id"), "workspace not found", "owner", "admin"); !ok {
		return pgtype.UUID{}, pgtype.UUID{}, false
	}
	if !h.outboundWebhooksAvailable(w) {
		return pgtype.UUID{}, pgtype.UUID{}, false
	}
	subscriptionID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "subscriptionId"), "subscription_id")
	if !ok {
		return pgtype.UUID{}, pgtype.UUID{}, false
	}
	return workspaceID, subscriptionID, true
}

func (h *Handler) writeOutboundWebhookMutationResult(w http.ResponseWriter, item outwebhook.Subscription, err error, action string) {
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "outbound webhook not found")
		return
	}
	if errors.Is(err, outwebhook.ErrInvalidInput) {
		writeError(w, http.StatusBadRequest, "invalid outbound webhook subscription")
		return
	}
	if err != nil {
		slog.Error(action+" outbound webhook failed", "error", err)
		writeError(w, http.StatusInternalServerError, "failed to "+action+" outbound webhook")
		return
	}
	writeJSON(w, http.StatusOK, item)
}
