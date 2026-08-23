package push

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type Queries interface {
	GetNotificationPreference(context.Context, db.GetNotificationPreferenceParams) (db.NotificationPreference, error)
	GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error)
	ListActiveRecipientDevicesForPush(context.Context, db.ListActiveRecipientDevicesForPushParams) ([]db.RecipientDevice, error)
	InvalidateRecipientDevice(context.Context, pgtype.UUID) error
}

type Dispatcher struct {
	queries   Queries
	transport Transport
	cfg       Config
	logger    *slog.Logger
}

func NewDispatcher(queries Queries, transport Transport, cfg Config, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{queries: queries, transport: transport, cfg: cfg, logger: logger}
}

func (d *Dispatcher) Register(bus *events.Bus) {
	bus.Subscribe(protocol.EventInboxNew, func(event events.Event) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			d.Deliver(ctx, event)
		}()
	})
}

func (d *Dispatcher) Deliver(ctx context.Context, event events.Event) {
	item, ok := inboxItem(event.Payload)
	if !ok || stringValue(item["recipient_type"]) != "member" {
		return
	}
	recipientIDString := stringValue(item["recipient_id"])
	workspaceIDString := stringValue(item["workspace_id"])
	if recipientIDString == "" || workspaceIDString == "" || event.ActorID == recipientIDString {
		return
	}
	recipientID, err := util.ParseUUID(recipientIDString)
	if err != nil || !recipientID.Valid {
		return
	}
	workspaceID, err := util.ParseUUID(workspaceIDString)
	if err != nil || !workspaceID.Valid {
		return
	}
	if d.systemNotificationsMuted(ctx, workspaceID, recipientID) {
		return
	}
	devices, err := d.queries.ListActiveRecipientDevicesForPush(ctx, db.ListActiveRecipientDevicesForPushParams{
		UserID:          recipientID,
		BundleID:        d.cfg.Topic,
		PushEnvironment: d.cfg.Environment,
	})
	if err != nil {
		d.logger.WarnContext(ctx, "APNs device lookup failed", "error", err)
		return
	}
	if len(devices) == 0 {
		return
	}
	workspaceSlug := ""
	if workspace, err := d.queries.GetWorkspace(ctx, workspaceID); err == nil {
		workspaceSlug = workspace.Slug
	}
	payload := payloadForItem(item, workspaceIDString, workspaceSlug)
	for _, device := range devices {
		result, sendErr := d.transport.Send(ctx, device.DeviceToken, payload)
		if sendErr != nil {
			d.logger.WarnContext(ctx, "APNs delivery failed", "device_id", util.UUIDToString(device.ID), "error", sendErr)
			continue
		}
		if result.InvalidToken {
			if err := d.queries.InvalidateRecipientDevice(ctx, device.ID); err != nil {
				d.logger.WarnContext(ctx, "APNs device invalidation failed", "device_id", util.UUIDToString(device.ID), "error", err)
			}
			continue
		}
		if result.StatusCode < 200 || result.StatusCode >= 300 {
			d.logger.WarnContext(ctx, "APNs delivery rejected", "device_id", util.UUIDToString(device.ID), "status", result.StatusCode, "reason", result.Reason)
		}
	}
}

func (d *Dispatcher) systemNotificationsMuted(ctx context.Context, workspaceID, recipientID pgtype.UUID) bool {
	pref, err := d.queries.GetNotificationPreference(ctx, db.GetNotificationPreferenceParams{
		WorkspaceID: workspaceID,
		UserID:      recipientID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err != nil {
		d.logger.WarnContext(ctx, "APNs preference lookup failed", "error", err)
		return true
	}
	var preferences map[string]string
	if err := json.Unmarshal(pref.Preferences, &preferences); err != nil {
		return true
	}
	return preferences["system_notifications"] == "muted"
}

func inboxItem(payload any) (map[string]any, bool) {
	outer, ok := payload.(map[string]any)
	if !ok {
		return nil, false
	}
	item, ok := outer["item"].(map[string]any)
	return item, ok
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case *string:
		if typed != nil {
			return *typed
		}
	}
	return ""
}

func payloadForItem(item map[string]any, workspaceID, workspaceSlug string) Payload {
	var payload Payload
	payload.APS.Alert.Title = "Multica"
	payload.APS.Alert.Body = notificationBody(stringValue(item["type"]), stringValue(item["title"]))
	payload.APS.Sound = "default"
	payload.Multica = Destination{
		NotificationID: stringValue(item["id"]),
		WorkspaceID:    workspaceID,
		WorkspaceSlug:  workspaceSlug,
		IssueID:        stringValue(item["issue_id"]),
		CommentID:      commentID(item["details"]),
	}
	return payload
}

func notificationBody(notificationType, issueTitle string) string {
	label := map[string]string{
		"issue_assigned":   "Task assigned",
		"mentioned":        "You were mentioned",
		"new_comment":      "New comment",
		"status_changed":   "Status changed",
		"task_completed":   "Task completed",
		"task_failed":      "Task failed",
		"reaction_added":   "New reaction",
		"review_requested": "Review requested",
	}[notificationType]
	if label == "" {
		label = "New inbox activity"
	}
	issueTitle = strings.TrimSpace(issueTitle)
	if issueTitle == "" {
		return label
	}
	return truncateRunes(label+": "+issueTitle, 240)
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit-1]) + "…"
}

func commentID(details any) string {
	var raw []byte
	switch typed := details.(type) {
	case json.RawMessage:
		raw = typed
	case []byte:
		raw = typed
	case map[string]any:
		return stringValue(typed["comment_id"])
	default:
		return ""
	}
	var decoded map[string]any
	if json.Unmarshal(raw, &decoded) != nil {
		return ""
	}
	return stringValue(decoded["comment_id"])
}
