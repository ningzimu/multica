package push

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const (
	testRecipientID = "018f6f4f-2dd0-7f47-9f71-2af8d5a8a861"
	testWorkspaceID = "018f6f4f-2dd0-7f47-9f71-2af8d5a8a862"
	testDeviceID    = "018f6f4f-2dd0-7f47-9f71-2af8d5a8a863"
)

type fakeQueries struct {
	preference    []byte
	preferenceErr error
	devices       []db.RecipientDevice
	workspace     db.Workspace
	invalidated   []pgtype.UUID
	listParams    db.ListActiveRecipientDevicesForPushParams
}

func (f *fakeQueries) GetNotificationPreference(context.Context, db.GetNotificationPreferenceParams) (db.NotificationPreference, error) {
	return db.NotificationPreference{Preferences: f.preference}, f.preferenceErr
}

func (f *fakeQueries) GetWorkspace(context.Context, pgtype.UUID) (db.Workspace, error) {
	return f.workspace, nil
}

func (f *fakeQueries) ListActiveRecipientDevicesForPush(_ context.Context, params db.ListActiveRecipientDevicesForPushParams) ([]db.RecipientDevice, error) {
	f.listParams = params
	return f.devices, nil
}

func (f *fakeQueries) InvalidateRecipientDevice(_ context.Context, id pgtype.UUID) error {
	f.invalidated = append(f.invalidated, id)
	return nil
}

type recordedSend struct {
	token   string
	payload Payload
}

type fakeTransport struct {
	sends  []recordedSend
	result SendResult
	err    error
}

func (f *fakeTransport) Send(_ context.Context, token string, payload Payload) (SendResult, error) {
	f.sends = append(f.sends, recordedSend{token: token, payload: payload})
	return f.result, f.err
}

func mustUUID(t *testing.T, value string) pgtype.UUID {
	t.Helper()
	id, err := util.ParseUUID(value)
	if err != nil {
		t.Fatalf("parse UUID: %v", err)
	}
	return id
}

func inboxEvent(actorID string) events.Event {
	issueID := "018f6f4f-2dd0-7f47-9f71-2af8d5a8a864"
	return events.Event{
		ActorID: actorID,
		Payload: map[string]any{"item": map[string]any{
			"id":             "018f6f4f-2dd0-7f47-9f71-2af8d5a8a865",
			"workspace_id":   testWorkspaceID,
			"recipient_type": "member",
			"recipient_id":   testRecipientID,
			"type":           "new_comment",
			"issue_id":       &issueID,
			"title":          "Fix background notifications",
			"body":           "this complete comment must stay out of APNs",
			"details":        []byte(`{"comment_id":"018f6f4f-2dd0-7f47-9f71-2af8d5a8a866"}`),
		}},
	}
}

func TestDispatcherDeliversPrivacySafeInboxPayload(t *testing.T) {
	queries := &fakeQueries{
		preferenceErr: pgx.ErrNoRows,
		devices: []db.RecipientDevice{{
			ID:          mustUUID(t, testDeviceID),
			DeviceToken: "opaque-native-token",
		}},
		workspace: db.Workspace{Slug: "personal"},
	}
	transport := &fakeTransport{result: SendResult{StatusCode: 200}}
	dispatcher := NewDispatcher(queries, transport, Config{
		Topic: "vip.example.multica", Environment: "production",
	}, nil)

	dispatcher.Deliver(context.Background(), inboxEvent("another-user"))

	if len(transport.sends) != 1 {
		t.Fatalf("send count = %d, want 1", len(transport.sends))
	}
	sent := transport.sends[0]
	if sent.token != "opaque-native-token" {
		t.Fatalf("token = %q", sent.token)
	}
	if sent.payload.APS.Alert.Body != "New comment: Fix background notifications" {
		t.Fatalf("alert body = %q", sent.payload.APS.Alert.Body)
	}
	if sent.payload.Multica.WorkspaceSlug != "personal" || sent.payload.Multica.CommentID == "" {
		t.Fatalf("destination = %+v", sent.payload.Multica)
	}
	if queries.listParams.BundleID != "vip.example.multica" || queries.listParams.PushEnvironment != "production" {
		t.Fatalf("device routing params = %+v", queries.listParams)
	}
}

func TestDispatcherHonorsMuteAndSelfEvent(t *testing.T) {
	for _, test := range []struct {
		name       string
		preference []byte
		actorID    string
	}{
		{name: "muted", preference: []byte(`{"system_notifications":"muted"}`), actorID: "another-user"},
		{name: "self event", preference: []byte(`{}`), actorID: testRecipientID},
	} {
		t.Run(test.name, func(t *testing.T) {
			queries := &fakeQueries{preference: test.preference}
			transport := &fakeTransport{}
			NewDispatcher(queries, transport, Config{}, nil).
				Deliver(context.Background(), inboxEvent(test.actorID))
			if len(transport.sends) != 0 {
				t.Fatalf("send count = %d, want 0", len(transport.sends))
			}
		})
	}
}

func TestDispatcherInvalidatesPermanentTokenFailure(t *testing.T) {
	queries := &fakeQueries{
		preferenceErr: pgx.ErrNoRows,
		devices: []db.RecipientDevice{{
			ID:          mustUUID(t, testDeviceID),
			DeviceToken: "invalid-token",
		}},
	}
	transport := &fakeTransport{result: SendResult{
		StatusCode: 410, Reason: "Unregistered", InvalidToken: true,
	}}

	NewDispatcher(queries, transport, Config{}, nil).
		Deliver(context.Background(), inboxEvent("another-user"))

	if len(queries.invalidated) != 1 || queries.invalidated[0] != mustUUID(t, testDeviceID) {
		t.Fatalf("invalidated = %+v", queries.invalidated)
	}
}

func TestDispatcherFailsClosedOnPreferenceLookupError(t *testing.T) {
	queries := &fakeQueries{preferenceErr: errors.New("database unavailable")}
	transport := &fakeTransport{}
	NewDispatcher(queries, transport, Config{}, nil).
		Deliver(context.Background(), inboxEvent("another-user"))
	if len(transport.sends) != 0 {
		t.Fatalf("send count = %d, want 0", len(transport.sends))
	}
}

func TestNotificationBodyIsBoundedForAPNs(t *testing.T) {
	body := notificationBody("new_comment", strings.Repeat("通知", 300))
	if len([]rune(body)) != 240 || !strings.HasSuffix(body, "…") {
		t.Fatalf("bounded body length=%d suffix=%q", len([]rune(body)), body[len(body)-3:])
	}
}
