package outwebhook

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
)

func TestIssueEventCatalogRequiresAnExplicitKnownSelection(t *testing.T) {
	if err := validateEventSelection([]string{EventIssueCreated, EventIssuePriorityChanged}); err != nil {
		t.Fatalf("known explicit selection rejected: %v", err)
	}
	for _, selected := range [][]string{
		nil,
		{EventIssueCreated, EventIssueCreated},
		{EventIssueCreated, "issue.updated"},
	} {
		if err := validateEventSelection(selected); err == nil {
			t.Fatalf("invalid selection accepted: %#v", selected)
		}
	}
}

func TestIssueUpdateDerivesIndependentProductEventsOrFailsClosed(t *testing.T) {
	payload := map[string]any{
		"status_changed":   true,
		"priority_changed": true,
		"assignee_changed": false,
		"project_changed":  false,
		"prev_status":      "todo",
		"prev_priority":    "low",
	}
	events, err := issueProductEvents(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].eventType != EventIssueStatusChanged || events[1].eventType != EventIssuePriorityChanged {
		t.Fatalf("derived events = %#v", events)
	}
	delete(payload, "prev_priority")
	if _, err := issueProductEvents(payload); err == nil {
		t.Fatal("incomplete multi-field change produced a partial event set")
	}

	events, err = issueProductEvents(map[string]any{"action": "edited"})
	if err != nil || len(events) != 0 {
		t.Fatalf("status catalog edit was misclassified as an Issue status change: events=%#v err=%v", events, err)
	}
}

func TestIssueChangeEnvelopePreservesTypedPreviousAndNullCurrent(t *testing.T) {
	service := &Service{now: func() time.Time { return time.Unix(10, 0) }}
	event := events.Event{
		WorkspaceID: uuid.NewString(),
		ActorType:   "member",
		ActorID:     uuid.NewString(),
		Payload: map[string]any{
			"issue": map[string]any{
				"id": uuid.NewString(), "identifier": "MUL-7", "title": "Move issue",
				"status": "in_progress", "priority": "high", "project_id": nil,
				"assignee_type": nil, "assignee_id": nil,
			},
			"prev_project_id": uuid.NewString(),
		},
	}
	body, err := service.issueEventBody(context.Background(), event, uuid.New(), productEvent{eventType: EventIssueProjectChanged, changeField: "project"})
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Type string `json:"type"`
		Data struct {
			Change struct {
				Field    string  `json:"field"`
				Previous *string `json:"previous"`
				Current  *string `json:"current"`
			} `json:"change"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Type != EventIssueProjectChanged || envelope.Data.Change.Field != "project" || envelope.Data.Change.Previous == nil || envelope.Data.Change.Current != nil {
		t.Fatalf("typed project transition lost: %s", body)
	}
}

func TestDestinationPolicyRejectsUnsafeNetworkPaths(t *testing.T) {
	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	service := New(nil, nil, box, nil)
	for _, raw := range []string{
		"http://example.com/events",
		"https://localhost/events",
		"https://127.0.0.1/events",
		"https://[::1]/events",
		"https://169.254.169.254/events",
		"https://example.com:8443/events",
		"https://user:secret@example.com/events",
		"https://example.com/events?token=secret",
	} {
		if _, _, err := service.validateDestination(context.Background(), raw); err == nil {
			t.Fatalf("unsafe destination %q was accepted", raw)
		}
	}
}

func TestImmediateDispatcherHasBoundedLifecycle(t *testing.T) {
	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	service := New(nil, nil, box, nil)
	service.Register(events.New())
	if cap(service.dispatchQueue) != dispatchQueueSize {
		t.Fatalf("dispatch queue capacity = %d, want %d", cap(service.dispatchQueue), dispatchQueueSize)
	}
	service.Close()
	if !service.WaitWithTimeout(time.Second) {
		t.Fatal("bounded immediate dispatcher did not stop")
	}
}

func TestOperatorAllowlistIsExact(t *testing.T) {
	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	service := New(nil, nil, box, []string{"https://127.0.0.1:9443"})
	if _, allowed, err := service.validateDestination(context.Background(), "https://127.0.0.1:9443/events"); err != nil || !allowed {
		t.Fatalf("exact operator origin rejected: allowed=%v err=%v", allowed, err)
	}
	if _, _, err := service.validateDestination(context.Background(), "https://127.0.0.1:9444/events"); err == nil {
		t.Fatal("allowlist widened to another port")
	}
}
