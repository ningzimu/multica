package outwebhook

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
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

func TestCommentEnvelopeGoldenContracts(t *testing.T) {
	now := time.Date(2026, time.August, 25, 1, 2, 3, 0, time.UTC)
	service := &Service{now: func() time.Time { return now }}
	issueID := uuid.New()
	projectID := uuid.New()
	issue := db.Issue{
		ID:        pgtype.UUID{Bytes: issueID, Valid: true},
		Title:     "Parent issue",
		Status:    "in_progress",
		Priority:  "high",
		ProjectID: pgtype.UUID{Bytes: projectID, Valid: true},
	}
	commentID := uuid.NewString()
	parentID := uuid.NewString()
	longBody := strings.Repeat("界", 499) + "🙂終"

	tests := []struct {
		name      string
		eventType string
		payload   map[string]any
		assert    func(t *testing.T, envelope map[string]any)
	}{
		{
			name:      "reply created by an agent is unicode safely truncated",
			eventType: EventCommentCreated,
			payload: map[string]any{"comment": map[string]any{
				"id": commentID, "issue_id": issueID.String(), "parent_id": parentID,
				"author_type": "agent", "author_id": uuid.NewString(), "type": "comment",
				"content": longBody, "created_at": now.Format(time.RFC3339),
			}},
			assert: func(t *testing.T, envelope map[string]any) {
				comment := envelope["data"].(map[string]any)["comment"].(map[string]any)
				if comment["parent_id"] != parentID || comment["author"].(map[string]any)["type"] != "agent" {
					t.Fatalf("reply/author context missing: %#v", comment)
				}
				if got := []rune(comment["excerpt"].(string)); len(got) != maxCommentExcerptCodePoints || got[len(got)-1] != '🙂' || comment["truncated"] != true {
					t.Fatalf("excerpt was not code-point safe: len=%d tail=%q truncated=%v", len(got), string(got[len(got)-1]), comment["truncated"])
				}
			},
		},
		{
			name:      "body edit includes bounded content",
			eventType: EventCommentUpdated,
			payload: map[string]any{"body_changed": true, "comment": map[string]any{
				"id": commentID, "issue_id": issueID.String(), "parent_id": nil,
				"author_type": "member", "author_id": uuid.NewString(), "type": "comment",
				"content": "edited", "updated_at": now.Format(time.RFC3339),
			}},
			assert: func(t *testing.T, envelope map[string]any) {
				comment := envelope["data"].(map[string]any)["comment"].(map[string]any)
				if comment["excerpt"] != "edited" || comment["truncated"] != false {
					t.Fatalf("unexpected edit contract: %#v", comment)
				}
			},
		},
		{
			name:      "deletion omits deleted body",
			eventType: EventCommentDeleted,
			payload: map[string]any{
				"comment_id": commentID, "issue_id": issueID.String(), "parent_id": parentID,
				"author_type": "system", "author_id": "", "comment_type": "system",
			},
			assert: func(t *testing.T, envelope map[string]any) {
				comment := envelope["data"].(map[string]any)["comment"].(map[string]any)
				if _, ok := comment["excerpt"]; ok {
					t.Fatalf("deleted content leaked: %#v", comment)
				}
				if _, ok := comment["content"]; ok {
					t.Fatalf("deleted content leaked: %#v", comment)
				}
				if comment["deleted_at"] != now.Format(time.RFC3339) {
					t.Fatalf("deletion time missing: %#v", comment)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body, err := service.commentBody(events.Event{
				Type: commentProtocolType(test.eventType), WorkspaceID: uuid.NewString(), ActorType: "system", ActorID: "", Payload: test.payload,
			}, uuid.New(), issue, test.eventType)
			if err != nil {
				t.Fatal(err)
			}
			var envelope map[string]any
			if err := json.Unmarshal(body, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope["type"] != test.eventType {
				t.Fatalf("type = %v, want %s", envelope["type"], test.eventType)
			}
			issueData := envelope["data"].(map[string]any)["issue"].(map[string]any)
			if issueData["id"] != issueID.String() || issueData["project_id"] != projectID.String() {
				t.Fatalf("parent issue snapshot missing: %#v", issueData)
			}
			test.assert(t, envelope)
		})
	}
}

func commentProtocolType(eventType string) string {
	switch eventType {
	case EventCommentCreated:
		return "comment:created"
	case EventCommentUpdated:
		return "comment:updated"
	default:
		return "comment:deleted"
	}
}

func TestCommentUpdatedRequiresExplicitBodyChange(t *testing.T) {
	service := &Service{now: time.Now}
	issueID := uuid.New()
	issue := db.Issue{ID: pgtype.UUID{Bytes: issueID, Valid: true}, Title: "Parent", Status: "todo", Priority: "none"}
	for _, payload := range []map[string]any{
		{"comment": map[string]any{"id": uuid.NewString(), "issue_id": issueID.String(), "content": "unchanged"}},
		{"body_changed": false, "comment": map[string]any{"id": uuid.NewString(), "issue_id": issueID.String(), "content": "unchanged"}},
	} {
		if _, err := service.commentBody(events.Event{Payload: payload}, uuid.New(), issue, EventCommentUpdated); !errors.Is(err, errCommentBodyUnchanged) {
			t.Fatalf("attachment-only update error = %v, want errCommentBodyUnchanged", err)
		}
	}
}

func TestCommentCreatedAuthorAndParentVariants(t *testing.T) {
	service := &Service{now: time.Now}
	issueID := uuid.New()
	issue := db.Issue{
		ID: pgtype.UUID{Bytes: issueID, Valid: true}, Title: "Parent", Status: "todo", Priority: "none",
	}
	for _, authorType := range []string{"member", "agent", "system"} {
		for _, parentID := range []*string{nil, stringPointer(uuid.NewString())} {
			name := authorType + "/top-level"
			if parentID != nil {
				name = authorType + "/reply"
			}
			t.Run(name, func(t *testing.T) {
				authorID := uuid.NewString()
				if authorType == "system" {
					authorID = "00000000-0000-0000-0000-000000000000"
				}
				body, err := service.commentBody(events.Event{
					WorkspaceID: uuid.NewString(), ActorType: authorType, ActorID: authorID,
					Payload: map[string]any{"comment": map[string]any{
						"id": uuid.NewString(), "issue_id": issueID.String(), "parent_id": parentID,
						"author_type": authorType, "author_id": authorID, "type": "comment", "content": "visible excerpt",
					}},
				}, uuid.New(), issue, EventCommentCreated)
				if err != nil {
					t.Fatal(err)
				}
				var envelope struct {
					Data struct {
						Comment publicCommentSnapshot `json:"comment"`
					} `json:"data"`
				}
				if err := json.Unmarshal(body, &envelope); err != nil {
					t.Fatal(err)
				}
				if envelope.Data.Comment.Author.Type != authorType || !sameOptionalString(envelope.Data.Comment.ParentID, parentID) {
					t.Fatalf("author/parent contract mismatch: %#v", envelope.Data.Comment)
				}
			})
		}
	}
}

func TestCreateRejectsUnknownDuplicateOrEmptyEventSelections(t *testing.T) {
	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	service := New(nil, nil, box, nil)
	for _, events := range [][]string{
		nil,
		{"comment.created", "comment.created"},
		{"comment.created", "receiver.special"},
	} {
		_, err := service.Create(context.Background(), CreateInput{Name: "Receiver", Events: events})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("Create events=%v error=%v, want ErrInvalidInput", events, err)
		}
	}
}

func stringPointer(value string) *string { return &value }

func sameOptionalString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
