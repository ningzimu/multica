package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/analytics"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/outwebhook"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type receivedOutboundWebhook struct {
	body              []byte
	header            http.Header
	persistedBody     []byte
	persistedState    string
	persistedForEvent int
	persistenceError  error
}

func TestIssueCreationPersistsAndDeliversSecureWorkspaceWebhook(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan receivedOutboundWebhook, 1)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed := receivedOutboundWebhook{body: body, header: r.Header.Clone()}
		observed.persistenceError = testPool.QueryRow(context.Background(),
			`SELECT state, request_body FROM outbound_webhook_delivery WHERE id = $1`,
			r.Header.Get("X-Multica-Delivery-ID"),
		).Scan(&observed.persistedState, &observed.persistedBody)
		if observed.persistenceError == nil {
			observed.persistenceError = testPool.QueryRow(context.Background(),
				`SELECT count(*) FROM outbound_webhook_delivery WHERE event_id = $1`,
				r.Header.Get("X-Multica-Event-ID"),
			).Scan(&observed.persistedForEvent)
		}
		received <- observed
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	queries := db.New(testPool)
	bus := events.New()
	outbound := outwebhook.New(queries, testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()))
	outbound.Register(bus)
	t.Cleanup(func() {
		outbound.Close()
		if !outbound.WaitWithTimeout(time.Second) {
			t.Error("outbound webhook dispatcher did not stop")
		}
	})
	h := New(queries, testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = outbound

	create := testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Issue receiver", "destination": receiver.URL + "/events",
			"events": []string{"issue.created"}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated)
	var created struct {
		Subscription  outwebhook.Subscription `json:"subscription"`
		SigningSecret string                  `json:"signing_secret"`
	}
	create.JSON(&created)
	if created.SigningSecret == "" || created.Subscription.DestinationHint != receiver.URL {
		t.Fatalf("unsafe create response: %+v", created)
	}
	var second struct {
		Subscription  outwebhook.Subscription `json:"subscription"`
		SigningSecret string                  `json:"signing_secret"`
	}
	testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Second issue receiver", "destination": receiver.URL + "/events",
			"events": []string{"issue.created"}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&second)
	t.Cleanup(func() {
		for _, id := range []string{created.Subscription.ID, second.Subscription.ID} {
			_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, id)
			_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, id)
		}
	})

	var storedDestination, storedSecret []byte
	if err := testPool.QueryRow(context.Background(), `SELECT destination_ciphertext, secret_ciphertext FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID).Scan(&storedDestination, &storedSecret); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(storedDestination), receiver.URL) || strings.Contains(string(storedSecret), created.SigningSecret) {
		t.Fatal("destination and signing secret must not be stored in plaintext")
	}

	list := testutil.Call(t, h.ListOutboundWebhooks,
		withURLParam(newRequest(http.MethodGet, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", nil), "id", testWorkspaceID)).Want(http.StatusOK).Text()
	if strings.Contains(list, created.SigningSecret) || strings.Contains(list, second.SigningSecret) || strings.Contains(list, "/events") {
		t.Fatalf("normal reads exposed a secret or destination path: %s", list)
	}

	var issue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue,
		newRequest(http.MethodPost, "/api/issues", map[string]any{"title": "Webhook product seam"})).Want(http.StatusCreated).JSON(&issue)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID) })

	var got receivedOutboundWebhook
	select {
	case got = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("controlled receiver did not observe the outbound webhook")
	}
	if got.persistenceError != nil || got.persistedState != "pending" || string(got.persistedBody) != string(got.body) {
		t.Fatalf("delivery was not durably pending with its exact body before receiver I/O: state=%q err=%v", got.persistedState, got.persistenceError)
	}
	var gotSecond receivedOutboundWebhook
	select {
	case gotSecond = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("second controlled receiver request was not dispatched")
	}
	if got.persistedForEvent != 2 || gotSecond.persistedForEvent != 2 || got.header.Get("X-Multica-Event-ID") != gotSecond.header.Get("X-Multica-Event-ID") || got.header.Get("X-Multica-Delivery-ID") == gotSecond.header.Get("X-Multica-Delivery-ID") {
		t.Fatalf("all matched deliveries were not persisted before outbound I/O: first=%d second=%d", got.persistedForEvent, gotSecond.persistedForEvent)
	}
	var envelope struct {
		Version int    `json:"version"`
		ID      string `json:"id"`
		Type    string `json:"type"`
		Data    struct {
			Issue struct {
				ID    string `json:"id"`
				Title string `json:"title"`
			} `json:"issue"`
		} `json:"data"`
	}
	if err := json.Unmarshal(got.body, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Version != 1 || envelope.Type != "issue.created" || envelope.ID == "" || envelope.Data.Issue.ID != issue.ID || envelope.Data.Issue.Title != "Webhook product seam" {
		t.Fatalf("unexpected public envelope: %+v", envelope)
	}

	var deliveryID, eventID, subscriptionID, state string
	var persistedBody []byte
	if err := testPool.QueryRow(context.Background(), `SELECT id, event_id, subscription_id, state, request_body FROM outbound_webhook_delivery WHERE id = $1`, got.header.Get("X-Multica-Delivery-ID")).Scan(&deliveryID, &eventID, &subscriptionID, &state, &persistedBody); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for state == "pending" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		if err := testPool.QueryRow(context.Background(), `SELECT state FROM outbound_webhook_delivery WHERE id = $1`, deliveryID).Scan(&state); err != nil {
			t.Fatal(err)
		}
	}
	if state != "succeeded" || deliveryID != got.header.Get("X-Multica-Delivery-ID") || eventID != got.header.Get("X-Multica-Event-ID") || string(persistedBody) != string(got.body) {
		t.Fatalf("delivery identity/body mismatch: state=%s delivery=%s event=%s", state, deliveryID, eventID)
	}
	signingSecret := created.SigningSecret
	if subscriptionID == second.Subscription.ID {
		signingSecret = second.SigningSecret
	}
	mac := hmac.New(sha256.New, []byte(signingSecret))
	mac.Write([]byte(got.header.Get("X-Multica-Timestamp")))
	mac.Write([]byte("."))
	mac.Write(got.body)
	wantSignature := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(wantSignature), []byte(got.header.Get("X-Multica-Signature"))) {
		t.Fatal("receiver could not verify the exact persisted body")
	}
}

func TestMultiFieldIssueUpdatePersistsIndependentTypedProductEvents(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan receivedOutboundWebhook, 8)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed := receivedOutboundWebhook{body: body, header: r.Header.Clone()}
		observed.persistenceError = testPool.QueryRow(context.Background(),
			`SELECT state, request_body FROM outbound_webhook_delivery WHERE id = $1`,
			r.Header.Get("X-Multica-Delivery-ID"),
		).Scan(&observed.persistedState, &observed.persistedBody)
		received <- observed
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	queries := db.New(testPool)
	bus := events.New()
	outbound := outwebhook.New(queries, testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()))
	outbound.Register(bus)
	t.Cleanup(func() {
		outbound.Close()
		if !outbound.WaitWithTimeout(time.Second) {
			t.Error("outbound webhook dispatcher did not stop")
		}
	})
	h := New(queries, testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = outbound

	var created struct {
		Subscription outwebhook.Subscription `json:"subscription"`
	}
	testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Issue changes", "destination": receiver.URL + "/events",
			"events": []string{
				outwebhook.EventIssueStatusChanged,
				outwebhook.EventIssueAssigneeChanged,
				outwebhook.EventIssuePriorityChanged,
				outwebhook.EventIssueProjectChanged,
			},
			"scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&created)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, created.Subscription.ID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID)
	})

	var issue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue,
		newRequest(http.MethodPost, "/api/issues", map[string]any{"title": "Change every selected field"})).Want(http.StatusCreated).JSON(&issue)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID) })
	agentID := createHandlerTestAgent(t, "Outbound Webhook Assignee", []byte("[]"))
	projectID := createChatProjectTestProject(t, testWorkspaceID, "Outbound Webhook Project", "")

	updateRequest := withURLParam(newRequest(http.MethodPatch, "/api/issues/"+issue.ID, map[string]any{
		"status": "in_progress", "priority": "high", "project_id": projectID,
		"assignee_type": "agent", "assignee_id": agentID, "suppress_run": true,
	}), "id", issue.ID)
	updateRequest.Header.Set("X-Agent-ID", agentID)
	updateRequest.Header.Set("X-Actor-Source", "task_token")
	testutil.Call(t, h.UpdateIssue, updateRequest).Want(http.StatusOK)

	type changeEnvelope struct {
		ID    string `json:"id"`
		Type  string `json:"type"`
		Actor struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		} `json:"actor"`
		Data struct {
			Issue struct {
				ID       string `json:"id"`
				Assignee *struct {
					Type        string `json:"type"`
					ID          string `json:"id"`
					DisplayName string `json:"display_name"`
				} `json:"assignee"`
			} `json:"issue"`
			Change struct {
				Field    string          `json:"field"`
				Previous json.RawMessage `json:"previous"`
				Current  json.RawMessage `json:"current"`
			} `json:"change"`
		} `json:"data"`
	}
	got := make(map[string]changeEnvelope, 4)
	eventIDs := make(map[string]struct{}, 4)
	for range 4 {
		select {
		case request := <-received:
			if request.persistenceError != nil || request.persistedState != "pending" || string(request.persistedBody) != string(request.body) {
				t.Fatalf("delivery was not persisted before receiver I/O: state=%q err=%v", request.persistedState, request.persistenceError)
			}
			var envelope changeEnvelope
			if err := json.Unmarshal(request.body, &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.ID == "" || envelope.Data.Issue.ID != issue.ID {
				t.Fatalf("invalid change envelope: %s", request.body)
			}
			if _, duplicate := eventIDs[envelope.ID]; duplicate {
				t.Fatalf("Product Events shared an id: %s", envelope.ID)
			}
			eventIDs[envelope.ID] = struct{}{}
			got[envelope.Type] = envelope
		case <-time.After(5 * time.Second):
			t.Fatalf("receiver observed %d of 4 Issue Product Events", len(got))
		}
	}
	if len(got) != 4 {
		t.Fatalf("received event types = %v", got)
	}
	if got[outwebhook.EventIssueStatusChanged].Actor.Type != "agent" || got[outwebhook.EventIssueStatusChanged].Actor.ID != agentID {
		t.Fatalf("plugin/API actor identity lost: %+v", got[outwebhook.EventIssueStatusChanged].Actor)
	}
	assertTransition := func(eventType, field, previous, current string) {
		t.Helper()
		event, ok := got[eventType]
		if !ok || event.Data.Change.Field != field || string(event.Data.Change.Previous) != previous || string(event.Data.Change.Current) != current {
			t.Fatalf("%s transition = %+v", eventType, event.Data.Change)
		}
	}
	assertTransition(outwebhook.EventIssueStatusChanged, "status", `"todo"`, `"in_progress"`)
	assertTransition(outwebhook.EventIssuePriorityChanged, "priority", `"none"`, `"high"`)
	assertTransition(outwebhook.EventIssueProjectChanged, "project", `null`, `"`+projectID+`"`)
	assignee := got[outwebhook.EventIssueAssigneeChanged]
	if string(assignee.Data.Change.Previous) != "null" || assignee.Data.Issue.Assignee == nil || assignee.Data.Issue.Assignee.Type != "agent" || assignee.Data.Issue.Assignee.ID != agentID || assignee.Data.Issue.Assignee.DisplayName != "Outbound Webhook Assignee" {
		t.Fatalf("assignee transition lost typed identity: %+v", assignee)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE subscription_id = $1 AND event_type = 'issue.updated'`, created.Subscription.ID); count != 0 {
		t.Fatalf("broad issue.updated duplicate persisted: %d", count)
	}

	testutil.Call(t, h.BatchUpdateIssues, newRequest(http.MethodPatch, "/api/issues/batch", map[string]any{
		"issue_ids": []string{issue.ID},
		"updates":   map[string]any{"priority": "low"},
	})).Want(http.StatusOK)
	select {
	case request := <-received:
		var batch changeEnvelope
		if err := json.Unmarshal(request.body, &batch); err != nil {
			t.Fatal(err)
		}
		if batch.Type != outwebhook.EventIssuePriorityChanged || string(batch.Data.Change.Previous) != `"high"` || string(batch.Data.Change.Current) != `"low"` {
			t.Fatalf("batch source lost canonical before/current values: %s", request.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("batch Issue update produced no Product Event")
	}
}

func TestIssueAssigneeEventsPreserveVariantsAndExplicitNulls(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer receiver.Close()
	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	queries := db.New(testPool)
	bus := events.New()
	outbound := outwebhook.New(queries, testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()))
	outbound.Register(bus)
	t.Cleanup(func() { outbound.Close(); outbound.WaitWithTimeout(time.Second) })
	h := New(queries, testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = outbound

	workspaceUUID, _ := util.ParseUUID(testWorkspaceID)
	creatorUUID, _ := util.ParseUUID(testUserID)
	created, err := outbound.Create(context.Background(), outwebhook.CreateInput{
		WorkspaceID: workspaceUUID, CreatedBy: creatorUUID, Name: "Assignee variants",
		Destination: receiver.URL + "/events", Events: []string{outwebhook.EventIssueAssigneeChanged, outwebhook.EventIssueProjectChanged},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		subscriptionID, _ := util.ParseUUID(created.Subscription.ID)
		_, _ = outbound.Delete(context.Background(), workspaceUUID, subscriptionID)
	})
	var issue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue, newRequest(http.MethodPost, "/api/issues", map[string]any{"title": "Assignee variants"})).Want(http.StatusCreated).JSON(&issue)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID) })
	agentID := createHandlerTestAgent(t, "Webhook Variant Agent", []byte("[]"))
	squadID := createCommentTriggerPreviewSquad(t, "Webhook Variant Squad", agentID)
	projectID := createChatProjectTestProject(t, testWorkspaceID, "Webhook Null Project", "")

	update := func(body map[string]any) map[string]any {
		t.Helper()
		body["suppress_run"] = true
		testutil.Call(t, h.UpdateIssue, withURLParam(newRequest(http.MethodPatch, "/api/issues/"+issue.ID, body), "id", issue.ID)).Want(http.StatusOK)
		var raw []byte
		if err := testPool.QueryRow(context.Background(), `
			SELECT request_body FROM outbound_webhook_delivery
			WHERE subscription_id = $1 ORDER BY created_at DESC LIMIT 1
		`, created.Subscription.ID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var envelope map[string]any
		if err := json.Unmarshal(raw, &envelope); err != nil {
			t.Fatal(err)
		}
		return envelope["data"].(map[string]any)["change"].(map[string]any)
	}

	change := update(map[string]any{"assignee_type": "agent", "assignee_id": agentID})
	if change["previous"] != nil || change["current"].(map[string]any)["display_name"] != "Webhook Variant Agent" {
		t.Fatalf("nil -> agent transition = %#v", change)
	}
	change = update(map[string]any{"assignee_type": "squad", "assignee_id": squadID})
	if change["previous"].(map[string]any)["type"] != "agent" || change["current"].(map[string]any)["display_name"] != "Webhook Variant Squad" {
		t.Fatalf("agent -> squad transition = %#v", change)
	}
	change = update(map[string]any{"assignee_type": "member", "assignee_id": testUserID})
	if change["previous"].(map[string]any)["type"] != "squad" || change["current"].(map[string]any)["type"] != "member" {
		t.Fatalf("squad -> member transition = %#v", change)
	}
	change = update(map[string]any{"assignee_type": nil, "assignee_id": nil})
	if change["previous"].(map[string]any)["type"] != "member" || change["current"] != nil {
		t.Fatalf("member -> unassigned transition = %#v", change)
	}
	_ = update(map[string]any{"project_id": projectID})
	change = update(map[string]any{"project_id": nil})
	if change["previous"] != projectID || change["current"] != nil {
		t.Fatalf("project explicit null transition = %#v", change)
	}

	transferSquadID := createCommentTriggerPreviewSquad(t, "Webhook Transfer Squad", agentID)
	_ = update(map[string]any{"assignee_type": "squad", "assignee_id": transferSquadID})
	testutil.Call(t, h.DeleteSquad, withURLParams(
		newRequest(http.MethodDelete, "/api/workspaces/"+testWorkspaceID+"/squads/"+transferSquadID, nil),
		"workspaceId", testWorkspaceID,
		"id", transferSquadID,
	)).Want(http.StatusNoContent)
	var transferBody []byte
	if err := testPool.QueryRow(context.Background(), `
		SELECT request_body FROM outbound_webhook_delivery
		WHERE subscription_id = $1 AND event_type = $2 ORDER BY created_at DESC LIMIT 1
	`, created.Subscription.ID, outwebhook.EventIssueAssigneeChanged).Scan(&transferBody); err != nil {
		t.Fatal(err)
	}
	var transferEnvelope map[string]any
	if err := json.Unmarshal(transferBody, &transferEnvelope); err != nil {
		t.Fatal(err)
	}
	transferChange := transferEnvelope["data"].(map[string]any)["change"].(map[string]any)
	if transferChange["previous"].(map[string]any)["type"] != "squad" || transferChange["current"].(map[string]any)["type"] != "agent" {
		t.Fatalf("bulk squad assignee transfer = %#v", transferChange)
	}
}

func TestOutboundWebhookMutationPermissionsAndWorkspaceBoundary(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	outbound := outwebhook.New(db.New(testPool), testPool, box, []string{"https://127.0.0.1:9443"})
	h := *testHandler
	h.OutboundWebhooks = outbound

	memberID := dbfx.User(t, "Outbound Webhook Member", "outbound-webhook-member-"+uuid.NewString()+"@multica.test")
	dbfx.Member(t, testWorkspaceID, memberID, "member")
	request := withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
		"name": "Forbidden", "destination": "https://example.com/events",
		"events": []string{"issue.created"}, "scope_mode": "workspace",
	}), "id", testWorkspaceID)
	request.Header.Set("X-User-ID", memberID)
	testutil.Call(t, h.CreateOutboundWebhook, request).Want(http.StatusForbidden)

	foreignWorkspaceID := dbfx.Workspace(t, "Outbound Webhook Foreign", "outbound-webhook-foreign-"+uuid.NewString())
	foreignWorkspaceUUID, err := util.ParseUUID(foreignWorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	creatorUUID, err := util.ParseUUID(testUserID)
	if err != nil {
		t.Fatal(err)
	}
	created, err := outbound.Create(context.Background(), outwebhook.CreateInput{
		WorkspaceID: foreignWorkspaceUUID,
		CreatedBy:   creatorUUID,
		Name:        "Foreign subscription",
		Destination: "https://127.0.0.1:9443/events",
		Events:      []string{"issue.created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		subscriptionID, parseErr := util.ParseUUID(created.Subscription.ID)
		if parseErr == nil {
			_, _ = outbound.Delete(context.Background(), foreignWorkspaceUUID, subscriptionID)
		}
	})

	get := withURLParams(newRequest(http.MethodGet, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+created.Subscription.ID, nil),
		"id", testWorkspaceID, "subscriptionId", created.Subscription.ID)
	testutil.Call(t, h.GetOutboundWebhook, get).Want(http.StatusNotFound)
}

func TestOutboundWebhookEventSelectionCanBeEditedExplicitly(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	outbound := outwebhook.New(db.New(testPool), testPool, box, []string{"https://127.0.0.1:9443"})
	h := *testHandler
	h.OutboundWebhooks = outbound

	var created struct {
		Subscription outwebhook.Subscription `json:"subscription"`
	}
	testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Explicit events", "destination": "https://127.0.0.1:9443/events",
			"events": []string{outwebhook.EventIssueCreated, outwebhook.EventIssueStatusChanged}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&created)
	t.Cleanup(func() {
		subscriptionID, _ := util.ParseUUID(created.Subscription.ID)
		workspaceID, _ := util.ParseUUID(testWorkspaceID)
		_, _ = outbound.Delete(context.Background(), workspaceID, subscriptionID)
	})

	var updated outwebhook.Subscription
	testutil.Call(t, h.UpdateOutboundWebhookEvents, withURLParams(
		newRequest(http.MethodPatch, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+created.Subscription.ID+"/events", map[string]any{
			"events": []string{outwebhook.EventIssuePriorityChanged},
		}),
		"id", testWorkspaceID,
		"subscriptionId", created.Subscription.ID,
	)).Want(http.StatusOK).JSON(&updated)
	if updated.EventCatalogVersion != outwebhook.CatalogVersion || len(updated.Events) != 1 || updated.Events[0] != outwebhook.EventIssuePriorityChanged {
		t.Fatalf("event selection was implicitly widened: %+v", updated)
	}

	testutil.Call(t, h.UpdateOutboundWebhookEvents, withURLParams(
		newRequest(http.MethodPatch, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+created.Subscription.ID+"/events", map[string]any{
			"events": []string{"issue.updated"},
		}),
		"id", testWorkspaceID,
		"subscriptionId", created.Subscription.ID,
	)).Want(http.StatusBadRequest)
}

func TestOutboundWebhookDeleteWaitsForCaptureAndSweepsItsDelivery(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	outbound := outwebhook.New(db.New(testPool), testPool, box, []string{"https://127.0.0.1:9443"})
	workspaceID, err := util.ParseUUID(dbfx.Workspace(t, "Outbound Webhook Lock", "outbound-webhook-lock-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	creatorID, err := util.ParseUUID(testUserID)
	if err != nil {
		t.Fatal(err)
	}
	created, err := outbound.Create(context.Background(), outwebhook.CreateInput{
		WorkspaceID: workspaceID,
		CreatedBy:   creatorID,
		Name:        "Lock protocol",
		Destination: "https://127.0.0.1:9443/events",
		Events:      []string{"issue.created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	subscriptionID, err := util.ParseUUID(created.Subscription.ID)
	if err != nil {
		t.Fatal(err)
	}

	captureTx, err := testPool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer captureTx.Rollback(context.Background())
	captureQueries := db.New(testPool).WithTx(captureTx)
	rows, err := captureQueries.ListActiveOutboundWebhookSubscriptionsForEvent(context.Background(), db.ListActiveOutboundWebhookSubscriptionsForEventParams{WorkspaceID: workspaceID, EventType: outwebhook.EventIssueCreated})
	if err != nil || len(rows) != 1 {
		t.Fatalf("lock subscription for capture: rows=%d err=%v", len(rows), err)
	}
	eventID, _ := util.ParseUUID(uuid.NewString())
	if _, err := captureQueries.CreateOutboundWebhookDelivery(context.Background(), db.CreateOutboundWebhookDeliveryParams{
		EventID: eventID, SubscriptionID: subscriptionID, WorkspaceID: workspaceID,
		EventType: outwebhook.EventIssueCreated, RequestBody: []byte(`{"version":1}`),
	}); err != nil {
		t.Fatal(err)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleted, deleteErr := outbound.Delete(context.Background(), workspaceID, subscriptionID)
		if deleteErr == nil && !deleted {
			deleteErr = errors.New("subscription was not deleted")
		}
		deleteDone <- deleteErr
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("delete bypassed the capture row lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := captureTx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delete did not resume after capture committed")
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE workspace_id = $1`, workspaceID); count != 0 {
		t.Fatalf("orphan deliveries after serialized delete: %d", count)
	}
}

func TestWorkspaceDeleteFencePreventsLateOutboundWebhookDelivery(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	workspaceID, err := util.ParseUUID(dbfx.Workspace(t, "Outbound Webhook Workspace Fence", "outbound-webhook-workspace-fence-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	creatorID, _ := util.ParseUUID(testUserID)
	outbound := outwebhook.New(db.New(testPool), testPool, box, []string{"https://127.0.0.1:9443"})
	created, err := outbound.Create(context.Background(), outwebhook.CreateInput{
		WorkspaceID: workspaceID, CreatedBy: creatorID, Name: "Workspace fence",
		Destination: "https://127.0.0.1:9443/events", Events: []string{"issue.created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	subscriptionID, _ := util.ParseUUID(created.Subscription.ID)

	captureTx, err := testPool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer captureTx.Rollback(context.Background())
	captureQueries := db.New(testPool).WithTx(captureTx)
	if _, err := captureQueries.LockWorkspaceForOutboundWebhookCapture(context.Background(), workspaceID); err != nil {
		t.Fatal(err)
	}
	if _, err := captureQueries.ListActiveOutboundWebhookSubscriptionsForEvent(context.Background(), db.ListActiveOutboundWebhookSubscriptionsForEventParams{WorkspaceID: workspaceID, EventType: outwebhook.EventIssueCreated}); err != nil {
		t.Fatal(err)
	}
	eventID, _ := util.ParseUUID(uuid.NewString())
	if _, err := captureQueries.CreateOutboundWebhookDelivery(context.Background(), db.CreateOutboundWebhookDeliveryParams{
		EventID: eventID, SubscriptionID: subscriptionID, WorkspaceID: workspaceID,
		EventType: outwebhook.EventIssueCreated, RequestBody: []byte(`{"version":1}`),
	}); err != nil {
		t.Fatal(err)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteTx, beginErr := testPool.Begin(context.Background())
		if beginErr != nil {
			deleteDone <- beginErr
			return
		}
		defer deleteTx.Rollback(context.Background())
		deleteQueries := db.New(testPool).WithTx(deleteTx)
		if _, lockErr := deleteQueries.LockWorkspaceForDelete(context.Background(), workspaceID); lockErr != nil {
			deleteDone <- lockErr
			return
		}
		if sweepErr := deleteQueries.DeleteWorkspaceOutboundWebhooks(context.Background(), workspaceID); sweepErr != nil {
			deleteDone <- sweepErr
			return
		}
		deleteDone <- deleteTx.Commit(context.Background())
	}()
	select {
	case err := <-deleteDone:
		t.Fatalf("workspace teardown bypassed capture fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := captureTx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("workspace teardown did not resume after capture committed")
	}
	if deliveries := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE workspace_id = $1`, workspaceID); deliveries != 0 {
		t.Fatalf("workspace teardown left %d outbound webhook deliveries", deliveries)
	}
	if subscriptions := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_subscription WHERE workspace_id = $1`, workspaceID); subscriptions != 0 {
		t.Fatalf("workspace teardown left %d outbound webhook subscriptions", subscriptions)
	}
}

func TestOutboundWebhooksFailClosedWithoutEncryptionKey(t *testing.T) {
	h := *testHandler
	h.OutboundWebhooks = outwebhook.New(h.Queries, testPool, nil, nil)
	body := testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Unavailable", "destination": "https://example.com/events",
			"events": []string{"issue.created"}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusServiceUnavailable).Text()
	if !strings.Contains(body, "not configured") {
		t.Fatalf("unexpected response: %s", body)
	}
}
