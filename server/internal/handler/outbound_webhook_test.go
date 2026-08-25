package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/analytics"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/integrations/outwebhook"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type receivedOutboundWebhook struct {
	body              []byte
	header            http.Header
	persistedBody     []byte
	persistedState    string
	persistedForEvent int
	persistenceError  error
}

type outboundWebhookRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn outboundWebhookRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return fn(request)
}

type outboundWebhookObserver struct {
	mu         sync.Mutex
	operations map[string]int
	oldest     float64
	oldestSet  bool
}

func newOutboundWebhookObserver() *outboundWebhookObserver {
	return &outboundWebhookObserver{operations: make(map[string]int)}
}

func (o *outboundWebhookObserver) RecordOutboundWebhookOperation(operation string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.operations[operation]++
}

func (o *outboundWebhookObserver) SetOutboundWebhookOldestPending(seconds float64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.oldest = seconds
	o.oldestSet = true
}

func (o *outboundWebhookObserver) recorded(operation string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.operations[operation] > 0
}

func (o *outboundWebhookObserver) snapshot() map[string]int {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make(map[string]int, len(o.operations))
	for operation, count := range o.operations {
		result[operation] = count
	}
	return result
}

func (o *outboundWebhookObserver) observedOldestPending() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.oldestSet && o.oldest >= 0
}

func createReliabilityWebhookSubscription(t *testing.T, h *Handler, name, destination string) outwebhook.Subscription {
	t.Helper()
	var created struct {
		Subscription outwebhook.Subscription `json:"subscription"`
	}
	testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": name, "destination": destination,
			"events": []string{"issue.created"}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&created)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, created.Subscription.ID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID)
	})
	return created.Subscription
}

func createReliabilityWebhookIssue(t *testing.T, h *Handler, title string) string {
	t.Helper()
	var issue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue,
		newRequest(http.MethodPost, "/api/issues", map[string]any{"title": title})).Want(http.StatusCreated).JSON(&issue)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID) })
	return issue.ID
}

func waitOutboundWebhookDeliveryState(t *testing.T, deliveryID, want string) (string, int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var state string
	var attemptCount int
	for time.Now().Before(deadline) {
		if err := testPool.QueryRow(context.Background(), `SELECT state, attempt_count FROM outbound_webhook_delivery WHERE id = $1`, deliveryID).Scan(&state, &attemptCount); err != nil {
			t.Fatal(err)
		}
		if state == want {
			return state, attemptCount
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("delivery %s state = %s, want %s", deliveryID, state, want)
	return state, attemptCount
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

func TestOutboundWebhookRetriesTransientReceiverFailure(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan string, 6)
	statuses := []int{http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusNoContent}
	var receiverAttempts atomic.Int32
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := int(receiverAttempts.Add(1)) - 1
		status := statuses[attempt]
		if status == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "0")
		}
		w.WriteHeader(status)
	}))
	defer receiver.Close()
	client := receiver.Client()
	baseTransport := client.Transport
	var transportAttempts atomic.Int32
	client.Transport = outboundWebhookRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		received <- request.Header.Get("X-Multica-Delivery-ID")
		if transportAttempts.Add(1) == 1 {
			return nil, errors.New("controlled network failure")
		}
		return baseTransport.RoundTrip(request)
	})

	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	policy := outwebhook.DefaultDeliveryPolicy()
	policy.PollInterval = 5 * time.Millisecond
	policy.InitialBackoff = 5 * time.Millisecond
	policy.MaxBackoff = 20 * time.Millisecond
	outbound := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(client), outwebhook.WithDeliveryPolicy(policy), outwebhook.WithRetryJitter(func(delay time.Duration) time.Duration { return delay }))
	bus := events.New()
	outbound.Register(bus)
	t.Cleanup(func() {
		outbound.Close()
		outbound.WaitWithTimeout(time.Second)
	})
	h := New(db.New(testPool), testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = outbound

	var created struct {
		Subscription outwebhook.Subscription `json:"subscription"`
	}
	testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Retry receiver", "destination": receiver.URL + "/events",
			"events": []string{"issue.created"}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&created)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, created.Subscription.ID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID)
	})

	var issue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue,
		newRequest(http.MethodPost, "/api/issues", map[string]any{"title": "Retry durable webhook"})).Want(http.StatusCreated).JSON(&issue)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID) })

	first := ""
	for attempt := 0; attempt < 6; attempt++ {
		select {
		case deliveryID := <-received:
			if first == "" {
				first = deliveryID
			} else if deliveryID != first {
				t.Fatalf("retry changed delivery id: first=%s attempt=%s", first, deliveryID)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("transient receiver failure was not retried at attempt %d", attempt+1)
		}
	}
	var state string
	var attemptCount int
	deadline := time.Now().Add(5 * time.Second)
	for state != "succeeded" && time.Now().Before(deadline) {
		if err := testPool.QueryRow(context.Background(), `SELECT state, attempt_count FROM outbound_webhook_delivery WHERE id = $1`, first).Scan(&state, &attemptCount); err != nil {
			t.Fatal(err)
		}
		if state != "succeeded" {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if state != "succeeded" || attemptCount != 6 {
		t.Fatalf("retry outcome = state %s attempts %d, want succeeded after 6", state, attemptCount)
	}
}

func TestOutboundWebhookRestartRecoversPendingAndExpiredLease(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan receivedOutboundWebhook, 2)
	var attempts atomic.Int32
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- receivedOutboundWebhook{body: body, header: r.Header.Clone()}
		if attempts.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	policy := outwebhook.DefaultDeliveryPolicy()
	policy.PollInterval = 5 * time.Millisecond
	policy.LeaseDuration = 500 * time.Millisecond
	serviceOne := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithDeliveryPolicy(policy))
	bus := events.New()
	serviceOne.Register(bus)
	h := New(db.New(testPool), testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = serviceOne
	subscription := createReliabilityWebhookSubscription(t, h, "Restart receiver", receiver.URL+"/events")
	createReliabilityWebhookIssue(t, h, "Restart durable webhook")

	var first receivedOutboundWebhook
	select {
	case first = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("first server did not start a durable delivery")
	}
	serviceOne.Close()
	if !serviceOne.WaitWithTimeout(time.Second) {
		t.Fatal("first server did not stop")
	}

	leaseExpiry := time.Now().Add(250 * time.Millisecond)
	if _, err := testPool.Exec(context.Background(), `
		UPDATE outbound_webhook_delivery
		SET lease_token = gen_random_uuid(), lease_expires_at = $2, next_attempt_at = now()
		WHERE id = $1 AND state = 'pending'
	`, first.header.Get("X-Multica-Delivery-ID"), leaseExpiry); err != nil {
		t.Fatal(err)
	}
	observer := newOutboundWebhookObserver()
	serviceTwo := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithDeliveryPolicy(policy), outwebhook.WithObserver(observer))
	serviceTwo.Register(events.New())
	t.Cleanup(func() {
		serviceTwo.Close()
		serviceTwo.WaitWithTimeout(time.Second)
	})

	select {
	case <-received:
		t.Fatal("replacement server reclaimed a live lease before it expired")
	case <-time.After(100 * time.Millisecond):
	}
	var second receivedOutboundWebhook
	select {
	case second = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement server did not reclaim the expired lease")
	}
	if first.header.Get("X-Multica-Delivery-ID") != second.header.Get("X-Multica-Delivery-ID") ||
		first.header.Get("X-Multica-Event-ID") != second.header.Get("X-Multica-Event-ID") ||
		string(first.body) != string(second.body) {
		t.Fatal("lease recovery changed the delivery id, event id, or saved body")
	}
	state, count := waitOutboundWebhookDeliveryState(t, second.header.Get("X-Multica-Delivery-ID"), "succeeded")
	if state != "succeeded" || count != 2 {
		t.Fatalf("recovered delivery = %s after %d counted attempts", state, count)
	}
	if got := subscription.ID; got == "" {
		t.Fatal("subscription fixture was not created")
	}
	if !observer.recorded("lease_recovered") || !observer.recorded("delivery_succeeded") {
		t.Fatalf("lease recovery metrics were not observed: %+v", observer.snapshot())
	}
}

func TestOutboundWebhookHonorsBoundedRetryAfter(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan time.Time, 2)
	var attempts atomic.Int32
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- time.Now()
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "3600")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	policy := outwebhook.DefaultDeliveryPolicy()
	policy.PollInterval = 2 * time.Millisecond
	policy.InitialBackoff = 5 * time.Millisecond
	policy.MaxBackoff = 300 * time.Millisecond
	policy.MaxRetryAfter = 200 * time.Millisecond
	observer := newOutboundWebhookObserver()
	serviceUnderTest := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithDeliveryPolicy(policy), outwebhook.WithObserver(observer))
	bus := events.New()
	serviceUnderTest.Register(bus)
	t.Cleanup(func() {
		serviceUnderTest.Close()
		serviceUnderTest.WaitWithTimeout(time.Second)
	})
	h := New(db.New(testPool), testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = serviceUnderTest
	subscription := createReliabilityWebhookSubscription(t, h, "Retry-After receiver", receiver.URL+"/events")
	createReliabilityWebhookIssue(t, h, "Bounded Retry-After")

	var first time.Time
	select {
	case first = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not observe initial Retry-After attempt")
	}
	var deliveryID string
	var nextAttemptAt, lastAttemptAt time.Time
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err := testPool.QueryRow(context.Background(), `
			SELECT id, next_attempt_at, last_attempt_at
			FROM outbound_webhook_delivery
			WHERE subscription_id = $1 AND attempt_count = 1 AND lease_token IS NULL
		`, subscription.ID).Scan(&deliveryID, &nextAttemptAt, &lastAttemptAt)
		if err == nil {
			break
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if nextAttemptAt.IsZero() {
		t.Fatal("Retry-After schedule was not persisted")
	}
	if delay := nextAttemptAt.Sub(lastAttemptAt); delay < 160*time.Millisecond || delay > 260*time.Millisecond {
		t.Fatalf("persisted bounded Retry-After = %s, want approximately 200ms", delay)
	}
	select {
	case <-received:
		t.Fatal("delivery retried before the persisted Retry-After delay")
	case <-time.After(75 * time.Millisecond):
	}
	var second time.Time
	select {
	case second = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery did not retry after bounded Retry-After")
	}
	if elapsed := second.Sub(first); elapsed < 160*time.Millisecond || elapsed > time.Second {
		t.Fatalf("receiver retry delay = %s, want bounded delay of at least 160ms", elapsed)
	}
	state, count := waitOutboundWebhookDeliveryState(t, deliveryID, "succeeded")
	if state != "succeeded" || count != 2 {
		t.Fatalf("Retry-After delivery = %s after %d attempts, want succeeded after 2", state, count)
	}
	if !observer.recorded("delivery_retry_scheduled") || !observer.recorded("delivery_succeeded") {
		t.Fatalf("retry metrics were not observed: %+v", observer.snapshot())
	}
}

func TestOutboundWebhookTerminalFailuresPauseAndSuccessResets(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	responses := make(chan int, 4)
	for _, status := range []int{http.StatusBadRequest, http.StatusNoContent, http.StatusBadRequest, http.StatusBadRequest} {
		responses <- status
	}
	received := make(chan string, 6)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Get("X-Multica-Delivery-ID")
		w.WriteHeader(<-responses)
	}))
	defer receiver.Close()

	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	policy := outwebhook.DefaultDeliveryPolicy()
	policy.PollInterval = 5 * time.Millisecond
	policy.ConsecutiveFailureThreshold = 2
	observer := newOutboundWebhookObserver()
	serviceUnderTest := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithDeliveryPolicy(policy), outwebhook.WithObserver(observer))
	bus := events.New()
	serviceUnderTest.Register(bus)
	t.Cleanup(func() {
		serviceUnderTest.Close()
		serviceUnderTest.WaitWithTimeout(time.Second)
	})
	h := New(db.New(testPool), testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = serviceUnderTest
	subscription := createReliabilityWebhookSubscription(t, h, "Failure threshold receiver", receiver.URL+"/events")

	checks := []struct {
		wantDeliveryState string
		wantFailures      int
		wantStatus        string
	}{
		{wantDeliveryState: "failed", wantFailures: 1, wantStatus: "active"},
		{wantDeliveryState: "succeeded", wantFailures: 0, wantStatus: "active"},
		{wantDeliveryState: "failed", wantFailures: 1, wantStatus: "active"},
		{wantDeliveryState: "failed", wantFailures: 2, wantStatus: "paused"},
	}
	for i, check := range checks {
		createReliabilityWebhookIssue(t, h, fmt.Sprintf("Terminal outcome %d", i+1))
		var deliveryID string
		select {
		case deliveryID = <-received:
		case <-time.After(5 * time.Second):
			t.Fatalf("receiver did not observe outcome %d", i+1)
		}
		waitOutboundWebhookDeliveryState(t, deliveryID, check.wantDeliveryState)
		var status string
		var pauseReason *string
		var failures int
		if err := testPool.QueryRow(context.Background(), `
			SELECT status, pause_reason, consecutive_terminal_failures
			FROM outbound_webhook_subscription WHERE id = $1
		`, subscription.ID).Scan(&status, &pauseReason, &failures); err != nil {
			t.Fatal(err)
		}
		if status != check.wantStatus || failures != check.wantFailures {
			t.Fatalf("outcome %d subscription = %s/%d, want %s/%d", i+1, status, failures, check.wantStatus, check.wantFailures)
		}
		if status == "paused" && (pauseReason == nil || *pauseReason != "failure_threshold") {
			t.Fatalf("paused subscription reason = %v", pauseReason)
		}
	}

	visible, err := serviceUnderTest.Get(context.Background(), mustParseOutboundWebhookUUID(t, testWorkspaceID), mustParseOutboundWebhookUUID(t, subscription.ID))
	if err != nil {
		t.Fatal(err)
	}
	if visible.Status != "paused" || visible.PauseReason == nil || *visible.PauseReason != "failure_threshold" {
		t.Fatalf("automatic pause was not visible through the subscription API: %+v", visible)
	}
	for _, operation := range []string{"delivery_failed", "delivery_succeeded", "subscription_paused"} {
		if !observer.recorded(operation) {
			t.Fatalf("operation metric %s was not observed: %+v", operation, observer.snapshot())
		}
	}
}

func TestOutboundWebhookConcurrencyAndPendingGrowthAreBounded(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	started := make(chan string, 4)
	release := make(chan struct{})
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- r.Header.Get("X-Multica-Delivery-ID")
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	defer receiver.Close()

	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	policy := outwebhook.DefaultDeliveryPolicy()
	policy.PollInterval = 5 * time.Millisecond
	policy.GlobalConcurrency = 2
	policy.SubscriptionConcurrency = 1
	policy.MaxPendingPerSubscription = 3
	serviceUnderTest := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithDeliveryPolicy(policy))
	bus := events.New()
	serviceUnderTest.Register(bus)
	competingService := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithDeliveryPolicy(policy))
	competingService.Register(events.New())
	t.Cleanup(func() {
		serviceUnderTest.Close()
		serviceUnderTest.WaitWithTimeout(time.Second)
		competingService.Close()
		competingService.WaitWithTimeout(time.Second)
	})
	h := New(db.New(testPool), testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = serviceUnderTest
	first := createReliabilityWebhookSubscription(t, h, "Bounded receiver A", receiver.URL+"/events")
	second := createReliabilityWebhookSubscription(t, h, "Bounded receiver B", receiver.URL+"/events")

	for i := 0; i < 4; i++ {
		createReliabilityWebhookIssue(t, h, fmt.Sprintf("Bounded webhook %d", i+1))
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d bounded workers started", i)
		}
	}
	select {
	case <-started:
		t.Fatal("global concurrency exceeded two live deliveries")
	case <-time.After(100 * time.Millisecond):
	}

	var live int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM outbound_webhook_delivery
		WHERE state = 'pending' AND lease_token IS NOT NULL AND lease_expires_at > now()
	`).Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 2 {
		t.Fatalf("live delivery leases = %d, want 2", live)
	}
	rows, err := testPool.Query(context.Background(), `
		SELECT subscription_id, count(*) FILTER (WHERE lease_token IS NOT NULL AND lease_expires_at > now()), count(*)
		FROM outbound_webhook_delivery
		WHERE subscription_id = ANY($1::uuid[]) AND state = 'pending'
		GROUP BY subscription_id
	`, []string{first.ID, second.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var subscriptionID string
		var liveForSubscription, pending int
		if err := rows.Scan(&subscriptionID, &liveForSubscription, &pending); err != nil {
			t.Fatal(err)
		}
		if liveForSubscription != 1 || pending != 3 {
			t.Fatalf("subscription %s has live=%d pending=%d, want 1/3", subscriptionID, liveForSubscription, pending)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("bounded subscriptions observed = %d, want 2", seen)
	}

	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := testPool.QueryRow(context.Background(), `
			SELECT count(*) FROM outbound_webhook_delivery
			WHERE subscription_id = ANY($1::uuid[]) AND state = 'pending'
		`, []string{first.ID, second.ID}).Scan(&live); err != nil {
			t.Fatal(err)
		}
		if live == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if live != 0 {
		t.Fatalf("bounded delivery queue did not drain: %d pending", live)
	}
}

func TestOutboundWebhookRetryAttemptsAreBounded(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan string, 3)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Get("X-Multica-Delivery-ID")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer receiver.Close()

	box, _ := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	policy := outwebhook.DefaultDeliveryPolicy()
	policy.PollInterval = 5 * time.Millisecond
	policy.InitialBackoff = 5 * time.Millisecond
	policy.MaxBackoff = 5 * time.Millisecond
	policy.MaxAttempts = 2
	policy.ConsecutiveFailureThreshold = 10
	serviceUnderTest := outwebhook.New(db.New(testPool), testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithDeliveryPolicy(policy), outwebhook.WithRetryJitter(func(delay time.Duration) time.Duration { return delay }))
	bus := events.New()
	serviceUnderTest.Register(bus)
	t.Cleanup(func() {
		serviceUnderTest.Close()
		serviceUnderTest.WaitWithTimeout(time.Second)
	})
	h := New(db.New(testPool), testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = serviceUnderTest
	createReliabilityWebhookSubscription(t, h, "Attempt cap receiver", receiver.URL+"/events")
	createReliabilityWebhookIssue(t, h, "Bound retry attempts")

	var first, second string
	select {
	case first = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for initial delivery attempt")
	}
	select {
	case second = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for retry delivery attempt")
	}
	if second != first {
		t.Fatal("bounded retry changed the delivery identifier")
	}
	state, count := waitOutboundWebhookDeliveryState(t, first, "failed")
	if state != "failed" || count != 2 {
		t.Fatalf("bounded retry = %s after %d attempts, want failed after 2", state, count)
	}
	select {
	case <-received:
		t.Fatal("delivery exceeded its configured attempt cap")
	case <-time.After(100 * time.Millisecond):
	}
}

func mustParseOutboundWebhookUUID(t *testing.T, raw string) pgtype.UUID {
	t.Helper()
	parsed, err := util.ParseUUID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
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

func TestCommentLifecyclePersistsAndDeliversSelectedWorkspaceWebhooks(t *testing.T) {
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

	create := testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Comment receiver", "destination": receiver.URL + "/events",
			"events": []string{"comment.created", "comment.updated", "comment.deleted"}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated)
	var subscription struct {
		Subscription outwebhook.Subscription `json:"subscription"`
	}
	create.JSON(&subscription)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, subscription.Subscription.ID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, subscription.Subscription.ID)
	})

	projectID := dbfx.Project(t, "Comment webhook project")
	issueID := dbfx.Issue(t, "Comment webhook parent", testutil.Cols{"project_id": projectID})
	createComment := func(content string, parentID *string) CommentResponse {
		t.Helper()
		body := map[string]any{"content": content}
		if parentID != nil {
			body["parent_id"] = *parentID
		}
		var comment CommentResponse
		testutil.Call(t, h.CreateComment,
			withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", body), "id", issueID)).Want(http.StatusCreated).JSON(&comment)
		return comment
	}
	receive := func(wantType string) map[string]any {
		t.Helper()
		select {
		case got := <-received:
			if got.persistenceError != nil || got.persistedState != "pending" || string(got.persistedBody) != string(got.body) {
				t.Fatalf("%s was not persisted before receiver I/O: state=%q err=%v", wantType, got.persistedState, got.persistenceError)
			}
			if got.header.Get("X-Multica-Event") != wantType {
				t.Fatalf("event header = %q, want %q", got.header.Get("X-Multica-Event"), wantType)
			}
			var envelope map[string]any
			if err := json.Unmarshal(got.body, &envelope); err != nil {
				t.Fatal(err)
			}
			return envelope
		case <-time.After(5 * time.Second):
			t.Fatalf("controlled receiver did not observe %s", wantType)
			return nil
		}
	}

	root := createComment(strings.Repeat("界", 500)+"🙂", nil)
	createdEnvelope := receive("comment.created")
	createdJSON, _ := json.Marshal(createdEnvelope)
	if strings.Contains(string(createdJSON), "attachments") || strings.Contains(string(createdJSON), "reactions") || strings.Contains(string(createdJSON), `"content"`) {
		t.Fatalf("internal comment fields leaked: %s", createdJSON)
	}
	createdData := createdEnvelope["data"].(map[string]any)
	createdComment := createdData["comment"].(map[string]any)
	if len([]rune(createdComment["excerpt"].(string))) != 500 || createdComment["truncated"] != true {
		t.Fatalf("created excerpt contract failed: %#v", createdComment)
	}
	if createdEnvelope["actor"].(map[string]any)["type"] != "member" || createdComment["author"].(map[string]any)["type"] != "member" {
		t.Fatalf("member actor/author context missing: %#v", createdEnvelope)
	}
	issueSnapshot := createdData["issue"].(map[string]any)
	if issueSnapshot["id"] != issueID || issueSnapshot["title"] != "Comment webhook parent" || issueSnapshot["project_id"] != projectID {
		t.Fatalf("parent issue snapshot missing: %#v", issueSnapshot)
	}

	reply := createComment("reply body", &root.ID)
	replyEnvelope := receive("comment.created")
	if got := replyEnvelope["data"].(map[string]any)["comment"].(map[string]any)["parent_id"]; got != root.ID {
		t.Fatalf("reply parent = %v, want %s", got, root.ID)
	}

	testutil.Call(t, h.UpdateComment,
		withURLParam(newRequest(http.MethodPut, "/api/comments/"+reply.ID, map[string]any{"content": "edited reply"}), "commentId", reply.ID)).Want(http.StatusOK)
	updatedEnvelope := receive("comment.updated")
	if got := updatedEnvelope["data"].(map[string]any)["comment"].(map[string]any)["excerpt"]; got != "edited reply" {
		t.Fatalf("updated excerpt = %v", got)
	}

	bus.Publish(events.Event{
		Type: protocol.EventCommentUpdated, WorkspaceID: testWorkspaceID, ActorType: "member", ActorID: testUserID,
		Payload: map[string]any{"body_changed": false, "issue_project_id": nil, "comment": map[string]any{
			"id": reply.ID, "issue_id": issueID, "author_type": "member", "author_id": testUserID,
			"type": "comment", "content": "edited reply",
		}},
	})
	select {
	case got := <-received:
		t.Fatalf("attachment-only update unexpectedly delivered %s", got.header.Get("X-Multica-Event"))
	case <-time.After(200 * time.Millisecond):
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE subscription_id = $1 AND event_type = 'comment.updated'`, subscription.Subscription.ID); count != 1 {
		t.Fatalf("attachment-only update changed persisted delivery count to %d, want 1", count)
	}

	testutil.Call(t, h.DeleteComment,
		withURLParam(newRequest(http.MethodDelete, "/api/comments/"+reply.ID, nil), "commentId", reply.ID)).Want(http.StatusNoContent)
	deletedEnvelope := receive("comment.deleted")
	deletedJSON, _ := json.Marshal(deletedEnvelope)
	if strings.Contains(string(deletedJSON), "edited reply") || strings.Contains(string(deletedJSON), "excerpt") || strings.Contains(string(deletedJSON), "content") {
		t.Fatalf("deleted body leaked: %s", deletedJSON)
	}
	deletedComment := deletedEnvelope["data"].(map[string]any)["comment"].(map[string]any)
	if deletedComment["id"] != reply.ID || deletedComment["parent_id"] != root.ID || deletedComment["deleted_at"] == "" {
		t.Fatalf("deletion identity/context missing: %#v", deletedComment)
	}

	for _, variant := range []struct {
		authorType string
		parentID   *string
	}{
		{authorType: "agent"},
		{authorType: "system", parentID: &root.ID},
	} {
		authorID := uuid.NewString()
		if variant.authorType == "system" {
			authorID = "00000000-0000-0000-0000-000000000000"
		}
		bus.Publish(events.Event{
			Type: protocol.EventCommentCreated, WorkspaceID: testWorkspaceID,
			ActorType: variant.authorType, ActorID: authorID,
			Payload: map[string]any{
				"issue_title": "Comment contract", "issue_status": "todo", "issue_priority": "none", "issue_project_id": nil,
				"comment": map[string]any{
					"id": uuid.NewString(), "issue_id": issueID, "parent_id": variant.parentID,
					"author_type": variant.authorType, "author_id": authorID,
					"type": "comment", "content": variant.authorType + " body",
				},
			},
		})
		envelope := receive("comment.created")
		comment := envelope["data"].(map[string]any)["comment"].(map[string]any)
		if envelope["actor"].(map[string]any)["type"] != variant.authorType || comment["author"].(map[string]any)["type"] != variant.authorType {
			t.Fatalf("%s actor/author contract missing: %#v", variant.authorType, envelope)
		}
		if variant.parentID != nil && comment["parent_id"] != *variant.parentID {
			t.Fatalf("%s reply parent missing: %#v", variant.authorType, comment)
		}
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

	for _, tc := range []struct {
		name    string
		method  string
		suffix  string
		body    any
		handler func(http.ResponseWriter, *http.Request)
	}{
		{name: "update", method: http.MethodPut, body: map[string]any{"name": "Denied", "events": []string{"issue.created"}, "scope_mode": "workspace"}, handler: h.UpdateOutboundWebhook},
		{name: "pause", method: http.MethodPost, suffix: "/pause", handler: h.PauseOutboundWebhook},
		{name: "resume", method: http.MethodPost, suffix: "/resume", handler: h.ResumeOutboundWebhook},
		{name: "test", method: http.MethodPost, suffix: "/test", handler: h.TestOutboundWebhook},
		{name: "rotate", method: http.MethodPost, suffix: "/rotate-secret", handler: h.RotateOutboundWebhookSecret},
		{name: "delete", method: http.MethodDelete, handler: h.DeleteOutboundWebhook},
	} {
		t.Run("member cannot "+tc.name, func(t *testing.T) {
			subscriptionID := uuid.NewString()
			request := withURLParams(newRequest(tc.method, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+subscriptionID+tc.suffix, tc.body),
				"id", testWorkspaceID, "subscriptionId", subscriptionID)
			request.Header.Set("X-User-ID", memberID)
			testutil.Call(t, tc.handler, request).Want(http.StatusForbidden)
		})
	}

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
	pause := withURLParams(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+created.Subscription.ID+"/pause", nil),
		"id", testWorkspaceID, "subscriptionId", created.Subscription.ID)
	testutil.Call(t, h.PauseOutboundWebhook, pause).Want(http.StatusNotFound)
}

func TestOutboundWebhookProjectScopeValidationUpdateAndDeletion(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	observer := newOutboundWebhookObserver()
	outbound := outwebhook.New(db.New(testPool), testPool, box, []string{"https://127.0.0.1:9443"}, outwebhook.WithObserver(observer))
	h := *testHandler
	h.OutboundWebhooks = outbound
	projectA := dbfx.Project(t, "Webhook scope A")
	projectB := dbfx.Project(t, "Webhook scope B")
	foreignWorkspaceID := dbfx.Workspace(t, "Webhook scope foreign", "webhook-scope-foreign-"+uuid.NewString())
	foreignProject := createChatProjectTestProject(t, foreignWorkspaceID, "Foreign webhook scope", "")

	create := func(scopeMode string, projectIDs []string) *testutil.Response {
		t.Helper()
		return testutil.Call(t, h.CreateOutboundWebhook,
			withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
				"name": "Scoped receiver", "destination": "https://127.0.0.1:9443/events",
				"events": []string{"issue.created"}, "scope_mode": scopeMode, "project_ids": projectIDs,
			}), "id", testWorkspaceID))
	}
	create(outwebhook.ScopeProject, nil).Want(http.StatusBadRequest)
	create(outwebhook.ScopeWorkspace, []string{projectA}).Want(http.StatusBadRequest)
	create(outwebhook.ScopeProject, []string{foreignProject}).Want(http.StatusBadRequest)

	var created struct {
		Subscription outwebhook.Subscription `json:"subscription"`
	}
	create(outwebhook.ScopeWorkspace, nil).Want(http.StatusCreated).JSON(&created)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, created.Subscription.ID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID)
	})

	update := withURLParams(newRequest(http.MethodPatch, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+created.Subscription.ID+"/scope", map[string]any{
		"scope_mode": outwebhook.ScopeProject, "project_ids": []string{projectA, projectB},
	}), "id", testWorkspaceID, "subscriptionId", created.Subscription.ID)
	var scoped outwebhook.Subscription
	testutil.Call(t, h.UpdateOutboundWebhookScope, update).Want(http.StatusOK).JSON(&scoped)
	if scoped.ScopeMode != outwebhook.ScopeProject || len(scoped.ProjectIDs) != 2 {
		t.Fatalf("multi-project scope not returned: %+v", scoped)
	}

	testutil.Call(t, h.DeleteProject,
		withURLParam(newRequest(http.MethodDelete, "/api/projects/"+projectA, nil), "id", projectA)).Want(http.StatusNoContent)
	var status, pauseReason string
	var projectIDs []string
	if err := testPool.QueryRow(context.Background(), `SELECT status, COALESCE(pause_reason, ''), project_ids::text[] FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID).Scan(&status, &pauseReason, &projectIDs); err != nil {
		t.Fatal(err)
	}
	if status != "active" || pauseReason != "" || len(projectIDs) != 1 || projectIDs[0] != projectB {
		t.Fatalf("first deletion did not narrow scope safely: status=%s reason=%s projects=%v", status, pauseReason, projectIDs)
	}

	testutil.Call(t, h.DeleteProject,
		withURLParam(newRequest(http.MethodDelete, "/api/projects/"+projectB, nil), "id", projectB)).Want(http.StatusNoContent)
	if err := testPool.QueryRow(context.Background(), `SELECT status, pause_reason, project_ids::text[] FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID).Scan(&status, &pauseReason, &projectIDs); err != nil {
		t.Fatal(err)
	}
	if status != "paused" || pauseReason != "scope_empty" || len(projectIDs) != 0 {
		t.Fatalf("empty scope widened or stayed active: status=%s reason=%s projects=%v", status, pauseReason, projectIDs)
	}
	if got := observer.snapshot()["subscription_paused"]; got != 1 {
		t.Fatalf("scope-empty pause metric count = %d, want 1", got)
	}
}

func TestProjectScopedWebhookUsesIssueAndCommentEventTimeProjectSnapshots(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan string, 6)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- r.Header.Get("X-Multica-Event")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	queries := db.New(testPool)
	bus := events.New()
	moveIssueID := ""
	moveToProjectID := ""
	bus.Subscribe(protocol.EventCommentCreated, func(events.Event) {
		if moveIssueID != "" && moveToProjectID != "" {
			if _, err := testPool.Exec(context.Background(), `UPDATE issue SET project_id = $1 WHERE id = $2`, moveToProjectID, moveIssueID); err != nil {
				t.Errorf("move issue before outbound capture: %v", err)
			}
			moveIssueID = ""
		}
	})
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
	projectA := dbfx.Project(t, "Webhook event scope A")
	projectB := dbfx.Project(t, "Webhook event scope B")

	var created struct {
		Subscription outwebhook.Subscription `json:"subscription"`
	}
	testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Project events", "destination": receiver.URL + "/events",
			"events":     []string{outwebhook.EventIssueCreated, outwebhook.EventIssueProjectChanged, outwebhook.EventCommentCreated},
			"scope_mode": outwebhook.ScopeProject, "project_ids": []string{projectA},
		}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&created)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, created.Subscription.ID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, created.Subscription.ID)
	})

	var issue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue,
		newRequest(http.MethodPost, "/api/issues", map[string]any{"title": "Initially unprojected"})).Want(http.StatusCreated).JSON(&issue)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID) })
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE subscription_id = $1`, created.Subscription.ID); count != 0 {
		t.Fatalf("unprojected Issue matched Project Scope: %d deliveries", count)
	}

	receive := func(want string) {
		t.Helper()
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("received event %q, want %q", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("controlled receiver did not observe %s", want)
		}
	}
	testutil.Call(t, h.UpdateIssue,
		withURLParam(newRequest(http.MethodPatch, "/api/issues/"+issue.ID, map[string]any{"project_id": projectA, "suppress_run": true}), "id", issue.ID)).Want(http.StatusOK)
	receive(outwebhook.EventIssueProjectChanged)

	testutil.Call(t, h.CreateComment,
		withURLParam(newRequest(http.MethodPost, "/api/issues/"+issue.ID+"/comments", map[string]any{"content": "Project snapshot comment"}), "id", issue.ID)).Want(http.StatusCreated)
	receive(outwebhook.EventCommentCreated)

	// A listener registered before Outbound moves storage to project B after
	// the comment commits. Outbound must still use the SQL-returned project A
	// snapshot carried by the event.
	moveIssueID, moveToProjectID = issue.ID, projectB
	testutil.Call(t, h.CreateComment,
		withURLParam(newRequest(http.MethodPost, "/api/issues/"+issue.ID+"/comments", map[string]any{"content": "Race-safe project snapshot"}), "id", issue.ID)).Want(http.StatusCreated)
	receive(outwebhook.EventCommentCreated)

	testutil.Call(t, h.UpdateIssue,
		withURLParam(newRequest(http.MethodPatch, "/api/issues/"+issue.ID, map[string]any{"project_id": projectA, "suppress_run": true}), "id", issue.ID)).Want(http.StatusOK)
	receive(outwebhook.EventIssueProjectChanged)

	testutil.Call(t, h.UpdateIssue,
		withURLParam(newRequest(http.MethodPatch, "/api/issues/"+issue.ID, map[string]any{"project_id": nil, "suppress_run": true}), "id", issue.ID)).Want(http.StatusOK)
	receive(outwebhook.EventIssueProjectChanged)

	deadline := time.Now().Add(5 * time.Second)
	for {
		var succeeded int
		if err := testPool.QueryRow(context.Background(), `
			SELECT count(*) FROM outbound_webhook_delivery
			WHERE subscription_id = $1 AND state = 'succeeded'
		`, created.Subscription.ID).Scan(&succeeded); err != nil {
			t.Fatal(err)
		}
		if succeeded == 5 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("project-scoped multi-event deliveries persisted succeeded = %d, want 5", succeeded)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var history outwebhook.DeliveryPage
	historyRequest := withURLParams(newRequest(http.MethodGet, "/deliveries?limit=10", nil),
		"id", testWorkspaceID, "subscriptionId", created.Subscription.ID)
	testutil.Call(t, h.ListOutboundWebhookDeliveries, historyRequest).Want(http.StatusOK).JSON(&history)
	if history.Total != 5 || len(history.Deliveries) != 5 {
		t.Fatalf("project-scoped successful history = %+v, want 5 persisted deliveries", history)
	}
	for _, delivery := range history.Deliveries {
		if delivery.State != "succeeded" || delivery.ResponseStatus == nil || *delivery.ResponseStatus != http.StatusNoContent {
			t.Fatalf("project-scoped successful history row = %+v", delivery)
		}
	}

	var projectBIssue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue,
		newRequest(http.MethodPost, "/api/issues", map[string]any{"title": "Second selected project", "project_id": projectB})).Want(http.StatusCreated).JSON(&projectBIssue)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, projectBIssue.ID)
	})
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE subscription_id = $1 AND event_type = 'issue.created'`, created.Subscription.ID); count != 0 {
		t.Fatalf("single-project scope matched another project: %d deliveries", count)
	}
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
	rows, err := captureQueries.ListActiveOutboundWebhookSubscriptionsForEvent(context.Background(), db.ListActiveOutboundWebhookSubscriptionsForEventParams{
		WorkspaceID: workspaceID,
		EventType:   outwebhook.EventIssueCreated,
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("lock subscription for capture: rows=%d err=%v", len(rows), err)
	}
	eventID, _ := util.ParseUUID(uuid.NewString())
	if _, err := captureQueries.CreateOutboundWebhookDelivery(context.Background(), db.CreateOutboundWebhookDeliveryParams{
		EventID: eventID, SubscriptionID: subscriptionID, WorkspaceID: workspaceID,
		EventType: outwebhook.EventIssueCreated, RequestBody: []byte(`{"version":1}`), MaxPending: 1,
		SigningSecretCiphertext: []byte("test-only"), DestinationCiphertext: []byte("test-only"), SecretVersion: 1,
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
	if _, err := captureQueries.ListActiveOutboundWebhookSubscriptionsForEvent(context.Background(), db.ListActiveOutboundWebhookSubscriptionsForEventParams{
		WorkspaceID: workspaceID,
		EventType:   outwebhook.EventIssueCreated,
	}); err != nil {
		t.Fatal(err)
	}
	eventID, _ := util.ParseUUID(uuid.NewString())
	if _, err := captureQueries.CreateOutboundWebhookDelivery(context.Background(), db.CreateOutboundWebhookDeliveryParams{
		EventID: eventID, SubscriptionID: subscriptionID, WorkspaceID: workspaceID,
		EventType: outwebhook.EventIssueCreated, RequestBody: []byte(`{"version":1}`), MaxPending: 1,
		SigningSecretCiphertext: []byte("test-only"), DestinationCiphertext: []byte("test-only"), SecretVersion: 1,
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
	var listed struct {
		Subscriptions       []outwebhook.Subscription `json:"subscriptions"`
		CapabilityAvailable bool                      `json:"capability_available"`
	}
	testutil.Call(t, h.ListOutboundWebhooks,
		withURLParam(newRequest(http.MethodGet, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", nil), "id", testWorkspaceID)).Want(http.StatusOK).JSON(&listed)
	if listed.CapabilityAvailable || len(listed.Subscriptions) != 0 {
		t.Fatalf("unavailable capability response was unsafe: %+v", listed)
	}
}

func TestOutboundWebhookLifecycleKeepsSecretsOneTimeAndTestsThroughPersistedDelivery(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	received := make(chan receivedOutboundWebhook, 1)
	receiver := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- receivedOutboundWebhook{body: body, header: r.Header.Clone()}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	queries := db.New(testPool)
	bus := events.New()
	observer := newOutboundWebhookObserver()
	outbound := outwebhook.New(queries, testPool, box, []string{receiver.URL}, outwebhook.WithHTTPClient(receiver.Client()), outwebhook.WithObserver(observer))
	h := New(queries, testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = outbound

	var created struct {
		Subscription  outwebhook.Subscription `json:"subscription"`
		SigningSecret string                  `json:"signing_secret"`
	}
	testutil.Call(t, h.CreateOutboundWebhook,
		withURLParam(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", map[string]any{
			"name": "Lifecycle receiver", "destination": receiver.URL + "/events",
			"events": []string{"issue.created"}, "scope_mode": "workspace",
		}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&created)
	subscriptionID := created.Subscription.ID
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id = $1`, subscriptionID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id = $1`, subscriptionID)
		outbound.Close()
		outbound.WaitWithTimeout(time.Second)
	})

	params := func(method, suffix string, body any) *http.Request {
		return withURLParams(newRequest(method, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+subscriptionID+suffix, body),
			"id", testWorkspaceID, "subscriptionId", subscriptionID)
	}
	var paused outwebhook.Subscription
	testutil.Call(t, h.PauseOutboundWebhook, params(http.MethodPost, "/pause", nil)).Want(http.StatusOK).JSON(&paused)
	if paused.Status != "paused" || paused.PauseReason == nil || *paused.PauseReason != "manual" {
		t.Fatalf("manual pause was not represented safely: %+v", paused)
	}
	testutil.Call(t, h.PauseOutboundWebhook, params(http.MethodPost, "/pause", nil)).Want(http.StatusOK)
	if got := observer.snapshot()["subscription_paused"]; got != 1 {
		t.Fatalf("manual pause metric count = %d after idempotent pause, want 1", got)
	}

	var tested outwebhook.TestResult
	testutil.Call(t, h.TestOutboundWebhook, params(http.MethodPost, "/test", nil)).Want(http.StatusAccepted).JSON(&tested)
	if tested.DeliveryID == "" || tested.EventID == "" || tested.State != "pending" {
		t.Fatalf("synthetic delivery was not persisted: %+v", tested)
	}

	var rotated struct {
		Subscription  outwebhook.Subscription `json:"subscription"`
		SigningSecret string                  `json:"signing_secret"`
	}
	testutil.Call(t, h.RotateOutboundWebhookSecret, params(http.MethodPost, "/rotate-secret", nil)).Want(http.StatusOK).JSON(&rotated)
	if rotated.SigningSecret == "" || rotated.SigningSecret == created.SigningSecret || rotated.Subscription.SecretVersion != 2 {
		t.Fatalf("rotation did not disclose exactly one new version: %+v", rotated)
	}

	listBody := testutil.Call(t, h.ListOutboundWebhooks,
		withURLParam(newRequest(http.MethodGet, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks", nil), "id", testWorkspaceID)).Want(http.StatusOK).Text()
	if strings.Contains(listBody, created.SigningSecret) || strings.Contains(listBody, rotated.SigningSecret) || strings.Contains(listBody, "/events") {
		t.Fatalf("normal reads exposed a secret or destination path: %s", listBody)
	}

	outbound.Register(bus)
	var got receivedOutboundWebhook
	select {
	case got = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("paused subscription test did not use the normal dispatcher")
	}
	if got.header.Get("X-Multica-Event") != outwebhook.EventWebhookTest || got.header.Get("X-Multica-Delivery-ID") != tested.DeliveryID {
		t.Fatalf("unexpected synthetic request: event=%q delivery=%q", got.header.Get("X-Multica-Event"), got.header.Get("X-Multica-Delivery-ID"))
	}
	signatureFor := func(secret string) string {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(got.header.Get("X-Multica-Timestamp")))
		mac.Write([]byte("."))
		mac.Write(got.body)
		return "v1=" + hex.EncodeToString(mac.Sum(nil))
	}
	if got.header.Get("X-Multica-Signature") != signatureFor(created.SigningSecret) || got.header.Get("X-Multica-Signature") == signatureFor(rotated.SigningSecret) {
		t.Fatal("automatic delivery did not retain the signing secret version captured with its body")
	}

	var issue struct {
		ID string `json:"id"`
	}
	testutil.Call(t, h.CreateIssue, newRequest(http.MethodPost, "/api/issues", map[string]any{"title": "Paused webhook must not capture"})).Want(http.StatusCreated).JSON(&issue)
	t.Cleanup(func() { _, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID) })
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE subscription_id = $1 AND event_type = 'issue.created'`, subscriptionID); count != 0 {
		t.Fatalf("paused subscription captured %d normal deliveries", count)
	}

	if _, err := testPool.Exec(context.Background(), `UPDATE outbound_webhook_subscription SET pause_reason = 'scope_empty' WHERE id = $1`, subscriptionID); err != nil {
		t.Fatal(err)
	}
	testutil.Call(t, h.ResumeOutboundWebhook, params(http.MethodPost, "/resume", nil)).Want(http.StatusBadRequest)
	if _, err := testPool.Exec(context.Background(), `UPDATE outbound_webhook_subscription SET pause_reason = 'manual' WHERE id = $1`, subscriptionID); err != nil {
		t.Fatal(err)
	}
	var resumed outwebhook.Subscription
	testutil.Call(t, h.ResumeOutboundWebhook, params(http.MethodPost, "/resume", nil)).Want(http.StatusOK).JSON(&resumed)
	if resumed.Status != "active" || resumed.PauseReason != nil {
		t.Fatalf("subscription did not resume: %+v", resumed)
	}
	allEvents := []string{"issue.created", "issue.status_changed", "issue.assignee_changed", "issue.priority_changed", "issue.project_changed", "comment.created", "comment.updated", "comment.deleted"}
	var updated outwebhook.Subscription
	testutil.Call(t, h.UpdateOutboundWebhook, params(http.MethodPut, "", map[string]any{
		"name": "Updated lifecycle receiver", "events": allEvents, "scope_mode": "workspace",
	})).Want(http.StatusOK).JSON(&updated)
	if updated.Name != "Updated lifecycle receiver" || len(updated.Events) != len(allEvents) {
		t.Fatalf("configuration update lost explicit event selection: %+v", updated)
	}

	foreignID := uuid.NewString()
	foreignRequest := withURLParams(newRequest(http.MethodPost, "/api/workspaces/"+testWorkspaceID+"/outbound-webhooks/"+foreignID+"/pause", nil),
		"id", testWorkspaceID, "subscriptionId", foreignID)
	testutil.Call(t, h.PauseOutboundWebhook, foreignRequest).Want(http.StatusNotFound)

	testutil.Call(t, h.DeleteOutboundWebhook, params(http.MethodDelete, "", nil)).Want(http.StatusNoContent)
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE subscription_id = $1`, subscriptionID); count != 0 {
		t.Fatalf("delete left %d retained deliveries", count)
	}
}

func TestOutboundWebhookHistoryRedeliveryAndRetention(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database unavailable")
	}
	type received struct {
		body   []byte
		header http.Header
	}
	firstRequests := make(chan received, 2)
	first := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read first receiver request: %v", err)
			return
		}
		firstRequests <- received{body, r.Header.Clone()}
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write([]byte("token=receiver-secret " + strings.Repeat("界", 600))); err != nil {
			t.Errorf("write first receiver response: %v", err)
		}
	}))
	defer first.Close()
	secondRequests := make(chan received, 2)
	second := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read second receiver request: %v", err)
			return
		}
		secondRequests <- received{body, r.Header.Clone()}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer second.Close()
	box, err := secretbox.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	queries, bus := db.New(testPool), events.New()
	client := first.Client()
	firstTransport := client.Transport
	secondTransport := second.Client().Transport
	client.Transport = outboundWebhookRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if strings.HasPrefix(request.URL.String(), second.URL) {
			return secondTransport.RoundTrip(request)
		}
		return firstTransport.RoundTrip(request)
	})
	observer := newOutboundWebhookObserver()
	outbound := outwebhook.New(queries, testPool, box, []string{first.URL, second.URL}, outwebhook.WithHTTPClient(client), outwebhook.WithHistoryPolicy(30*24*time.Hour, time.Hour), outwebhook.WithObserver(observer))
	outbound.Register(bus)
	t.Cleanup(func() { outbound.Close(); outbound.WaitWithTimeout(time.Second) })
	h := New(queries, testPool, testHandler.Hub, bus, service.NewEmailService(), nil, nil, analytics.NoopClient{}, Config{AllowSignup: true})
	h.OutboundWebhooks = outbound
	var created struct {
		Subscription  outwebhook.Subscription `json:"subscription"`
		SigningSecret string                  `json:"signing_secret"`
	}
	testutil.Call(t, h.CreateOutboundWebhook, withURLParam(newRequest(http.MethodPost, "/create", map[string]any{
		"name": "History receiver", "destination": first.URL + "/events", "events": []string{"issue.created"}, "scope_mode": "workspace",
	}), "id", testWorkspaceID)).Want(http.StatusCreated).JSON(&created)
	subscriptionID := created.Subscription.ID
	t.Cleanup(func() {
		if _, err := testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_delivery WHERE subscription_id=$1`, subscriptionID); err != nil {
			t.Errorf("delete history test deliveries: %v", err)
		}
		if _, err := testPool.Exec(context.Background(), `DELETE FROM outbound_webhook_subscription WHERE id=$1`, subscriptionID); err != nil {
			t.Errorf("delete history test subscription: %v", err)
		}
	})
	createReliabilityWebhookIssue(t, h, "History and redelivery")
	var original received
	select {
	case original = <-firstRequests:
	case <-time.After(5 * time.Second):
		t.Fatal("original delivery was not received")
	}
	originalID := original.header.Get("X-Multica-Delivery-ID")
	waitOutboundWebhookDeliveryState(t, originalID, "failed")
	params := func(method, suffix string, body any) *http.Request {
		return withURLParams(newRequest(method, suffix, body), "id", testWorkspaceID, "subscriptionId", subscriptionID)
	}
	memberID := addSecondWorkspaceMember(t, "outbound-history-member@multica.test")
	memberParams := func(method, suffix string) *http.Request {
		return withURLParams(newRequestAs(memberID, method, suffix, nil), "id", testWorkspaceID, "subscriptionId", subscriptionID, "deliveryId", originalID)
	}
	testutil.Call(t, h.ListOutboundWebhookDeliveries, memberParams(http.MethodGet, "/deliveries?limit=1")).Want(http.StatusForbidden)
	testutil.Call(t, h.GetOutboundWebhookDelivery, memberParams(http.MethodGet, "/detail")).Want(http.StatusForbidden)
	var page outwebhook.DeliveryPage
	testutil.Call(t, h.ListOutboundWebhookDeliveries, params(http.MethodGet, "/deliveries?limit=1", nil)).Want(http.StatusOK).JSON(&page)
	if len(page.Deliveries) != 1 || page.Total != 1 || page.Deliveries[0].ResponseExcerpt == nil {
		t.Fatalf("unexpected history: %+v", page)
	}
	if len([]rune(*page.Deliveries[0].ResponseExcerpt)) > 512 || strings.Contains(*page.Deliveries[0].ResponseExcerpt, "receiver-secret") {
		t.Fatalf("unsafe excerpt: %q", *page.Deliveries[0].ResponseExcerpt)
	}
	detailRequest := withURLParams(newRequest(http.MethodGet, "/detail", nil), "id", testWorkspaceID, "subscriptionId", subscriptionID, "deliveryId", originalID)
	detailBody := testutil.Call(t, h.GetOutboundWebhookDelivery, detailRequest).Want(http.StatusOK).Text()
	for _, forbidden := range []string{"request_body", "destination_ciphertext", "signing_secret", "receiver-secret"} {
		if strings.Contains(detailBody, forbidden) {
			t.Fatalf("detail exposed %q", forbidden)
		}
	}
	testutil.Call(t, h.PauseOutboundWebhook, params(http.MethodPost, "/pause", nil)).Want(http.StatusOK)
	redeliveryRequest := func() *http.Request {
		return withURLParams(newRequest(http.MethodPost, "/redeliver", nil), "id", testWorkspaceID, "subscriptionId", subscriptionID, "deliveryId", originalID)
	}
	testutil.Call(t, h.RedeliverOutboundWebhookDelivery, redeliveryRequest()).Want(http.StatusConflict)
	testutil.Call(t, h.ResumeOutboundWebhook, params(http.MethodPost, "/resume", nil)).Want(http.StatusOK)
	testutil.Call(t, h.UpdateOutboundWebhook, params(http.MethodPut, "/update", map[string]any{"name": "History receiver", "destination": second.URL + "/events", "events": []string{"issue.created"}, "scope_mode": "workspace"})).Want(http.StatusOK)
	var rotated struct {
		SigningSecret string `json:"signing_secret"`
	}
	testutil.Call(t, h.RotateOutboundWebhookSecret, params(http.MethodPost, "/rotate", nil)).Want(http.StatusOK).JSON(&rotated)
	var redelivery outwebhook.Delivery
	testutil.Call(t, h.RedeliverOutboundWebhookDelivery, redeliveryRequest()).Want(http.StatusAccepted).JSON(&redelivery)
	if redelivery.ID == originalID || redelivery.RedeliveryOf == nil || *redelivery.RedeliveryOf != originalID || redelivery.EventID != original.header.Get("X-Multica-Event-ID") {
		t.Fatalf("wrong linkage: %+v", redelivery)
	}
	var replay received
	select {
	case replay = <-secondRequests:
	case <-time.After(5 * time.Second):
		t.Fatal("redelivery did not use current destination")
	}
	if string(replay.body) != string(original.body) || replay.header.Get("X-Multica-Delivery-ID") != redelivery.ID {
		t.Fatal("redelivery body or ID changed incorrectly")
	}
	mac := hmac.New(sha256.New, []byte(rotated.SigningSecret))
	mac.Write([]byte(replay.header.Get("X-Multica-Timestamp")))
	mac.Write([]byte("."))
	mac.Write(replay.body)
	if replay.header.Get("X-Multica-Signature") != "v1="+hex.EncodeToString(mac.Sum(nil)) {
		t.Fatal("redelivery did not use current signing secret")
	}
	if _, err := testPool.Exec(context.Background(), `UPDATE outbound_webhook_delivery SET state='succeeded', completed_at=now()-interval '31 days' WHERE id=$1`, originalID); err != nil {
		t.Fatal(err)
	}
	outbound.RunMaintenance(context.Background())
	if !observer.recorded("cleanup_succeeded") || !observer.recorded("redelivery_created") || !observer.observedOldestPending() {
		t.Fatalf("history maintenance metrics were not observed: %+v", observer.snapshot())
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM outbound_webhook_delivery WHERE id=$1`, originalID); count != 0 {
		t.Fatalf("retention left %d rows", count)
	}
}
