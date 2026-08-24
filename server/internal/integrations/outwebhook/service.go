package outwebhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
	"github.com/multica-ai/multica/server/pkg/remotemcp"
)

const (
	EventIssueCreated = "issue.created"
	ScopeWorkspace    = "workspace"
	CatalogVersion    = 1
	maxResponseBytes  = 64 << 10
	deliveryTimeout   = 10 * time.Second
	dispatchWorkers   = 4
	dispatchQueueSize = 64
)

var (
	ErrUnavailable  = errors.New("outbound webhooks are unavailable")
	ErrInvalidInput = errors.New("invalid outbound webhook input")
)

type Service struct {
	queries   *db.Queries
	txStarter interface {
		Begin(context.Context) (pgx.Tx, error)
	}
	box            *secretbox.Box
	allowedOrigins map[string]struct{}
	clientFor      func(*url.URL, bool) *http.Client
	now            func() time.Time
	dispatchQueue  chan pendingDispatch
	dispatchCancel context.CancelFunc
	dispatchWG     sync.WaitGroup
	startOnce      sync.Once
	closeOnce      sync.Once
}

type pendingDispatch struct {
	subscription db.OutboundWebhookSubscription
	delivery     db.OutboundWebhookDelivery
}

type Option func(*Service)

// WithHTTPClient is intended for a controlled receiver in integration tests.
// Destination validation still runs before this client is selected.
func WithHTTPClient(client *http.Client) Option {
	return func(service *Service) {
		service.clientFor = func(*url.URL, bool) *http.Client { return client }
	}
}

type CreateInput struct {
	WorkspaceID pgtype.UUID
	CreatedBy   pgtype.UUID
	Name        string
	Destination string
	Events      []string
}

type Subscription struct {
	ID                  string    `json:"id"`
	WorkspaceID         string    `json:"workspace_id"`
	Name                string    `json:"name"`
	DestinationHint     string    `json:"destination_hint"`
	Events              []string  `json:"events"`
	EventCatalogVersion int32     `json:"event_catalog_version"`
	ScopeMode           string    `json:"scope_mode"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type CreateResult struct {
	Subscription  Subscription `json:"subscription"`
	SigningSecret string       `json:"signing_secret"`
}

func New(queries *db.Queries, txStarter interface {
	Begin(context.Context) (pgx.Tx, error)
}, box *secretbox.Box, allowedOrigins []string, options ...Option) *Service {
	allowed := make(map[string]struct{}, len(allowedOrigins))
	for _, raw := range allowedOrigins {
		if origin := canonicalOrigin(raw); origin != "" {
			allowed[origin] = struct{}{}
		}
	}
	service := &Service{
		queries:        queries,
		txStarter:      txStarter,
		box:            box,
		allowedOrigins: allowed,
		clientFor: func(endpoint *url.URL, operatorAllowed bool) *http.Client {
			if operatorAllowed {
				return &http.Client{Timeout: deliveryTimeout, CheckRedirect: rejectRedirect}
			}
			client := remotemcp.NewSecureHTTPClient(endpoint)
			client.Timeout = deliveryTimeout
			return client
		},
		now:           time.Now,
		dispatchQueue: make(chan pendingDispatch, dispatchQueueSize),
	}
	for _, option := range options {
		option(service)
	}
	return service
}

func (s *Service) Available() bool { return s != nil && s.box != nil }

func (s *Service) Register(bus *events.Bus) {
	if s == nil || bus == nil {
		return
	}
	if s.Available() {
		s.startDispatcher()
	}
	bus.Subscribe(protocol.EventIssueCreated, s.captureIssueCreated)
}

func (s *Service) startDispatcher() {
	s.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.dispatchCancel = cancel
		for range dispatchWorkers {
			s.dispatchWG.Add(1)
			go func() {
				defer s.dispatchWG.Done()
				for {
					select {
					case item := <-s.dispatchQueue:
						if ctx.Err() != nil {
							return
						}
						s.deliver(ctx, item.subscription, item.delivery)
					case <-ctx.Done():
						return
					}
				}
			}()
		}
	})
}

// Close cancels queued and active immediate delivery work. Every queued item is
// already durable; the leased recovery dispatcher introduced in #12 can resume
// any row that remains pending after shutdown.
func (s *Service) Close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		if s.dispatchCancel != nil {
			s.dispatchCancel()
		}
	})
}

func (s *Service) WaitWithTimeout(timeout time.Duration) bool {
	if s == nil {
		return true
	}
	done := make(chan struct{})
	go func() {
		s.dispatchWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *Service) Create(ctx context.Context, input CreateInput) (CreateResult, error) {
	if !s.Available() {
		return CreateResult{}, ErrUnavailable
	}
	input.Name = strings.TrimSpace(input.Name)
	if len(input.Name) < 1 || len(input.Name) > 100 {
		return CreateResult{}, fmt.Errorf("%w: name must contain 1 to 100 characters", ErrInvalidInput)
	}
	if len(input.Events) != 1 || input.Events[0] != EventIssueCreated {
		return CreateResult{}, fmt.Errorf("%w: event selection must contain issue.created", ErrInvalidInput)
	}
	endpoint, _, err := s.validateDestination(ctx, input.Destination)
	if err != nil {
		return CreateResult{}, fmt.Errorf("%w: destination is not allowed", ErrInvalidInput)
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return CreateResult{}, fmt.Errorf("generate signing secret: %w", err)
	}
	secret := "whsec_" + base64.RawURLEncoding.EncodeToString(secretBytes)
	destinationCiphertext, err := s.box.Seal([]byte(endpoint.String()))
	if err != nil {
		return CreateResult{}, fmt.Errorf("encrypt destination: %w", err)
	}
	secretCiphertext, err := s.box.Seal([]byte(secret))
	if err != nil {
		return CreateResult{}, fmt.Errorf("encrypt signing secret: %w", err)
	}
	eventsJSON, _ := json.Marshal(input.Events)
	row, err := s.queries.CreateOutboundWebhookSubscription(ctx, db.CreateOutboundWebhookSubscriptionParams{
		WorkspaceID: input.WorkspaceID, Name: input.Name,
		DestinationCiphertext: destinationCiphertext, SecretCiphertext: secretCiphertext,
		DestinationHint: safeDestinationHint(endpoint), Events: eventsJSON, CreatedBy: input.CreatedBy,
	})
	if err != nil {
		return CreateResult{}, err
	}
	return CreateResult{Subscription: subscriptionResponse(row), SigningSecret: secret}, nil
}

func (s *Service) List(ctx context.Context, workspaceID pgtype.UUID) ([]Subscription, error) {
	rows, err := s.queries.ListOutboundWebhookSubscriptions(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	result := make([]Subscription, 0, len(rows))
	for _, row := range rows {
		result = append(result, subscriptionResponse(row))
	}
	return result, nil
}

func (s *Service) Get(ctx context.Context, workspaceID, id pgtype.UUID) (Subscription, error) {
	row, err := s.queries.GetOutboundWebhookSubscription(ctx, db.GetOutboundWebhookSubscriptionParams{WorkspaceID: workspaceID, ID: id})
	if err != nil {
		return Subscription{}, err
	}
	return subscriptionResponse(row), nil
}

func (s *Service) Delete(ctx context.Context, workspaceID, id pgtype.UUID) (bool, error) {
	if s.txStarter == nil {
		return false, errors.New("outbound webhook transaction store is unavailable")
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	queries := s.queries.WithTx(tx)
	if _, err := queries.GetOutboundWebhookSubscriptionForUpdate(ctx, db.GetOutboundWebhookSubscriptionForUpdateParams{WorkspaceID: workspaceID, ID: id}); err != nil {
		return false, err
	}
	if err := queries.DeleteOutboundWebhookDeliveriesBySubscription(ctx, db.DeleteOutboundWebhookDeliveriesBySubscriptionParams{WorkspaceID: workspaceID, SubscriptionID: id}); err != nil {
		return false, err
	}
	count, err := queries.DeleteOutboundWebhookSubscription(ctx, db.DeleteOutboundWebhookSubscriptionParams{WorkspaceID: workspaceID, ID: id})
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return count == 1, nil
}

func (s *Service) captureIssueCreated(event events.Event) {
	if !s.Available() || s.txStarter == nil {
		return
	}
	workspaceID, err := util.ParseUUID(event.WorkspaceID)
	if err != nil {
		slog.Error("outbound webhook capture rejected invalid workspace", "event_type", EventIssueCreated)
		return
	}
	ctx := context.Background()
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		slog.Error("outbound webhook capture transaction failed", "event_type", EventIssueCreated, "error", err)
		return
	}
	defer tx.Rollback(ctx)
	queries := s.queries.WithTx(tx)
	if _, err := queries.LockWorkspaceForOutboundWebhookCapture(ctx, workspaceID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Error("outbound webhook workspace capture lock failed", "event_type", EventIssueCreated, "error", err)
		}
		return
	}
	rows, err := queries.ListActiveIssueCreatedOutboundWebhookSubscriptions(ctx, workspaceID)
	if err != nil {
		slog.Error("outbound webhook subscription lookup failed", "event_type", EventIssueCreated, "error", err)
		return
	}
	if len(rows) == 0 {
		_ = tx.Rollback(ctx)
		return
	}
	eventID := uuid.New()
	body, err := s.issueCreatedBody(event, eventID)
	if err != nil {
		slog.Error("outbound webhook canonicalization failed", "event_type", EventIssueCreated, "error", err)
		return
	}
	pending := make([]pendingDispatch, 0, len(rows))
	for _, subscription := range rows {
		delivery, err := queries.CreateOutboundWebhookDelivery(ctx, db.CreateOutboundWebhookDeliveryParams{
			EventID: pgtype.UUID{Bytes: eventID, Valid: true}, SubscriptionID: subscription.ID,
			WorkspaceID: subscription.WorkspaceID, EventType: EventIssueCreated, RequestBody: body,
		})
		if err != nil {
			slog.Error("outbound webhook delivery persistence failed", "event_type", EventIssueCreated, "error", err)
			return
		}
		pending = append(pending, pendingDispatch{subscription: subscription, delivery: delivery})
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("outbound webhook delivery commit failed", "event_type", EventIssueCreated, "error", err)
		return
	}
	for _, item := range pending {
		select {
		case s.dispatchQueue <- item:
		default:
			slog.Warn("outbound webhook immediate dispatch queue full", "delivery_id", util.UUIDToString(item.delivery.ID), "event_type", item.delivery.EventType)
		}
	}
}

func (s *Service) issueCreatedBody(event events.Event, eventID uuid.UUID) ([]byte, error) {
	payload, ok := event.Payload.(map[string]any)
	if !ok {
		return nil, errors.New("issue.created payload is not an object")
	}
	rawIssue, ok := payload["issue"]
	if !ok {
		return nil, errors.New("issue.created payload has no issue")
	}
	encoded, err := json.Marshal(rawIssue)
	if err != nil {
		return nil, err
	}
	var issue struct {
		ID           string  `json:"id"`
		Identifier   string  `json:"identifier"`
		Title        string  `json:"title"`
		Status       string  `json:"status"`
		Priority     string  `json:"priority"`
		ProjectID    *string `json:"project_id"`
		AssigneeType *string `json:"assignee_type"`
		AssigneeID   *string `json:"assignee_id"`
	}
	if err := json.Unmarshal(encoded, &issue); err != nil || issue.ID == "" || issue.Title == "" {
		return nil, errors.New("issue.created payload is incomplete")
	}
	envelope := struct {
		Version    int       `json:"version"`
		ID         string    `json:"id"`
		Type       string    `json:"type"`
		OccurredAt time.Time `json:"occurred_at"`
		Workspace  struct {
			ID string `json:"id"`
		} `json:"workspace"`
		Actor struct {
			Type string `json:"type"`
			ID   string `json:"id"`
		} `json:"actor"`
		Data struct {
			Issue any `json:"issue"`
		} `json:"data"`
	}{Version: 1, ID: eventID.String(), Type: EventIssueCreated, OccurredAt: s.now().UTC()}
	envelope.Workspace.ID = event.WorkspaceID
	envelope.Actor.Type, envelope.Actor.ID = event.ActorType, event.ActorID
	envelope.Data.Issue = issue
	return json.Marshal(envelope)
}

func (s *Service) deliver(ctx context.Context, subscription db.OutboundWebhookSubscription, delivery db.OutboundWebhookDelivery) {
	destination, err := s.box.Open(subscription.DestinationCiphertext)
	if err != nil {
		s.markFailed(delivery, 0, "encrypted destination is unavailable")
		return
	}
	secret, err := s.box.Open(subscription.SecretCiphertext)
	if err != nil {
		s.markFailed(delivery, 0, "encrypted signing secret is unavailable")
		return
	}
	endpoint, operatorAllowed, err := s.validateDestination(ctx, string(destination))
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.markFailed(delivery, 0, "destination is no longer allowed")
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint.String(), bytes.NewReader(delivery.RequestBody))
	if err != nil {
		s.markFailed(delivery, 0, "request could not be created")
		return
	}
	timestamp := strconv.FormatInt(s.now().Unix(), 10)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(delivery.RequestBody)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Multica-Webhook/1")
	req.Header.Set("X-Multica-Event", delivery.EventType)
	req.Header.Set("X-Multica-Event-ID", util.UUIDToString(delivery.EventID))
	req.Header.Set("X-Multica-Delivery-ID", util.UUIDToString(delivery.ID))
	req.Header.Set("X-Multica-Timestamp", timestamp)
	req.Header.Set("X-Multica-Signature", "v1="+hex.EncodeToString(mac.Sum(nil)))
	resp, err := s.clientFor(endpoint, operatorAllowed).Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		s.markFailed(delivery, 0, "receiver did not answer")
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.markFailed(delivery, resp.StatusCode, fmt.Sprintf("receiver returned HTTP %d", resp.StatusCode))
		return
	}
	if err := s.queries.MarkOutboundWebhookDeliverySucceeded(context.Background(), db.MarkOutboundWebhookDeliverySucceededParams{ID: delivery.ID, ResponseStatus: pgtype.Int4{Int32: int32(resp.StatusCode), Valid: true}}); err != nil {
		slog.Error("outbound webhook delivery state update failed", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType, "error", err)
	}
}

func (s *Service) markFailed(delivery db.OutboundWebhookDelivery, status int, reason string) {
	params := db.MarkOutboundWebhookDeliveryFailedParams{ID: delivery.ID, FailureReason: pgtype.Text{String: reason, Valid: true}}
	if status != 0 {
		params.ResponseStatus = pgtype.Int4{Int32: int32(status), Valid: true}
	}
	if err := s.queries.MarkOutboundWebhookDeliveryFailed(context.Background(), params); err != nil {
		slog.Error("outbound webhook delivery state update failed", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType, "error", err)
	}
}

func (s *Service) validateDestination(ctx context.Context, raw string) (*url.URL, bool, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, false, errors.New("destination must be HTTPS without user info, query, or fragment")
	}
	origin := canonicalOrigin(parsed.String())
	_, operatorAllowed := s.allowedOrigins[origin]
	if parsed.Port() != "" && parsed.Port() != "443" && !operatorAllowed {
		return nil, false, errors.New("destination uses an unsafe port")
	}
	if operatorAllowed {
		return parsed, true, nil
	}
	validated, err := remotemcp.ValidatePublicHTTPSEndpoint(ctx, parsed.String(), nil, nil)
	if err != nil {
		return nil, false, fmt.Errorf("destination is not public HTTPS: %w", err)
	}
	return validated, false, nil
}

func subscriptionResponse(row db.OutboundWebhookSubscription) Subscription {
	events := []string{}
	_ = json.Unmarshal(row.Events, &events)
	return Subscription{ID: util.UUIDToString(row.ID), WorkspaceID: util.UUIDToString(row.WorkspaceID), Name: row.Name, DestinationHint: row.DestinationHint, Events: events, EventCatalogVersion: row.EventCatalogVersion, ScopeMode: row.ScopeMode, CreatedAt: row.CreatedAt.Time, UpdatedAt: row.UpdatedAt.Time}
}

func safeDestinationHint(endpoint *url.URL) string { return endpoint.Scheme + "://" + endpoint.Host }

func canonicalOrigin(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return ""
	}
	return strings.ToLower(parsed.Scheme + "://" + parsed.Host)
}

func rejectRedirect(*http.Request, []*http.Request) error {
	return errors.New("outbound webhook redirects are not allowed")
}
