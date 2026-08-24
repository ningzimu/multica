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
	mathrand "math/rand/v2"
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
	EventIssueCreated         = "issue.created"
	EventIssueStatusChanged   = "issue.status_changed"
	EventIssueAssigneeChanged = "issue.assignee_changed"
	EventIssuePriorityChanged = "issue.priority_changed"
	EventIssueProjectChanged  = "issue.project_changed"
	EventCommentCreated       = "comment.created"
	EventCommentUpdated       = "comment.updated"
	EventCommentDeleted       = "comment.deleted"
	EventWebhookTest          = "webhook.test"
	ScopeWorkspace            = "workspace"
	ScopeProject              = "project"
	CatalogVersion            = 1

	maxCommentExcerptCodePoints = 500
	maxResponseBytes            = 64 << 10
	deliveryTimeout             = 10 * time.Second
	dispatchQueueSize           = 1
	hardMaxAttempts             = 32
	hardMaxWorkers              = 64
	hardMaxPerSub               = 16
	hardMaxPending              = 100_000
	hardMaxFailures             = 100
)

var eventCatalog = []string{
	EventIssueCreated,
	EventIssueStatusChanged,
	EventIssueAssigneeChanged,
	EventIssuePriorityChanged,
	EventIssueProjectChanged,
	EventCommentCreated,
	EventCommentUpdated,
	EventCommentDeleted,
}

var (
	ErrUnavailable          = errors.New("outbound webhooks are unavailable")
	ErrInvalidInput         = errors.New("invalid outbound webhook input")
	errCommentBodyUnchanged = errors.New("comment body is unchanged")
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
	policy         DeliveryPolicy
	jitter         func(time.Duration) time.Duration
	dispatchQueue  chan struct{}
	dispatchCancel context.CancelFunc
	dispatchWG     sync.WaitGroup
	startOnce      sync.Once
	closeOnce      sync.Once
}

type DeliveryPolicy struct {
	PollInterval                time.Duration
	LeaseDuration               time.Duration
	InitialBackoff              time.Duration
	MaxBackoff                  time.Duration
	MaxRetryAfter               time.Duration
	MaxAttempts                 int32
	GlobalConcurrency           int64
	SubscriptionConcurrency     int64
	MaxPendingPerSubscription   int64
	ConsecutiveFailureThreshold int32
}

type Option func(*Service)

// WithHTTPClient is intended for a controlled receiver in integration tests.
// Destination validation still runs before this client is selected.
func WithHTTPClient(client *http.Client) Option {
	return func(service *Service) {
		service.clientFor = func(*url.URL, bool) *http.Client { return client }
	}
}

func WithDeliveryPolicy(policy DeliveryPolicy) Option {
	return func(service *Service) {
		service.policy = normalizeDeliveryPolicy(policy)
	}
}

// WithRetryJitter makes retry scheduling deterministic in integration tests.
func WithRetryJitter(jitter func(time.Duration) time.Duration) Option {
	return func(service *Service) {
		if jitter != nil {
			service.jitter = jitter
		}
	}
}

func DefaultDeliveryPolicy() DeliveryPolicy {
	return DeliveryPolicy{
		PollInterval:                time.Second,
		LeaseDuration:               2 * time.Minute,
		InitialBackoff:              time.Second,
		MaxBackoff:                  time.Hour,
		MaxRetryAfter:               time.Hour,
		MaxAttempts:                 8,
		GlobalConcurrency:           4,
		SubscriptionConcurrency:     1,
		MaxPendingPerSubscription:   10_000,
		ConsecutiveFailureThreshold: 5,
	}
}

type CreateInput struct {
	WorkspaceID pgtype.UUID
	CreatedBy   pgtype.UUID
	Name        string
	Destination string
	Events      []string
	ScopeMode   string
	ProjectIDs  []pgtype.UUID
}

type UpdateInput struct {
	WorkspaceID pgtype.UUID
	ID          pgtype.UUID
	Name        string
	Destination *string
	Events      []string
	ScopeMode   string
	ProjectIDs  []pgtype.UUID
}

type TestInput struct {
	WorkspaceID pgtype.UUID
	ID          pgtype.UUID
	ActorID     pgtype.UUID
}

type Subscription struct {
	ID                          string    `json:"id"`
	WorkspaceID                 string    `json:"workspace_id"`
	Name                        string    `json:"name"`
	DestinationHint             string    `json:"destination_hint"`
	Events                      []string  `json:"events"`
	EventCatalogVersion         int32     `json:"event_catalog_version"`
	ScopeMode                   string    `json:"scope_mode"`
	ProjectIDs                  []string  `json:"project_ids"`
	Status                      string    `json:"status"`
	PauseReason                 *string   `json:"pause_reason"`
	ConsecutiveTerminalFailures int32     `json:"consecutive_terminal_failures"`
	SigningSecretHint           string    `json:"signing_secret_hint"`
	SecretVersion               int32     `json:"secret_version"`
	CreatedAt                   time.Time `json:"created_at"`
	UpdatedAt                   time.Time `json:"updated_at"`
}

type CreateResult struct {
	Subscription  Subscription `json:"subscription"`
	SigningSecret string       `json:"signing_secret"`
}

type TestResult struct {
	DeliveryID string `json:"delivery_id"`
	EventID    string `json:"event_id"`
	State      string `json:"state"`
}

type UpdateScopeInput struct {
	WorkspaceID    pgtype.UUID
	SubscriptionID pgtype.UUID
	ScopeMode      string
	ProjectIDs     []pgtype.UUID
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
		policy:        DefaultDeliveryPolicy(),
		jitter:        boundedJitter,
		dispatchQueue: make(chan struct{}, dispatchQueueSize),
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
	bus.Subscribe(protocol.EventIssueUpdated, s.captureIssueUpdated)
	bus.Subscribe(protocol.EventCommentCreated, func(event events.Event) { s.captureComment(event, EventCommentCreated) })
	bus.Subscribe(protocol.EventCommentUpdated, func(event events.Event) { s.captureComment(event, EventCommentUpdated) })
	bus.Subscribe(protocol.EventCommentDeleted, func(event events.Event) { s.captureComment(event, EventCommentDeleted) })
}

func (s *Service) startDispatcher() {
	s.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.dispatchCancel = cancel
		for range int(s.policy.GlobalConcurrency) {
			s.dispatchWG.Add(1)
			go func() {
				defer s.dispatchWG.Done()
				s.runDispatcher(ctx)
			}()
		}
		s.wakeDispatcher()
	})
}

func (s *Service) runDispatcher(ctx context.Context) {
	for ctx.Err() == nil {
		if s.dispatchOne(ctx) {
			continue
		}
		timer := time.NewTimer(s.policy.PollInterval)
		select {
		case <-s.dispatchQueue:
			stopTimer(timer)
		case <-timer.C:
		case <-ctx.Done():
			stopTimer(timer)
			return
		}
	}
}

func (s *Service) dispatchOne(ctx context.Context) bool {
	if s.queries == nil || s.txStarter == nil {
		return false
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("outbound webhook delivery claim transaction failed", "error", err)
		}
		return false
	}
	defer tx.Rollback(context.Background())
	if ctx.Err() != nil {
		return false
	}
	queries := s.queries.WithTx(tx)
	if err := queries.LockOutboundWebhookDeliveryClaim(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Error("outbound webhook delivery claim lock failed", "error", err)
		}
		return false
	}
	delivery, err := queries.ClaimDueOutboundWebhookDelivery(ctx, db.ClaimDueOutboundWebhookDeliveryParams{
		LeaseSeconds:               s.policy.LeaseDuration.Seconds(),
		MaxAttempts:                s.policy.MaxAttempts,
		MaxGlobalConcurrency:       s.policy.GlobalConcurrency,
		MaxSubscriptionConcurrency: s.policy.SubscriptionConcurrency,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		rows, settleErr := queries.FailExhaustedOutboundWebhookDelivery(ctx, db.FailExhaustedOutboundWebhookDeliveryParams{
			MaxAttempts:      s.policy.MaxAttempts,
			FailureThreshold: s.policy.ConsecutiveFailureThreshold,
		})
		if settleErr != nil && ctx.Err() == nil {
			slog.Error("outbound webhook exhausted delivery settlement failed", "error", settleErr)
		}
		if settleErr == nil && rows > 0 {
			if err := tx.Commit(ctx); err != nil {
				if ctx.Err() == nil {
					slog.Error("outbound webhook exhausted delivery commit failed", "error", err)
				}
				return false
			}
			return true
		}
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	if err != nil {
		slog.Error("outbound webhook delivery claim failed", "error", err)
		return false
	}
	if err := tx.Commit(ctx); err != nil {
		if ctx.Err() == nil {
			slog.Error("outbound webhook delivery claim commit failed", "error", err)
		}
		return false
	}
	subscription, err := s.queries.GetOutboundWebhookSubscription(ctx, db.GetOutboundWebhookSubscriptionParams{
		WorkspaceID: delivery.WorkspaceID,
		ID:          delivery.SubscriptionID,
	})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Error("outbound webhook claimed subscription lookup failed", "delivery_id", util.UUIDToString(delivery.ID), "error", err)
		}
		s.releaseClaim(delivery)
		return true
	}
	if subscription.Status != "active" && delivery.EventType != EventWebhookTest {
		s.releaseClaim(delivery)
		return true
	}
	s.deliver(ctx, subscription, delivery)
	return true
}

func (s *Service) wakeDispatcher() {
	select {
	case s.dispatchQueue <- struct{}{}:
	default:
	}
}

// Close cancels claimed delivery work. The database remains authoritative, so
// a released or expired lease is recovered after restart.
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
	if err := validateEventSelection(input.Events); err != nil {
		return CreateResult{}, err
	}
	if input.ScopeMode == "" {
		input.ScopeMode = ScopeWorkspace
	}
	if err := validateScopeShape(input.ScopeMode, input.ProjectIDs); err != nil {
		return CreateResult{}, err
	}
	if input.ProjectIDs == nil {
		input.ProjectIDs = []pgtype.UUID{}
	}
	endpoint, _, err := s.validateDestination(ctx, input.Destination)
	if err != nil {
		return CreateResult{}, fmt.Errorf("%w: destination is not allowed", ErrInvalidInput)
	}
	secret, err := generateSigningSecret()
	if err != nil {
		return CreateResult{}, err
	}
	destinationCiphertext, err := s.box.Seal([]byte(endpoint.String()))
	if err != nil {
		return CreateResult{}, fmt.Errorf("encrypt destination: %w", err)
	}
	secretCiphertext, err := s.box.Seal([]byte(secret))
	if err != nil {
		return CreateResult{}, fmt.Errorf("encrypt signing secret: %w", err)
	}
	eventsJSON, _ := json.Marshal(input.Events)
	if s.txStarter == nil {
		return CreateResult{}, errors.New("outbound webhook transaction store is unavailable")
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		return CreateResult{}, err
	}
	defer tx.Rollback(ctx)
	queries := s.queries.WithTx(tx)
	if err := lockScopeProjects(ctx, queries, input.WorkspaceID, input.ProjectIDs); err != nil {
		return CreateResult{}, err
	}
	row, err := queries.CreateOutboundWebhookSubscription(ctx, db.CreateOutboundWebhookSubscriptionParams{
		WorkspaceID: input.WorkspaceID, Name: input.Name,
		DestinationCiphertext: destinationCiphertext, SecretCiphertext: secretCiphertext,
		DestinationHint: safeDestinationHint(endpoint), Events: eventsJSON, ScopeMode: input.ScopeMode,
		ProjectIds: input.ProjectIDs, CreatedBy: input.CreatedBy,
		SigningSecretHint: signingSecretHint(secret),
	})
	if err != nil {
		return CreateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, err
	}
	return CreateResult{Subscription: subscriptionResponse(row), SigningSecret: secret}, nil
}

func generateSigningSecret() (string, error) {
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return "", fmt.Errorf("generate signing secret: %w", err)
	}
	return "whsec_" + base64.RawURLEncoding.EncodeToString(secretBytes), nil
}

func (s *Service) UpdateScope(ctx context.Context, input UpdateScopeInput) (Subscription, error) {
	if !s.Available() {
		return Subscription{}, ErrUnavailable
	}
	if err := validateScopeShape(input.ScopeMode, input.ProjectIDs); err != nil {
		return Subscription{}, err
	}
	if input.ProjectIDs == nil {
		input.ProjectIDs = []pgtype.UUID{}
	}
	if s.txStarter == nil {
		return Subscription{}, errors.New("outbound webhook transaction store is unavailable")
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer tx.Rollback(ctx)
	queries := s.queries.WithTx(tx)
	if err := lockScopeProjects(ctx, queries, input.WorkspaceID, input.ProjectIDs); err != nil {
		return Subscription{}, err
	}
	if _, err := queries.GetOutboundWebhookSubscriptionForUpdate(ctx, db.GetOutboundWebhookSubscriptionForUpdateParams{WorkspaceID: input.WorkspaceID, ID: input.SubscriptionID}); err != nil {
		return Subscription{}, err
	}
	row, err := queries.UpdateOutboundWebhookSubscriptionScope(ctx, db.UpdateOutboundWebhookSubscriptionScopeParams{
		WorkspaceID: input.WorkspaceID, ID: input.SubscriptionID, ScopeMode: input.ScopeMode, ProjectIds: input.ProjectIDs,
	})
	if err != nil {
		return Subscription{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Subscription{}, err
	}
	return subscriptionResponse(row), nil
}

func validateScopeShape(scopeMode string, projectIDs []pgtype.UUID) error {
	if scopeMode == ScopeWorkspace {
		if len(projectIDs) != 0 {
			return fmt.Errorf("%w: workspace scope cannot select projects", ErrInvalidInput)
		}
		return nil
	}
	if scopeMode != ScopeProject || len(projectIDs) == 0 {
		return fmt.Errorf("%w: project scope requires at least one project", ErrInvalidInput)
	}
	seen := make(map[uuid.UUID]struct{}, len(projectIDs))
	for _, id := range projectIDs {
		if !id.Valid {
			return fmt.Errorf("%w: invalid project id", ErrInvalidInput)
		}
		if _, duplicate := seen[id.Bytes]; duplicate {
			return fmt.Errorf("%w: duplicate project id", ErrInvalidInput)
		}
		seen[id.Bytes] = struct{}{}
	}
	return nil
}

func lockScopeProjects(ctx context.Context, queries *db.Queries, workspaceID pgtype.UUID, projectIDs []pgtype.UUID) error {
	if len(projectIDs) == 0 {
		return nil
	}
	rows, err := queries.LockOutboundWebhookScopeProjects(ctx, db.LockOutboundWebhookScopeProjectsParams{WorkspaceID: workspaceID, ProjectIds: projectIDs})
	if err != nil {
		return err
	}
	if len(rows) != len(projectIDs) {
		return fmt.Errorf("%w: project does not belong to workspace", ErrInvalidInput)
	}
	return nil
}

func signingSecretHint(secret string) string {
	if len(secret) < 4 {
		return "whsec_..."
	}
	return "whsec_..." + secret[len(secret)-4:]
}

func validateEventSelection(selected []string) error {
	if len(selected) == 0 {
		return fmt.Errorf("%w: select at least one event", ErrInvalidInput)
	}
	known := make(map[string]struct{}, len(eventCatalog))
	for _, eventType := range eventCatalog {
		known[eventType] = struct{}{}
	}
	seen := make(map[string]struct{}, len(selected))
	for _, eventType := range selected {
		if _, ok := known[eventType]; !ok {
			return fmt.Errorf("%w: unsupported event %q", ErrInvalidInput, eventType)
		}
		if _, duplicate := seen[eventType]; duplicate {
			return fmt.Errorf("%w: duplicate event %q", ErrInvalidInput, eventType)
		}
		seen[eventType] = struct{}{}
	}
	return nil
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

func (s *Service) Update(ctx context.Context, input UpdateInput) (Subscription, error) {
	if !s.Available() || s.txStarter == nil {
		return Subscription{}, ErrUnavailable
	}
	name := strings.TrimSpace(input.Name)
	if len(name) < 1 || len(name) > 100 {
		return Subscription{}, fmt.Errorf("%w: name must contain 1 to 100 characters", ErrInvalidInput)
	}
	if err := validateEventSelection(input.Events); err != nil {
		return Subscription{}, err
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer tx.Rollback(context.Background())
	queries := s.queries.WithTx(tx)
	if input.ProjectIDs != nil {
		if err := validateScopeShape(input.ScopeMode, input.ProjectIDs); err != nil {
			return Subscription{}, err
		}
		// Project deletion takes the project row before shrinking subscription
		// scopes. Use the same lock order for explicit scope changes.
		if err := lockScopeProjects(ctx, queries, input.WorkspaceID, input.ProjectIDs); err != nil {
			return Subscription{}, err
		}
	}
	existing, err := queries.GetOutboundWebhookSubscriptionForUpdate(ctx, db.GetOutboundWebhookSubscriptionForUpdateParams{
		WorkspaceID: input.WorkspaceID, ID: input.ID,
	})
	if err != nil {
		return Subscription{}, err
	}
	// Older clients built before Project Scope omit project_ids and send their
	// only known scope value. Preserve the stored scope in that case so editing
	// lifecycle fields can never widen an existing Project Scope.
	if input.ProjectIDs == nil {
		input.ScopeMode = existing.ScopeMode
		input.ProjectIDs = existing.ProjectIds
	}
	destinationCiphertext := existing.DestinationCiphertext
	destinationHint := existing.DestinationHint
	if input.Destination != nil {
		endpoint, _, err := s.validateDestination(ctx, *input.Destination)
		if err != nil {
			return Subscription{}, fmt.Errorf("%w: destination is not allowed", ErrInvalidInput)
		}
		destinationCiphertext, err = s.box.Seal([]byte(endpoint.String()))
		if err != nil {
			return Subscription{}, fmt.Errorf("encrypt destination: %w", err)
		}
		destinationHint = safeDestinationHint(endpoint)
	}
	eventsJSON, err := json.Marshal(input.Events)
	if err != nil {
		return Subscription{}, err
	}
	updated, err := queries.UpdateOutboundWebhookSubscription(ctx, db.UpdateOutboundWebhookSubscriptionParams{
		WorkspaceID: input.WorkspaceID, ID: input.ID, Name: name,
		DestinationCiphertext: destinationCiphertext, DestinationHint: destinationHint,
		Events: eventsJSON, ScopeMode: input.ScopeMode, ProjectIds: input.ProjectIDs,
	})
	if err != nil {
		return Subscription{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Subscription{}, err
	}
	return subscriptionResponse(updated), nil
}

func (s *Service) Pause(ctx context.Context, workspaceID, id pgtype.UUID) (Subscription, error) {
	if !s.Available() {
		return Subscription{}, ErrUnavailable
	}
	row, err := s.queries.PauseOutboundWebhookSubscription(ctx, db.PauseOutboundWebhookSubscriptionParams{WorkspaceID: workspaceID, ID: id})
	if err != nil {
		return Subscription{}, err
	}
	return subscriptionResponse(row), nil
}

func (s *Service) Resume(ctx context.Context, workspaceID, id pgtype.UUID) (Subscription, error) {
	if !s.Available() || s.txStarter == nil {
		return Subscription{}, ErrUnavailable
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		return Subscription{}, err
	}
	defer tx.Rollback(context.Background())
	queries := s.queries.WithTx(tx)
	row, err := queries.GetOutboundWebhookSubscriptionForUpdate(ctx, db.GetOutboundWebhookSubscriptionForUpdateParams{WorkspaceID: workspaceID, ID: id})
	if err != nil {
		return Subscription{}, err
	}
	if row.PauseReason.Valid && row.PauseReason.String == "scope_empty" {
		return Subscription{}, fmt.Errorf("%w: an empty project scope cannot be resumed", ErrInvalidInput)
	}
	row, err = queries.ResumeOutboundWebhookSubscription(ctx, db.ResumeOutboundWebhookSubscriptionParams{WorkspaceID: workspaceID, ID: id})
	if err != nil {
		return Subscription{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Subscription{}, err
	}
	s.wakeDispatcher()
	return subscriptionResponse(row), nil
}

func (s *Service) RotateSecret(ctx context.Context, workspaceID, id pgtype.UUID) (CreateResult, error) {
	if !s.Available() || s.txStarter == nil {
		return CreateResult{}, ErrUnavailable
	}
	secret, err := generateSigningSecret()
	if err != nil {
		return CreateResult{}, err
	}
	ciphertext, err := s.box.Seal([]byte(secret))
	if err != nil {
		return CreateResult{}, fmt.Errorf("encrypt signing secret: %w", err)
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		return CreateResult{}, err
	}
	defer tx.Rollback(context.Background())
	queries := s.queries.WithTx(tx)
	if _, err := queries.GetOutboundWebhookSubscriptionForUpdate(ctx, db.GetOutboundWebhookSubscriptionForUpdateParams{WorkspaceID: workspaceID, ID: id}); err != nil {
		return CreateResult{}, err
	}
	row, err := queries.RotateOutboundWebhookSigningSecret(ctx, db.RotateOutboundWebhookSigningSecretParams{
		WorkspaceID: workspaceID, ID: id, SecretCiphertext: ciphertext, SigningSecretHint: signingSecretHint(secret),
	})
	if err != nil {
		return CreateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, err
	}
	return CreateResult{Subscription: subscriptionResponse(row), SigningSecret: secret}, nil
}

func (s *Service) Test(ctx context.Context, input TestInput) (TestResult, error) {
	if !s.Available() || s.txStarter == nil {
		return TestResult{}, ErrUnavailable
	}
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		return TestResult{}, err
	}
	defer tx.Rollback(ctx)
	queries := s.queries.WithTx(tx)
	if _, err := queries.LockWorkspaceForOutboundWebhookCapture(ctx, input.WorkspaceID); err != nil {
		return TestResult{}, err
	}
	subscription, err := queries.GetOutboundWebhookSubscriptionForUpdate(ctx, db.GetOutboundWebhookSubscriptionForUpdateParams{WorkspaceID: input.WorkspaceID, ID: input.ID})
	if err != nil {
		return TestResult{}, err
	}
	eventID := uuid.New()
	body, err := json.Marshal(map[string]any{
		"version": 1, "id": eventID.String(), "type": EventWebhookTest, "occurred_at": s.now().UTC(),
		"workspace": map[string]string{"id": util.UUIDToString(input.WorkspaceID)},
		"actor":     map[string]string{"type": "member", "id": util.UUIDToString(input.ActorID)},
		"data":      map[string]bool{"synthetic": true},
	})
	if err != nil {
		return TestResult{}, err
	}
	delivery, err := queries.CreateOutboundWebhookDelivery(ctx, db.CreateOutboundWebhookDeliveryParams{
		EventID: pgtype.UUID{Bytes: eventID, Valid: true}, SubscriptionID: subscription.ID,
		WorkspaceID: subscription.WorkspaceID, EventType: EventWebhookTest, RequestBody: body,
		SigningSecretCiphertext: subscription.SecretCiphertext, DestinationCiphertext: subscription.DestinationCiphertext,
		SecretVersion: subscription.SecretVersion, MaxPending: s.policy.MaxPendingPerSubscription,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TestResult{}, fmt.Errorf("%w: pending delivery limit reached", ErrInvalidInput)
		}
		return TestResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TestResult{}, err
	}
	s.wakeDispatcher()
	return TestResult{DeliveryID: util.UUIDToString(delivery.ID), EventID: eventID.String(), State: delivery.State}, nil
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
	s.captureProductEvents(event, []productEvent{{eventType: EventIssueCreated}})
}

func (s *Service) captureIssueUpdated(event events.Event) {
	payload, ok := event.Payload.(map[string]any)
	if !ok {
		slog.Error("outbound webhook canonicalization failed", "event_type", "issue changes", "error", "issue:updated payload is not an object")
		return
	}
	changes, err := issueProductEvents(payload)
	if err != nil {
		slog.Error("outbound webhook canonicalization failed", "event_type", "issue changes", "error", err)
		return
	}
	if len(changes) > 0 {
		s.captureProductEvents(event, changes)
	}
}

func issueProductEvents(payload map[string]any) ([]productEvent, error) {
	changes := make([]productEvent, 0, 4)
	for _, candidate := range []struct {
		flag      string
		eventType string
		field     string
		previous  []string
	}{
		{flag: "status_changed", eventType: EventIssueStatusChanged, field: "status", previous: []string{"prev_status"}},
		{flag: "assignee_changed", eventType: EventIssueAssigneeChanged, field: "assignee", previous: []string{"prev_assignee_type", "prev_assignee_id"}},
		{flag: "priority_changed", eventType: EventIssuePriorityChanged, field: "priority", previous: []string{"prev_priority"}},
		{flag: "project_changed", eventType: EventIssueProjectChanged, field: "project", previous: []string{"prev_project_id"}},
	} {
		changed, _ := payload[candidate.flag].(bool)
		if !changed {
			continue
		}
		missing := false
		for _, key := range candidate.previous {
			if _, exists := payload[key]; !exists {
				missing = true
				break
			}
		}
		if missing {
			return nil, fmt.Errorf("%s payload has no previous value", candidate.eventType)
		}
		changes = append(changes, productEvent{eventType: candidate.eventType, changeField: candidate.field})
	}
	return changes, nil
}

type productEvent struct {
	eventType   string
	changeField string
}

func (s *Service) captureProductEvents(event events.Event, productEvents []productEvent) {
	if !s.Available() || s.txStarter == nil {
		return
	}
	workspaceID, err := util.ParseUUID(event.WorkspaceID)
	if err != nil {
		slog.Error("outbound webhook capture rejected invalid workspace", "event_type", event.Type)
		return
	}
	ctx := context.Background()
	tx, err := s.txStarter.Begin(ctx)
	if err != nil {
		slog.Error("outbound webhook capture transaction failed", "event_type", event.Type, "error", err)
		return
	}
	defer tx.Rollback(ctx)
	queries := s.queries.WithTx(tx)
	if _, err := queries.LockWorkspaceForOutboundWebhookCapture(ctx, workspaceID); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Error("outbound webhook workspace capture lock failed", "event_type", event.Type, "error", err)
		}
		return
	}
	inserted := false
	for _, product := range productEvents {
		rows, err := queries.ListActiveOutboundWebhookSubscriptionsForEvent(ctx, db.ListActiveOutboundWebhookSubscriptionsForEventParams{
			WorkspaceID: workspaceID,
			EventType:   product.eventType,
		})
		if err != nil {
			slog.Error("outbound webhook subscription lookup failed", "event_type", product.eventType, "error", err)
			return
		}
		if len(rows) == 0 {
			continue
		}
		eventID := uuid.New()
		body, err := s.productEventBody(ctx, event, eventID, product)
		if err != nil {
			if !errors.Is(err, errCommentBodyUnchanged) {
				slog.Error("outbound webhook canonicalization failed", "event_type", product.eventType, "error", err)
			}
			return
		}
		for _, subscription := range rows {
			if !subscriptionMatchesProjectScope(subscription, body, product.eventType) {
				continue
			}
			_, err := queries.CreateOutboundWebhookDelivery(ctx, db.CreateOutboundWebhookDeliveryParams{
				EventID: pgtype.UUID{Bytes: eventID, Valid: true}, SubscriptionID: subscription.ID,
				WorkspaceID: subscription.WorkspaceID, EventType: product.eventType, RequestBody: body,
				SigningSecretCiphertext: subscription.SecretCiphertext,
				DestinationCiphertext:   subscription.DestinationCiphertext,
				SecretVersion:           subscription.SecretVersion,
				MaxPending:              s.policy.MaxPendingPerSubscription,
			})
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					slog.Warn("outbound webhook pending delivery limit reached", "subscription_id", util.UUIDToString(subscription.ID), "event_type", product.eventType)
					continue
				}
				slog.Error("outbound webhook delivery persistence failed", "event_type", product.eventType, "error", err)
				return
			}
			inserted = true
		}
	}
	if !inserted {
		_ = tx.Rollback(ctx)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		slog.Error("outbound webhook delivery commit failed", "event_type", event.Type, "error", err)
		return
	}
	s.wakeDispatcher()
}

func subscriptionMatchesProjectScope(subscription db.OutboundWebhookSubscription, body []byte, eventType string) bool {
	if subscription.ScopeMode == ScopeWorkspace {
		return true
	}
	if subscription.ScopeMode != ScopeProject || len(subscription.ProjectIds) == 0 {
		return false
	}
	var envelope struct {
		Data struct {
			Issue struct {
				ProjectID *string `json:"project_id"`
			} `json:"issue"`
			Change *struct {
				Previous json.RawMessage `json:"previous"`
				Current  json.RawMessage `json:"current"`
			} `json:"change"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return false
	}
	candidates := make(map[string]struct{}, 2)
	if envelope.Data.Issue.ProjectID != nil {
		candidates[*envelope.Data.Issue.ProjectID] = struct{}{}
	}
	if eventType == EventIssueProjectChanged && envelope.Data.Change != nil {
		for _, raw := range []json.RawMessage{envelope.Data.Change.Previous, envelope.Data.Change.Current} {
			var projectID *string
			if len(raw) > 0 && json.Unmarshal(raw, &projectID) == nil && projectID != nil {
				candidates[*projectID] = struct{}{}
			}
		}
	}
	for _, selected := range subscription.ProjectIds {
		if _, ok := candidates[util.UUIDToString(selected)]; ok {
			return true
		}
	}
	return false
}

func (s *Service) captureComment(event events.Event, eventType string) {
	s.captureProductEvents(event, []productEvent{{eventType: eventType}})
}

func (s *Service) productEventBody(ctx context.Context, event events.Event, eventID uuid.UUID, product productEvent) ([]byte, error) {
	switch product.eventType {
	case EventCommentCreated, EventCommentUpdated, EventCommentDeleted:
		if product.eventType == EventCommentUpdated {
			payload, _ := event.Payload.(map[string]any)
			bodyChanged, _ := payload["body_changed"].(bool)
			if !bodyChanged {
				return nil, errCommentBodyUnchanged
			}
		}
		issue, err := commentEventIssueSnapshot(event.Payload, product.eventType)
		if err != nil {
			return nil, err
		}
		return s.commentBody(event, eventID, issue, product.eventType)
	default:
		return s.issueEventBody(ctx, event, eventID, product)
	}
}

type issuePayload struct {
	ID           string  `json:"id"`
	Identifier   string  `json:"identifier"`
	Title        string  `json:"title"`
	Status       string  `json:"status"`
	Priority     string  `json:"priority"`
	ProjectID    *string `json:"project_id"`
	AssigneeType *string `json:"assignee_type"`
	AssigneeID   *string `json:"assignee_id"`
}

type assigneeSnapshot struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
}

type issueSnapshot struct {
	ID         string            `json:"id"`
	Identifier string            `json:"identifier"`
	Title      string            `json:"title"`
	Status     string            `json:"status"`
	Priority   string            `json:"priority"`
	ProjectID  *string           `json:"project_id"`
	Assignee   *assigneeSnapshot `json:"assignee"`
}

type issueChange struct {
	Field    string `json:"field"`
	Previous any    `json:"previous"`
	Current  any    `json:"current"`
}

func (s *Service) issueEventBody(ctx context.Context, event events.Event, eventID uuid.UUID, product productEvent) ([]byte, error) {
	payload, ok := event.Payload.(map[string]any)
	if !ok {
		return nil, errors.New("issue event payload is not an object")
	}
	rawIssue, ok := payload["issue"]
	if !ok {
		return nil, errors.New("issue event payload has no issue")
	}
	encoded, err := json.Marshal(rawIssue)
	if err != nil {
		return nil, err
	}
	var issue issuePayload
	if err := json.Unmarshal(encoded, &issue); err != nil || issue.ID == "" || issue.Title == "" {
		return nil, errors.New("issue event payload is incomplete")
	}
	assignee, err := s.resolveAssignee(ctx, event.WorkspaceID, issue.AssigneeType, issue.AssigneeID)
	if err != nil {
		return nil, err
	}
	snapshot := issueSnapshot{ID: issue.ID, Identifier: issue.Identifier, Title: issue.Title, Status: issue.Status, Priority: issue.Priority, ProjectID: issue.ProjectID, Assignee: assignee}
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
			Issue  issueSnapshot `json:"issue"`
			Change *issueChange  `json:"change,omitempty"`
		} `json:"data"`
	}{Version: 1, ID: eventID.String(), Type: product.eventType, OccurredAt: s.now().UTC()}
	envelope.Workspace.ID = event.WorkspaceID
	envelope.Actor.Type, envelope.Actor.ID = event.ActorType, event.ActorID
	envelope.Data.Issue = snapshot
	if product.changeField != "" {
		change, err := s.issueChange(ctx, event.WorkspaceID, payload, issue, assignee, product.changeField)
		if err != nil {
			return nil, err
		}
		envelope.Data.Change = change
	}
	return json.Marshal(envelope)
}

func (s *Service) issueChange(ctx context.Context, workspaceID string, payload map[string]any, issue issuePayload, currentAssignee *assigneeSnapshot, field string) (*issueChange, error) {
	change := &issueChange{Field: field}
	switch field {
	case "status":
		previous, err := requiredString(payload, "prev_status")
		if err != nil {
			return nil, err
		}
		change.Previous, change.Current = previous, issue.Status
	case "priority":
		previous, err := requiredString(payload, "prev_priority")
		if err != nil {
			return nil, err
		}
		change.Previous, change.Current = previous, issue.Priority
	case "project":
		previous, err := nullableString(payload, "prev_project_id")
		if err != nil {
			return nil, err
		}
		change.Previous, change.Current = previous, issue.ProjectID
	case "assignee":
		previousType, err := nullableString(payload, "prev_assignee_type")
		if err != nil {
			return nil, err
		}
		previousID, err := nullableString(payload, "prev_assignee_id")
		if err != nil {
			return nil, err
		}
		previous, err := s.resolveAssignee(ctx, workspaceID, previousType, previousID)
		if err != nil {
			return nil, err
		}
		change.Previous, change.Current = previous, currentAssignee
	default:
		return nil, fmt.Errorf("unsupported issue change field %q", field)
	}
	return change, nil
}

func requiredString(payload map[string]any, key string) (string, error) {
	value, err := nullableString(payload, key)
	if err != nil || value == nil {
		return "", fmt.Errorf("issue change payload has invalid %s", key)
	}
	return *value, nil
}

func nullableString(payload map[string]any, key string) (*string, error) {
	raw, exists := payload[key]
	if !exists {
		return nil, fmt.Errorf("issue change payload has no %s", key)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}
	var value *string
	if err := json.Unmarshal(encoded, &value); err != nil {
		return nil, fmt.Errorf("issue change payload has invalid %s", key)
	}
	return value, nil
}

func (s *Service) resolveAssignee(ctx context.Context, workspaceID string, assigneeType, assigneeID *string) (*assigneeSnapshot, error) {
	if assigneeType == nil && assigneeID == nil {
		return nil, nil
	}
	if assigneeType == nil || assigneeID == nil || *assigneeType == "" || *assigneeID == "" {
		return nil, errors.New("issue assignee identity is incomplete")
	}
	workspaceUUID, err := util.ParseUUID(workspaceID)
	if err != nil {
		return nil, errors.New("issue assignee workspace is invalid")
	}
	identity, err := util.ParseUUID(*assigneeID)
	if err != nil {
		return nil, errors.New("issue assignee id is invalid")
	}
	snapshot := &assigneeSnapshot{Type: *assigneeType, ID: *assigneeID}
	switch *assigneeType {
	case "member":
		if _, err := s.queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{UserID: identity, WorkspaceID: workspaceUUID}); err != nil {
			return nil, errors.New("issue member assignee is unavailable")
		}
		user, err := s.queries.GetUser(ctx, identity)
		if err != nil {
			return nil, errors.New("issue member assignee is unavailable")
		}
		snapshot.DisplayName = user.Name
	case "agent":
		agent, err := s.queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: identity, WorkspaceID: workspaceUUID})
		if err != nil {
			return nil, errors.New("issue agent assignee is unavailable")
		}
		snapshot.DisplayName = agent.Name
	case "squad":
		squad, err := s.queries.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: identity, WorkspaceID: workspaceUUID})
		if err != nil {
			return nil, errors.New("issue squad assignee is unavailable")
		}
		snapshot.DisplayName = squad.Name
	default:
		return nil, fmt.Errorf("unsupported issue assignee type %q", *assigneeType)
	}
	return snapshot, nil
}

func commentEventIssueID(payload any, eventType string) (string, error) {
	object, ok := payload.(map[string]any)
	if !ok {
		return "", errors.New("comment payload is not an object")
	}
	if eventType == EventCommentDeleted {
		issueID, _ := object["issue_id"].(string)
		if issueID == "" {
			return "", errors.New("comment deletion payload has no issue id")
		}
		return issueID, nil
	}
	rawComment, ok := object["comment"]
	if !ok {
		return "", errors.New("comment payload has no comment")
	}
	encoded, err := json.Marshal(rawComment)
	if err != nil {
		return "", err
	}
	var comment struct {
		IssueID string `json:"issue_id"`
	}
	if err := json.Unmarshal(encoded, &comment); err != nil || comment.IssueID == "" {
		return "", errors.New("comment payload has no issue id")
	}
	return comment.IssueID, nil
}

func commentEventIssueSnapshot(payload any, eventType string) (publicParentIssueSnapshot, error) {
	object, ok := payload.(map[string]any)
	if !ok {
		return publicParentIssueSnapshot{}, errors.New("comment payload is not an object")
	}
	issueID, err := commentEventIssueID(payload, eventType)
	if err != nil {
		return publicParentIssueSnapshot{}, err
	}
	if _, err := util.ParseUUID(issueID); err != nil {
		return publicParentIssueSnapshot{}, errors.New("comment payload has an invalid issue id")
	}
	title, _ := object["issue_title"].(string)
	status, _ := object["issue_status"].(string)
	priority, _ := object["issue_priority"].(string)
	if title == "" || status == "" || priority == "" {
		return publicParentIssueSnapshot{}, errors.New("comment payload has an incomplete parent issue snapshot")
	}
	raw, ok := object["issue_project_id"]
	if !ok {
		return publicParentIssueSnapshot{}, errors.New("comment payload has no event-time project snapshot")
	}
	var value string
	switch typed := raw.(type) {
	case nil:
		return publicParentIssueSnapshot{ID: issueID, Title: title, Status: status, Priority: priority}, nil
	case string:
		value = typed
	case *string:
		if typed == nil {
			return publicParentIssueSnapshot{ID: issueID, Title: title, Status: status, Priority: priority}, nil
		}
		value = *typed
	default:
		return publicParentIssueSnapshot{}, errors.New("comment payload has an invalid event-time project snapshot")
	}
	if value == "" {
		return publicParentIssueSnapshot{}, errors.New("comment payload has an invalid event-time project snapshot")
	}
	if _, err := util.ParseUUID(value); err != nil {
		return publicParentIssueSnapshot{}, errors.New("comment payload has an invalid event-time project snapshot")
	}
	return publicParentIssueSnapshot{ID: issueID, Title: title, Status: status, Priority: priority, ProjectID: &value}, nil
}

type publicCommentSnapshot struct {
	ID       string  `json:"id"`
	IssueID  string  `json:"issue_id"`
	ParentID *string `json:"parent_id"`
	Author   struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	} `json:"author"`
	Type      string `json:"type"`
	Excerpt   string `json:"excerpt,omitempty"`
	Truncated *bool  `json:"truncated,omitempty"`
	CreatedAt string `json:"created_at,omitempty"`
	UpdatedAt string `json:"updated_at,omitempty"`
	DeletedAt string `json:"deleted_at,omitempty"`
}

type publicParentIssueSnapshot struct {
	ID        string  `json:"id"`
	Title     string  `json:"title"`
	Status    string  `json:"status"`
	Priority  string  `json:"priority"`
	ProjectID *string `json:"project_id"`
}

func (s *Service) commentBody(event events.Event, eventID uuid.UUID, issue publicParentIssueSnapshot, eventType string) ([]byte, error) {
	payload, ok := event.Payload.(map[string]any)
	if !ok {
		return nil, errors.New("comment payload is not an object")
	}

	comment := publicCommentSnapshot{}
	if eventType == EventCommentDeleted {
		comment.ID, _ = payload["comment_id"].(string)
		comment.IssueID, _ = payload["issue_id"].(string)
		comment.ParentID = optionalString(payload["parent_id"])
		comment.Author.Type, _ = payload["author_type"].(string)
		comment.Author.ID, _ = payload["author_id"].(string)
		comment.Type, _ = payload["comment_type"].(string)
		comment.DeletedAt = s.now().UTC().Format(time.RFC3339)
	} else {
		if eventType == EventCommentUpdated {
			bodyChanged, _ := payload["body_changed"].(bool)
			if !bodyChanged {
				return nil, errCommentBodyUnchanged
			}
		}
		rawComment, ok := payload["comment"]
		if !ok {
			return nil, errors.New("comment payload has no comment")
		}
		encoded, err := json.Marshal(rawComment)
		if err != nil {
			return nil, err
		}
		var source struct {
			ID         string  `json:"id"`
			IssueID    string  `json:"issue_id"`
			ParentID   *string `json:"parent_id"`
			AuthorType string  `json:"author_type"`
			AuthorID   string  `json:"author_id"`
			Content    string  `json:"content"`
			Type       string  `json:"type"`
			CreatedAt  string  `json:"created_at"`
			UpdatedAt  string  `json:"updated_at"`
		}
		if err := json.Unmarshal(encoded, &source); err != nil {
			return nil, err
		}
		comment.ID, comment.IssueID, comment.ParentID = source.ID, source.IssueID, source.ParentID
		comment.Author.Type, comment.Author.ID = source.AuthorType, source.AuthorID
		comment.Type, comment.CreatedAt, comment.UpdatedAt = source.Type, source.CreatedAt, source.UpdatedAt
		excerptRunes := []rune(source.Content)
		truncated := len(excerptRunes) > maxCommentExcerptCodePoints
		if truncated {
			excerptRunes = excerptRunes[:maxCommentExcerptCodePoints]
		}
		comment.Excerpt = string(excerptRunes)
		comment.Truncated = &truncated
	}
	if comment.ID == "" || comment.IssueID == "" || comment.IssueID != issue.ID || comment.Author.Type == "" || comment.Type == "" {
		return nil, errors.New("comment payload is incomplete")
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
			Comment publicCommentSnapshot     `json:"comment"`
			Issue   publicParentIssueSnapshot `json:"issue"`
		} `json:"data"`
	}{Version: 1, ID: eventID.String(), Type: eventType, OccurredAt: s.now().UTC()}
	envelope.Workspace.ID = event.WorkspaceID
	envelope.Actor.Type, envelope.Actor.ID = event.ActorType, event.ActorID
	envelope.Data.Comment = comment
	envelope.Data.Issue = issue
	return json.Marshal(envelope)
}

func optionalString(value any) *string {
	if value == nil {
		return nil
	}
	if raw, ok := value.(string); ok && raw != "" {
		return &raw
	}
	if raw, ok := value.(*string); ok && raw != nil && *raw != "" {
		copy := *raw
		return &copy
	}
	return nil
}

func (s *Service) deliver(ctx context.Context, subscription db.OutboundWebhookSubscription, delivery db.OutboundWebhookDelivery) {
	destination, err := s.box.Open(delivery.DestinationCiphertext)
	if err != nil {
		s.failClaim(delivery, 0, "encrypted destination is unavailable")
		return
	}
	secret, err := s.box.Open(delivery.SigningSecretCiphertext)
	if err != nil {
		s.failClaim(delivery, 0, "encrypted signing secret is unavailable")
		return
	}
	endpoint, operatorAllowed, err := s.validateDestination(ctx, string(destination))
	if err != nil {
		if ctx.Err() != nil {
			s.releaseClaim(delivery)
			return
		}
		s.failClaim(delivery, 0, "destination is no longer allowed")
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, endpoint.String(), bytes.NewReader(delivery.RequestBody))
	if err != nil {
		s.failClaim(delivery, 0, "request could not be created")
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
			s.releaseClaim(delivery)
			return
		}
		s.retryOrFail(delivery, 0, "receiver did not answer", "")
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseBytes))
	if retryableStatus(resp.StatusCode) {
		s.retryOrFail(delivery, resp.StatusCode, fmt.Sprintf("receiver returned HTTP %d", resp.StatusCode), resp.Header.Get("Retry-After"))
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.failClaim(delivery, resp.StatusCode, fmt.Sprintf("receiver returned HTTP %d", resp.StatusCode))
		return
	}
	rows, err := s.queries.SucceedClaimedOutboundWebhookDelivery(context.Background(), db.SucceedClaimedOutboundWebhookDeliveryParams{
		ID: delivery.ID, LeaseToken: delivery.LeaseToken,
		ResponseStatus: pgtype.Int4{Int32: int32(resp.StatusCode), Valid: true},
	})
	if err != nil {
		slog.Error("outbound webhook delivery state update failed", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType, "error", err)
	} else if rows == 0 {
		slog.Warn("outbound webhook delivery success ignored after lease loss", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType)
	}
}

func (s *Service) retryOrFail(delivery db.OutboundWebhookDelivery, status int, reason, retryAfter string) {
	attempt := delivery.AttemptCount
	if attempt >= s.policy.MaxAttempts {
		s.failClaim(delivery, status, reason)
		return
	}
	delay := s.retryDelay(attempt, retryAfter)
	params := db.RetryClaimedOutboundWebhookDeliveryParams{
		ID: delivery.ID, LeaseToken: delivery.LeaseToken,
		FailureReason: pgtype.Text{String: reason, Valid: true},
		NextAttemptAt: pgtype.Timestamptz{Time: s.now().Add(delay), Valid: true},
	}
	if status != 0 {
		params.ResponseStatus = pgtype.Int4{Int32: int32(status), Valid: true}
	}
	rows, err := s.queries.RetryClaimedOutboundWebhookDelivery(context.Background(), params)
	if err != nil {
		slog.Error("outbound webhook delivery state update failed", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType, "error", err)
		return
	}
	if rows == 0 {
		slog.Warn("outbound webhook delivery retry ignored after lease loss", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType)
		return
	}
	slog.Info("outbound webhook delivery scheduled for retry", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType, "attempt", attempt, "retry_in", delay)
	s.wakeDispatcher()
}

func (s *Service) failClaim(delivery db.OutboundWebhookDelivery, status int, reason string) {
	params := db.FailClaimedOutboundWebhookDeliveryParams{
		ID: delivery.ID, LeaseToken: delivery.LeaseToken,
		FailureReason:    pgtype.Text{String: reason, Valid: true},
		FailureThreshold: s.policy.ConsecutiveFailureThreshold,
	}
	if status != 0 {
		params.ResponseStatus = pgtype.Int4{Int32: int32(status), Valid: true}
	}
	rows, err := s.queries.FailClaimedOutboundWebhookDelivery(context.Background(), params)
	if err != nil {
		slog.Error("outbound webhook delivery state update failed", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType, "error", err)
	} else if rows == 0 {
		slog.Warn("outbound webhook terminal outcome ignored after lease loss", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType)
	}
}

func (s *Service) releaseClaim(delivery db.OutboundWebhookDelivery) {
	if !delivery.LeaseToken.Valid {
		return
	}
	if _, err := s.queries.ReleaseClaimedOutboundWebhookDelivery(context.Background(), db.ReleaseClaimedOutboundWebhookDeliveryParams{ID: delivery.ID, LeaseToken: delivery.LeaseToken}); err != nil {
		slog.Error("outbound webhook delivery lease release failed", "delivery_id", util.UUIDToString(delivery.ID), "event_type", delivery.EventType, "error", err)
	}
	s.wakeDispatcher()
}

func (s *Service) retryDelay(attempt int32, rawRetryAfter string) time.Duration {
	delay := s.policy.InitialBackoff
	for i := int32(1); i < attempt && delay < s.policy.MaxBackoff; i++ {
		if delay > s.policy.MaxBackoff/2 {
			delay = s.policy.MaxBackoff
			break
		}
		delay *= 2
	}
	if delay = s.jitter(delay); delay > s.policy.MaxBackoff {
		delay = s.policy.MaxBackoff
	}
	if retryAfter, ok := parseRetryAfter(rawRetryAfter, s.now()); ok {
		if retryAfter > s.policy.MaxRetryAfter {
			retryAfter = s.policy.MaxRetryAfter
		}
		if retryAfter > delay {
			delay = retryAfter
		}
	}
	if delay > s.policy.MaxBackoff {
		delay = s.policy.MaxBackoff
	}
	return delay
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
	var pauseReason *string
	if row.PauseReason.Valid {
		pauseReason = &row.PauseReason.String
	}
	projectIDs := make([]string, 0, len(row.ProjectIds))
	for _, id := range row.ProjectIds {
		projectIDs = append(projectIDs, util.UUIDToString(id))
	}
	return Subscription{
		ID: util.UUIDToString(row.ID), WorkspaceID: util.UUIDToString(row.WorkspaceID), Name: row.Name,
		DestinationHint: row.DestinationHint, Events: events, EventCatalogVersion: row.EventCatalogVersion,
		ScopeMode: row.ScopeMode, ProjectIDs: projectIDs, Status: row.Status, PauseReason: pauseReason,
		ConsecutiveTerminalFailures: row.ConsecutiveTerminalFailures,
		SigningSecretHint:           row.SigningSecretHint,
		SecretVersion:               row.SecretVersion,
		CreatedAt:                   row.CreatedAt.Time, UpdatedAt: row.UpdatedAt.Time,
	}
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

func normalizeDeliveryPolicy(policy DeliveryPolicy) DeliveryPolicy {
	defaults := DefaultDeliveryPolicy()
	if policy.PollInterval <= 0 {
		policy.PollInterval = defaults.PollInterval
	}
	if policy.LeaseDuration <= 0 {
		policy.LeaseDuration = defaults.LeaseDuration
	}
	if policy.InitialBackoff <= 0 {
		policy.InitialBackoff = defaults.InitialBackoff
	}
	if policy.MaxBackoff < policy.InitialBackoff {
		policy.MaxBackoff = defaults.MaxBackoff
	}
	if policy.MaxRetryAfter <= 0 {
		policy.MaxRetryAfter = defaults.MaxRetryAfter
	}
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = defaults.MaxAttempts
	} else if policy.MaxAttempts > hardMaxAttempts {
		policy.MaxAttempts = hardMaxAttempts
	}
	if policy.GlobalConcurrency <= 0 {
		policy.GlobalConcurrency = defaults.GlobalConcurrency
	} else if policy.GlobalConcurrency > hardMaxWorkers {
		policy.GlobalConcurrency = hardMaxWorkers
	}
	if policy.SubscriptionConcurrency > hardMaxPerSub {
		policy.SubscriptionConcurrency = hardMaxPerSub
	}
	if policy.SubscriptionConcurrency <= 0 || policy.SubscriptionConcurrency > policy.GlobalConcurrency {
		policy.SubscriptionConcurrency = min(defaults.SubscriptionConcurrency, policy.GlobalConcurrency)
	}
	if policy.MaxPendingPerSubscription <= 0 {
		policy.MaxPendingPerSubscription = defaults.MaxPendingPerSubscription
	} else if policy.MaxPendingPerSubscription > hardMaxPending {
		policy.MaxPendingPerSubscription = hardMaxPending
	}
	if policy.ConsecutiveFailureThreshold <= 0 {
		policy.ConsecutiveFailureThreshold = defaults.ConsecutiveFailureThreshold
	} else if policy.ConsecutiveFailureThreshold > hardMaxFailures {
		policy.ConsecutiveFailureThreshold = hardMaxFailures
	}
	return policy
}

func boundedJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	spread := delay / 5
	if spread == 0 {
		return delay
	}
	return delay - spread + time.Duration(mathrand.Int64N(int64(2*spread)+1))
}

func parseRetryAfter(raw string, now time.Time) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(time.Duration(1<<63-1)/time.Second) {
			return time.Duration(1<<63 - 1), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(raw)
	if err != nil || !when.After(now) {
		return 0, false
	}
	return when.Sub(now), true
}

func retryableStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly || status == http.StatusTooManyRequests || (status >= 500 && status < 600)
}

func stopTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
