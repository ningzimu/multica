package push

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	productionAPNsHost = "https://api.push.apple.com"
	sandboxAPNsHost    = "https://api.sandbox.push.apple.com"
)

type Config struct {
	TeamID         string
	KeyID          string
	Topic          string
	Environment    string
	PrivateKeyPath string
	Host           string
}

func ConfigFromEnv() (Config, bool, error) {
	cfg := Config{
		TeamID:         strings.TrimSpace(os.Getenv("MULTICA_APNS_TEAM_ID")),
		KeyID:          strings.TrimSpace(os.Getenv("MULTICA_APNS_KEY_ID")),
		Topic:          strings.TrimSpace(os.Getenv("MULTICA_APNS_TOPIC")),
		Environment:    strings.ToLower(strings.TrimSpace(os.Getenv("MULTICA_APNS_ENVIRONMENT"))),
		PrivateKeyPath: strings.TrimSpace(os.Getenv("MULTICA_APNS_PRIVATE_KEY_PATH")),
	}
	if cfg.TeamID == "" && cfg.KeyID == "" && cfg.Topic == "" && cfg.PrivateKeyPath == "" {
		return Config{}, false, nil
	}
	if cfg.Environment == "" {
		cfg.Environment = "production"
	}
	if cfg.Environment != "production" && cfg.Environment != "sandbox" {
		return Config{}, false, errors.New("MULTICA_APNS_ENVIRONMENT must be production or sandbox")
	}
	if cfg.TeamID == "" || cfg.KeyID == "" || cfg.Topic == "" || cfg.PrivateKeyPath == "" {
		return Config{}, false, errors.New("APNs configuration is incomplete")
	}
	if cfg.Environment == "sandbox" {
		cfg.Host = sandboxAPNsHost
	} else {
		cfg.Host = productionAPNsHost
	}
	return cfg, true, nil
}

type Payload struct {
	APS struct {
		Alert struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		} `json:"alert"`
		Sound string `json:"sound"`
	} `json:"aps"`
	Multica Destination `json:"multica"`
}

type Destination struct {
	NotificationID string `json:"notification_id"`
	WorkspaceID    string `json:"workspace_id"`
	WorkspaceSlug  string `json:"workspace_slug,omitempty"`
	IssueID        string `json:"issue_id,omitempty"`
	CommentID      string `json:"comment_id,omitempty"`
}

type SendResult struct {
	StatusCode   int
	Reason       string
	InvalidToken bool
	Retryable    bool
}

type Transport interface {
	Send(context.Context, string, Payload) (SendResult, error)
}

type HTTPTransport struct {
	client *http.Client
	cfg    Config
	key    *ecdsa.PrivateKey

	mu        sync.Mutex
	authToken string
	tokenAt   time.Time
}

func NewHTTPTransport(cfg Config, client *http.Client) (*HTTPTransport, error) {
	pem, err := os.ReadFile(cfg.PrivateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read APNs private key: %w", err)
	}
	key, err := jwt.ParseECPrivateKeyFromPEM(pem)
	if err != nil {
		return nil, fmt.Errorf("parse APNs private key: %w", err)
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPTransport{client: client, cfg: cfg, key: key}, nil
}

func (t *HTTPTransport) token(now time.Time) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.authToken != "" && now.Sub(t.tokenAt) < 50*time.Minute {
		return t.authToken, nil
	}
	claims := jwt.MapClaims{"iss": t.cfg.TeamID, "iat": now.Unix()}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = t.cfg.KeyID
	signed, err := token.SignedString(t.key)
	if err != nil {
		return "", err
	}
	t.authToken = signed
	t.tokenAt = now
	return signed, nil
}

func (t *HTTPTransport) Send(ctx context.Context, deviceToken string, payload Payload) (SendResult, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return SendResult{}, err
	}
	authToken, err := t.token(time.Now())
	if err != nil {
		return SendResult{}, err
	}
	endpoint := strings.TrimRight(t.cfg.Host, "/") + "/3/device/" + url.PathEscape(deviceToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return SendResult{}, err
	}
	req.Header.Set("authorization", "bearer "+authToken)
	req.Header.Set("apns-topic", t.cfg.Topic)
	req.Header.Set("apns-push-type", "alert")
	req.Header.Set("apns-priority", "10")
	req.Header.Set("content-type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		// net/http errors commonly include the full request URL. The APNs URL
		// contains the device token, so never pass that error through to logs.
		return SendResult{Retryable: true}, errors.New("APNs request failed")
	}
	defer resp.Body.Close()
	result := SendResult{StatusCode: resp.StatusCode}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return result, nil
	}
	var failure struct {
		Reason string `json:"reason"`
	}
	limited := io.LimitReader(resp.Body, 4*1024)
	_ = json.NewDecoder(limited).Decode(&failure)
	result.Reason = failure.Reason
	switch failure.Reason {
	case "BadDeviceToken", "DeviceTokenNotForTopic", "Unregistered":
		result.InvalidToken = true
	}
	result.Retryable = resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
	return result, nil
}
