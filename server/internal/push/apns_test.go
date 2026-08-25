package push

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestConfigFromEnvDisablesMissingOrIncompleteAPNs(t *testing.T) {
	for _, name := range []string{
		"MULTICA_APNS_TEAM_ID",
		"MULTICA_APNS_KEY_ID",
		"MULTICA_APNS_TOPIC",
		"MULTICA_APNS_ENVIRONMENT",
		"MULTICA_APNS_PRIVATE_KEY_PATH",
	} {
		t.Setenv(name, "")
	}
	if _, enabled, err := ConfigFromEnv(); err != nil || enabled {
		t.Fatalf("empty config enabled=%v err=%v", enabled, err)
	}

	t.Setenv("MULTICA_APNS_TOPIC", "vip.example.multica")
	if _, enabled, err := ConfigFromEnv(); err == nil || enabled {
		t.Fatalf("partial config enabled=%v err=%v", enabled, err)
	}
}

func TestConfigFromEnvSelectsAPNsHost(t *testing.T) {
	t.Setenv("MULTICA_APNS_TEAM_ID", "TEAM")
	t.Setenv("MULTICA_APNS_KEY_ID", "KEY")
	t.Setenv("MULTICA_APNS_TOPIC", "vip.example.multica")
	t.Setenv("MULTICA_APNS_PRIVATE_KEY_PATH", "/restricted/AuthKey.p8")
	t.Setenv("MULTICA_APNS_ENVIRONMENT", "sandbox")

	cfg, enabled, err := ConfigFromEnv()
	if err != nil || !enabled {
		t.Fatalf("enabled=%v err=%v", enabled, err)
	}
	if cfg.Host != sandboxAPNsHost || cfg.Environment != "sandbox" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestHTTPTransportDoesNotExposeDeviceTokenInNetworkError(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	const deviceToken = "secret-native-device-token"
	transport := &HTTPTransport{
		client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			return nil, errors.New("request failed for " + request.URL.String())
		})},
		cfg: Config{TeamID: "TEAM", KeyID: "KEY", Topic: "vip.example.multica", Host: productionAPNsHost},
		key: key,
	}

	_, sendErr := transport.Send(context.Background(), deviceToken, Payload{})
	if sendErr == nil || strings.Contains(sendErr.Error(), deviceToken) {
		t.Fatalf("unsafe APNs error: %v", sendErr)
	}
}
