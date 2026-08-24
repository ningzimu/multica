package outwebhook

import (
	"context"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util/secretbox"
)

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
