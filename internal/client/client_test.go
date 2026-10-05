package client

import (
	"context"
	"strconv"
	"testing"
	"time"

	"ws-tunnel/internal/protocol"
)

// TestRemoteForUsesThePushedAllowList covers §10: the client forwards only to
// what the server pushed, and an unknown port id resolves to nothing rather
// than to some default.
func TestRemoteForUsesThePushedAllowList(t *testing.T) {
	c := testClient()

	// No config yet — nothing is allowed.
	if got := c.remoteFor(19080); got != "" {
		t.Errorf("remoteFor before any config = %q, want empty", got)
	}

	c.setConfig(&protocol.NodeConfig{
		Ports: map[string]string{"19080": "127.0.0.1:8080"},
	})
	if got := c.remoteFor(19080); got != "127.0.0.1:8080" {
		t.Errorf("remoteFor(19080) = %q, want 127.0.0.1:8080", got)
	}
	if got := c.remoteFor(15432); got != "" {
		t.Errorf("remoteFor(15432) = %q, want empty for a port not in the list", got)
	}

	// A reload replaces the list wholesale: what it drops stops being allowed.
	c.setConfig(&protocol.NodeConfig{
		Ports: map[string]string{"15432": "127.0.0.1:5432"},
	})
	if got := c.remoteFor(19080); got != "" {
		t.Errorf("remoteFor(19080) after reload = %q, want empty", got)
	}
	if got := c.remoteFor(15432); got != "127.0.0.1:5432" {
		t.Errorf("remoteFor(15432) after reload = %q", got)
	}
}

func TestSendControlWithoutChannelFails(t *testing.T) {
	if err := testClient().sendControl(&protocol.Message{Type: protocol.TypeStats}); err == nil {
		t.Error("sendControl succeeded with no control channel")
	}
}

func TestLastErrorRoundTrips(t *testing.T) {
	c := testClient()
	if got := c.lastError(); got != "" {
		t.Errorf("fresh client: lastError = %q, want empty", got)
	}
	c.setLastError("first")
	c.setLastError("second")
	if got := c.lastError(); got != "second" {
		t.Errorf("lastError = %q, want the most recent failure", got)
	}
}

// TestStatsHeartbeatFallback documents the timer choice when the server pushed
// no heartbeat: statsLoop must fall back rather than build a zero-tick ticker,
// which panics.
func TestStatsHeartbeatFallback(t *testing.T) {
	c := testClient()
	c.setConfig(&protocol.NodeConfig{})
	if got := c.Config().Heartbeat.D(); got != 0 {
		t.Fatalf("test premise broken: heartbeat = %v, want 0", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.statsLoop(ctx) // must not panic on a zero heartbeat
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("statsLoop did not return after cancel")
	}
}

func TestPortIDsAreDecimalStrings(t *testing.T) {
	// The allow-list is keyed by the decimal form of the port id carried in the
	// stream header; a mismatch here silently rejects every stream.
	c := testClient()
	c.setConfig(&protocol.NodeConfig{Ports: map[string]string{strconv.Itoa(19080): "127.0.0.1:8080"}})
	if got := c.remoteFor(19080); got == "" {
		t.Error("port id did not resolve against its decimal key")
	}
}
