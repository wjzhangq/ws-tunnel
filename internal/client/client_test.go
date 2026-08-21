package client

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
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

// TestSupervisorResizesInPlace covers the supervisor contract the server relies
// on when it pushes a new channel count: grow and shrink without restarting the
// channels that are already up.
func TestSupervisorResizesInPlace(t *testing.T) {
	c := testClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu      sync.Mutex
		started []int
		live    atomic.Int64
	)
	// Stand in for runChannel: block until the slot's context is cancelled,
	// which is exactly the lifetime a real data channel has.
	sup := newSupervisor(c, ctx)
	sup.run = func(wctx context.Context, slot int) {
		mu.Lock()
		started = append(started, slot)
		mu.Unlock()
		live.Add(1)
		defer live.Add(-1)
		<-wctx.Done()
	}

	sup.SetTarget(4)
	waitCount(t, &live, 4)

	// Growing must only add: the four already-running slots keep running.
	sup.SetTarget(6)
	waitCount(t, &live, 6)
	mu.Lock()
	total := len(started)
	mu.Unlock()
	if total != 6 {
		t.Fatalf("started %d channels to reach 6, want 6 — existing slots were restarted", total)
	}

	sup.SetTarget(2)
	waitCount(t, &live, 2)
	mu.Lock()
	total = len(started)
	mu.Unlock()
	if total != 6 {
		t.Fatalf("started %d channels total after shrinking, want still 6", total)
	}

	// The pool never goes below one channel, whatever the server pushes.
	sup.SetTarget(0)
	waitCount(t, &live, 1)

	sup.Stop()
	waitCount(t, &live, 0)
}

func waitCount(t *testing.T, n *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("live channels = %d, want %d", n.Load(), want)
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
