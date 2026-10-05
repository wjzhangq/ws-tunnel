package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"ws-tunnel/internal/mux"
	"ws-tunnel/internal/protocol"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func autoPeer(ctx context.Context, c *mux.Conn) {
	for {
		st, err := c.Accept(ctx)
		if err != nil {
			return
		}
		go func(st *mux.Stream) {
			_ = st.Ack(protocol.AckOK)
			_, _ = io.Copy(io.Discard, st)
			_ = st.CloseWrite()
		}(st)
	}
}

func newTestSession(t *testing.T, maxStreams int, queueTimeout time.Duration) *NodeSession {
	t.Helper()
	ctx := t.Context()
	a, b := mux.MemPair()
	srvMux := mux.New(ctx, a)
	cliMux := mux.New(ctx, b)
	go autoPeer(ctx, cliMux)
	t.Cleanup(func() {
		srvMux.Close()
		cliMux.Close()
	})
	n := newNodeSession("node1", &protocol.NodeConfig{
		Channels:          1,
		MaxStreamsPerConn: maxStreams,
	}, queueTimeout, &NodeStats{}, testLogger())
	n.AttachMux(srvMux)
	return n
}

func TestOpenStreamRespectsMaxStreamsPerConn(t *testing.T) {
	n := newTestSession(t, 2, 50*time.Millisecond)

	for i := 0; i < 2; i++ {
		st, err := n.OpenStream(context.Background(), 19080)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if st == nil {
			t.Fatalf("open %d returned nil", i)
		}
	}
	if got := n.ActiveStreams(); got != 2 {
		t.Fatalf("active streams = %d, want 2", got)
	}

	start := time.Now()
	if _, err := n.OpenStream(context.Background(), 19080); err != ErrSaturated {
		t.Fatalf("err = %v, want ErrSaturated", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("returned after %v, want it to wait out queue_timeout", elapsed)
	}
	if got := n.stats.Saturated.Load(); got != 1 {
		t.Fatalf("saturated counter = %d, want 1", got)
	}
	if got := n.QueueDepth(); got != 0 {
		t.Fatalf("queue depth = %d after the waiter gave up, want 0", got)
	}
}

func TestTakeoverReplacesLiveSession(t *testing.T) {
	r := newRegistry(testLogger())
	a := newNodeSession("node1", &protocol.NodeConfig{MaxStreamsPerConn: 1}, time.Second, r.Stats("node1"), testLogger())
	b := newNodeSession("node1", &protocol.NodeConfig{MaxStreamsPerConn: 1}, time.Second, r.Stats("node1"), testLogger())
	if err := r.Register(a); err != nil {
		t.Fatal(err)
	}
	old := r.Takeover(b)
	if old != a {
		t.Fatalf("Takeover returned %v, want the incumbent", old)
	}
	if r.Get("node1") != b {
		t.Fatal("live session was not replaced")
	}
	if err := r.Register(newNodeSession("node1", &protocol.NodeConfig{MaxStreamsPerConn: 1}, time.Second, r.Stats("node1"), testLogger())); err != ErrNodeBusy {
		t.Fatalf("Register after Takeover: %v, want ErrNodeBusy", err)
	}
}
