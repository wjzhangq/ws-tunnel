package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/xtaci/smux"

	"ws-tunnel/internal/protocol"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// channelPair builds a real smux session pair over an in-memory pipe, so slot
// accounting is exercised against the same smux behaviour as production.
func channelPair(t *testing.T, id int) *DataChannel {
	t.Helper()
	a, b := net.Pipe()
	cfg := smux.DefaultConfig()
	cfg.KeepAliveDisabled = true

	srv, err := smux.Server(b, cfg)
	if err != nil {
		t.Fatalf("smux.Server: %v", err)
	}
	cli, err := smux.Client(a, cfg)
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	// Accept and drain whatever the test opens; without a reader the pipe
	// blocks and OpenStream would stall.
	go func() {
		for {
			st, err := srv.AcceptStream()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(io.Discard, st) }()
		}
	}()
	t.Cleanup(func() {
		_ = cli.Close()
		_ = srv.Close()
	})
	return &DataChannel{ID: id, sess: cli}
}

// newTestSession builds a session with `channels` usable data channels and the
// given per-channel stream ceiling.
func newTestSession(t *testing.T, channels, maxStreams int, queueTimeout time.Duration) *NodeSession {
	t.Helper()
	n := newNodeSession("node1", nil, &protocol.NodeConfig{
		Channels:          channels,
		MaxStreamsPerConn: maxStreams,
	}, queueTimeout, &NodeStats{}, testLogger())
	for i := 1; i <= channels; i++ {
		n.AddChannel(channelPair(t, i))
	}
	return n
}

func TestOpenStreamRespectsMaxStreamsPerConn(t *testing.T) {
	n := newTestSession(t, 1, 2, 50*time.Millisecond)

	for i := 0; i < 2; i++ {
		st, ch, err := n.OpenStream(context.Background())
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if st == nil || ch == nil {
			t.Fatalf("open %d returned nils", i)
		}
	}
	if got := n.ActiveStreams(); got != 2 {
		t.Fatalf("active streams = %d, want 2", got)
	}

	// The pool is full: this one must wait out queue_timeout and be reported
	// as saturated, not as "no channel".
	start := time.Now()
	if _, _, err := n.OpenStream(context.Background()); err != ErrSaturated {
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
