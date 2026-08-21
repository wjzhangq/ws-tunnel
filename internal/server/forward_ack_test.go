package server

import (
	"net"
	"testing"
	"time"

	"ws-tunnel/internal/protocol"
)

// TestForwardRecordsAckFailures walks the non-OK ack statuses and checks each
// lands in the counter /metrics exposes for it.
func TestForwardRecordsAckFailures(t *testing.T) {
	cases := []struct {
		name       string
		status     byte
		wantResult string
		wantDial   int64
	}{
		{"port not allowed", protocol.AckPortNotAllowed, "not_allowed", 0},
		{"local dial failed", protocol.AckDialFailed, "dial_failed", 1},
		{"rejected", protocol.AckRejected, "rejected", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newForwardFixture(t, 5*time.Second)
			ext, caller := net.Pipe()
			defer ext.Close()
			defer caller.Close()

			done := make(chan struct{})
			go func() {
				defer close(done)
				fx.srv.forward(fx.pl, ext)
			}()

			st := fx.accept()
			if st == nil {
				return
			}
			if _, err := protocol.ReadStreamHeader(st); err != nil {
				t.Fatalf("read stream header: %v", err)
			}
			if err := protocol.WriteAck(st, tc.status); err != nil {
				t.Fatalf("write ack: %v", err)
			}
			_ = st.Close()

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("forward did not return after a refusing ack")
			}

			if got := fx.stats.Result(tc.wantResult); got != 1 {
				t.Errorf("%s counter = %d, want 1", tc.wantResult, got)
			}
			if got := fx.stats.Result("ok"); got != 0 {
				t.Errorf("ok counter = %d, want 0", got)
			}
			if got := fx.stats.LocalDialErrors.Load(); got != tc.wantDial {
				t.Errorf("local_dial_errors = %d, want %d", got, tc.wantDial)
			}
			if _, _, lastErr := fx.stats.Times(); lastErr == "" {
				t.Error("last_error was not recorded for a refused stream")
			}
		})
	}
}

// TestForwardTimesOutWaitingForTheAck covers the dial_timeout deadline: a client
// that never answers must be counted as a timeout, not left hanging.
func TestForwardTimesOutWaitingForTheAck(t *testing.T) {
	fx := newForwardFixture(t, 150*time.Millisecond)
	ext, caller := net.Pipe()
	defer ext.Close()
	defer caller.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.srv.forward(fx.pl, ext)
	}()

	st := fx.accept()
	if st == nil {
		return
	}
	if _, err := protocol.ReadStreamHeader(st); err != nil {
		t.Fatalf("read stream header: %v", err)
	}
	// Never ack.

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("forward never gave up waiting for the ack")
	}
	if got := fx.stats.Result("timeout"); got != 1 {
		t.Errorf("timeout counter = %d, want 1", got)
	}
	if got := fx.sess.ActiveStreams(); got != 0 {
		t.Errorf("active streams = %d after the timeout, want 0", got)
	}
}

// TestForwardWithNodeOfflineIsRejected covers the §9 precondition: a listener
// may briefly outlive its session, and those connections must be dropped rather
// than blocked.
func TestForwardWithNodeOfflineIsRejected(t *testing.T) {
	fx := newForwardFixture(t, time.Second)
	fx.srv.registry.Unregister(fx.sess, "gone")

	ext, caller := net.Pipe()
	defer ext.Close()
	defer caller.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.srv.forward(fx.pl, ext)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("forward blocked with the node offline")
	}
	if got := fx.stats.Result("rejected"); got != 1 {
		t.Errorf("rejected counter = %d, want 1", got)
	}
}
