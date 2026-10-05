package server

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestOpenStreamQueueIsFIFO is the regression test for the fairness gap: with
// the pool full, waiters must be served in arrival order rather than whoever
// happens to win the wakeup race.
//
// One channel with one slot means every waiter past the first has to queue.
// Each waiter records itself and immediately gives the slot back, so the single
// slot cascades down the queue and `served` is exactly the handoff order.
func TestOpenStreamQueueIsFIFO(t *testing.T) {
	const waiters = 8
	n := newTestSession(t, 1, 5*time.Second)

	st0, err := n.OpenStream(context.Background(), 19080)
	if err != nil {
		t.Fatalf("initial open: %v", err)
	}

	var (
		mu     sync.Mutex
		served []int
		wg     sync.WaitGroup
	)

	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			st, err := n.OpenStream(context.Background(), 19080)
			if err != nil {
				t.Errorf("waiter %d: %v", id, err)
				return
			}
			mu.Lock()
			served = append(served, id)
			mu.Unlock()
			n.CloseStream(st)
		}(i)
		waitQueueDepth(t, n, int64(i+1))
	}

	n.CloseStream(st0)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(served) != waiters {
		t.Fatalf("served %d waiters, want %d", len(served), waiters)
	}
	for i, id := range served {
		if id != i {
			t.Fatalf("service order = %v, want strictly FIFO 0..%d", served, waiters-1)
		}
	}
	if got := n.ActiveStreams(); got != 0 {
		t.Fatalf("active streams = %d after everyone finished, want 0", got)
	}
	if got := n.QueueDepth(); got != 0 {
		t.Fatalf("queue depth = %d after everyone finished, want 0", got)
	}
}

// TestOpenStreamHandoffSurvivesTimeoutRace covers the race the waiter queue has
// to get right: a slot handed to a waiter that is simultaneously timing out must
// be used by that waiter rather than dropped, otherwise the slot leaks and the
// pool shrinks permanently.
func TestOpenStreamHandoffSurvivesTimeoutRace(t *testing.T) {
	n := newTestSession(t, 1, 2*time.Millisecond)

	for i := 0; i < 200; i++ {
		st, err := n.OpenStream(context.Background(), 19080)
		if err != nil {
			t.Fatalf("iteration %d: initial open: %v", i, err)
		}

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st, err := n.OpenStream(context.Background(), 19080); err == nil {
				n.CloseStream(st)
			} else if err != ErrSaturated {
				t.Errorf("iteration %d: err = %v, want nil or ErrSaturated", i, err)
			}
		}()

		n.CloseStream(st)
		wg.Wait()

		if got := n.ActiveStreams(); got != 0 {
			t.Fatalf("iteration %d: active streams = %d, want 0 — a slot leaked", i, got)
		}
	}
}

// TestOpenStreamDrainingAndClosed checks the two non-queue exits.
func TestOpenStreamDrainingAndClosed(t *testing.T) {
	n := newTestSession(t, 4, time.Second)
	n.SetDraining(true)
	if _, err := n.OpenStream(context.Background(), 19080); err != ErrDraining {
		t.Fatalf("draining: err = %v, want ErrDraining", err)
	}
	n.SetDraining(false)

	st, err := n.OpenStream(context.Background(), 19080)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := n.OpenStream(context.Background(), 19080); err != nil {
			t.Fatalf("fill %d: %v", i, err)
		}
	}
	done := make(chan error, 1)
	go func() {
		_, err := n.OpenStream(context.Background(), 19080)
		done <- err
	}()
	waitQueueDepth(t, n, 1)
	n.Close("test")
	if err := <-done; err != ErrSessionClosed {
		t.Fatalf("after close: err = %v, want ErrSessionClosed", err)
	}
	n.CloseStream(st)
}

// TestOpenStreamContextCancel checks a caller giving up before queue_timeout.
func TestOpenStreamContextCancel(t *testing.T) {
	n := newTestSession(t, 1, 10*time.Second)
	st, err := n.OpenStream(context.Background(), 19080)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer n.CloseStream(st)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := n.OpenStream(ctx, 19080)
		done <- err
	}()
	waitQueueDepth(t, n, 1)
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	waitQueueDepth(t, n, 0)
}

func waitQueueDepth(t *testing.T, n *NodeSession, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.QueueDepth() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue depth = %d, want %d", n.QueueDepth(), want)
}
