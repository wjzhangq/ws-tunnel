package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"ws-tunnel/internal/client"
	"ws-tunnel/internal/server"
)

// drainThenReply is a backend that reads until EOF before answering. Unlike
// echoServer it cannot reply at all unless the external client's FIN travels
// the whole way through the tunnel, which is exactly what this file guards.
func drainThenReply(t *testing.T, tag string) (addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				body, err := io.ReadAll(c)
				if err != nil {
					return
				}
				_, _ = c.Write([]byte(tag + ":" + string(body)))
			}(c)
		}
	}()
	return ln.Addr().String()
}

// startTunnel stands up a server plus one connected client and returns the
// reverse port once it is listening.
func startTunnel(t *testing.T, backend string) (revPort int) {
	t.Helper()
	wsPort := freePort(t)
	statusPort := freePort(t)
	revPort = freePort(t)

	cfgPath := filepath.Join(t.TempDir(), "config.yaml")
	body := fmt.Sprintf(`
listen: "127.0.0.1:%d"
status_listen: "127.0.0.1:%d"
settings:
  heartbeat: 1s
  dial_timeout: 2s
  queue_timeout: 2s
  max_streams_per_conn: 8
nodes:
  node1:
    key: "secret-1"
    channels: 2
ports:
  %d:
    node: node1
    remote: "%s"
`, wsPort, statusPort, revPort, backend)
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}

	srv, err := server.New(cfgPath, log.With("side", "server"))
	if err != nil {
		t.Fatal(err)
	}
	srvCtx, stopSrv := context.WithCancel(context.Background())
	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		if err := srv.Run(srvCtx); err != nil {
			t.Errorf("server: %v", err)
		}
	}()
	t.Cleanup(func() {
		stopSrv()
		select {
		case <-srvDone:
		case <-time.After(10 * time.Second):
			t.Error("server did not shut down in time")
		}
	})

	statusAddr := fmt.Sprintf("127.0.0.1:%d", statusPort)
	waitFor(t, "the status endpoint", 5*time.Second, func() bool {
		resp, err := http.Get("http://" + statusAddr + "/status")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return true
	})

	cli := &client.Client{
		URL: fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort),
		Key: "secret-1",
		Log: log.With("side", "client"),
	}
	cliCtx, stopCli := context.WithCancel(context.Background())
	cliDone := make(chan struct{})
	go func() {
		defer close(cliDone)
		_ = cli.Run(cliCtx)
	}()
	t.Cleanup(func() {
		stopCli()
		<-cliDone
	})

	waitFor(t, "node1 to come online", 10*time.Second, func() bool {
		doc := fetchStatus(t, statusAddr)
		return len(doc.Nodes) == 1 && doc.Nodes[0].Up &&
			doc.Nodes[0].Channels.Online == 2 &&
			len(doc.Nodes[0].Ports) == 1 && doc.Nodes[0].Ports[0].Listening
	})
	return revPort
}

// halfCloseRoundTrip sends payload, half-closes, and reads the answer to EOF.
func halfCloseRoundTrip(t *testing.T, port int, payload []byte) []byte {
	t.Helper()
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The whole point: the backend only answers once this FIN has travelled
	// server → tunnel → client → local service.
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	got, err := io.ReadAll(c)
	if err != nil {
		t.Fatalf("read after half-close: %v", err)
	}
	return got
}

// TestHalfCloseDeliversFIN guards the framed payload (§7.1, StreamVersion 0x02):
// an external client that half-closes must still receive the reply, and the
// backend must see EOF rather than hanging until a timeout.
func TestHalfCloseDeliversFIN(t *testing.T) {
	port := startTunnel(t, drainThenReply(t, "drained"))
	got := string(halfCloseRoundTrip(t, port, []byte("ping")))
	if got != "drained:ping" {
		t.Fatalf("got %q, want %q", got, "drained:ping")
	}
}

// TestHalfCloseLargePayload pushes enough bytes to span many frames in both
// directions, checking the length-delimited framing reassembles exactly.
func TestHalfCloseLargePayload(t *testing.T) {
	port := startTunnel(t, drainThenReply(t, "big"))

	const size = 512 * 1024 // > 16 io.Copy buffers, so plenty of frames
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	got := halfCloseRoundTrip(t, port, payload)
	want := append([]byte("big:"), payload...)
	if len(got) != len(want) {
		t.Fatalf("got %d bytes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("payload differs at byte %d: got %#x, want %#x", i, got[i], want[i])
		}
	}
}

// TestHalfCloseRepeated runs several half-closed connections over the same
// tunnel so a leaked stream or an unbalanced slot would show up as a hang.
func TestHalfCloseRepeated(t *testing.T) {
	port := startTunnel(t, drainThenReply(t, "n"))
	for i := 0; i < 20; i++ {
		payload := fmt.Sprintf("req-%d", i)
		got := string(halfCloseRoundTrip(t, port, []byte(payload)))
		if want := "n:" + payload; got != want {
			t.Fatalf("iteration %d: got %q, want %q", i, got, want)
		}
	}
}
