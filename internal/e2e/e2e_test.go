// Package e2e drives a real tunnel-server and tunnel-client over loopback:
// handshake, reverse listener, L4 forwarding, /status and hot reload.
package e2e

import (
	"context"
	"encoding/json"
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

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// echoServer answers every connection by echoing whatever it receives,
// prefixed with a tag so we can tell two backends apart.
func echoServer(t *testing.T, tag string) (addr string) {
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
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						if _, werr := c.Write(append([]byte(tag+":"), buf[:n]...)); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

func waitFor(t *testing.T, what string, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type statusDoc struct {
	Nodes []struct {
		Node     string `json:"node"`
		Up       bool   `json:"up"`
		Channels struct {
			Configured int `json:"configured"`
			Online     int `json:"online"`
		} `json:"channels"`
		Traffic struct {
			BytesIn  int64 `json:"bytes_in"`
			BytesOut int64 `json:"bytes_out"`
		} `json:"traffic"`
		Ports []struct {
			Port      int    `json:"port"`
			Remote    string `json:"remote"`
			Listening bool   `json:"listening"`
		} `json:"ports"`
	} `json:"nodes"`
	OfflineNodes []struct {
		Node string `json:"node"`
	} `json:"offline_nodes"`
}

func fetchStatus(t *testing.T, addr string) statusDoc {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var doc statusDoc
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

func roundTrip(t *testing.T, port int, payload string) (string, error) {
	t.Helper()
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		return "", err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte(payload)); err != nil {
		return "", err
	}
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil && n == 0 {
		return "", err
	}
	return string(buf[:n]), nil
}

func TestTunnelEndToEnd(t *testing.T) {
	backendA := echoServer(t, "A")
	backendB := echoServer(t, "B")

	wsPort := freePort(t)
	statusPort := freePort(t)
	revPortA := freePort(t)
	revPortB := freePort(t)

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	writeCfg := func(body string) {
		if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeCfg(fmt.Sprintf(`
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
`, wsPort, statusPort, revPortA, backendA))

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

	// The reverse port must NOT be open before the node connects (§9 step 1).
	if _, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", revPortA), 300*time.Millisecond); err == nil {
		t.Fatal("the reverse port must stay closed while the node is offline")
	}

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

	waitFor(t, "node1 to come online with both channels", 10*time.Second, func() bool {
		doc := fetchStatus(t, statusAddr)
		return len(doc.Nodes) == 1 && doc.Nodes[0].Up &&
			doc.Nodes[0].Channels.Online == 2 &&
			len(doc.Nodes[0].Ports) == 1 && doc.Nodes[0].Ports[0].Listening
	})

	t.Run("forwards raw bytes", func(t *testing.T) {
		got, err := roundTrip(t, revPortA, "ping")
		if err != nil {
			t.Fatal(err)
		}
		if got != "A:ping" {
			t.Fatalf("got %q, want %q", got, "A:ping")
		}
	})

	t.Run("many concurrent connections", func(t *testing.T) {
		const n = 24 // > channels, so streams share the smux sessions
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			go func(i int) {
				payload := fmt.Sprintf("msg-%d", i)
				got, err := roundTrip(t, revPortA, payload)
				if err != nil {
					errs <- err
					return
				}
				if got != "A:"+payload {
					errs <- fmt.Errorf("got %q, want %q", got, "A:"+payload)
					return
				}
				errs <- nil
			}(i)
		}
		for i := 0; i < n; i++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("status reports traffic", func(t *testing.T) {
		doc := fetchStatus(t, statusAddr)
		if doc.Nodes[0].Traffic.BytesIn == 0 || doc.Nodes[0].Traffic.BytesOut == 0 {
			t.Fatalf("byte counters look wrong: %+v", doc.Nodes[0].Traffic)
		}
	})

	t.Run("hot reload adds a port without a reconnect", func(t *testing.T) {
		writeCfg(fmt.Sprintf(`
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
  %d:
    node: node1
    remote: "%s"
`, wsPort, statusPort, revPortA, backendA, revPortB, backendB))

		waitFor(t, "the new reverse port", 15*time.Second, func() bool {
			doc := fetchStatus(t, statusAddr)
			if len(doc.Nodes) != 1 || len(doc.Nodes[0].Ports) != 2 {
				return false
			}
			for _, p := range doc.Nodes[0].Ports {
				if !p.Listening {
					return false
				}
			}
			return true
		})

		got, err := roundTrip(t, revPortB, "hi")
		if err != nil {
			t.Fatal(err)
		}
		if got != "B:hi" {
			t.Fatalf("the new port hit the wrong backend: %q", got)
		}
		// The pre-existing mapping keeps working — the reload was incremental.
		if got, err := roundTrip(t, revPortA, "still-here"); err != nil || got != "A:still-here" {
			t.Fatalf("the untouched port broke: %q %v", got, err)
		}
	})

	t.Run("removing a port releases the listener", func(t *testing.T) {
		writeCfg(fmt.Sprintf(`
listen: "127.0.0.1:%d"
status_listen: "127.0.0.1:%d"
settings:
  heartbeat: 1s
nodes:
  node1:
    key: "secret-1"
    channels: 2
ports:
  %d:
    node: node1
    remote: "%s"
`, wsPort, statusPort, revPortA, backendA))

		waitFor(t, "the removed port to be released", 15*time.Second, func() bool {
			c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", revPortB), 200*time.Millisecond)
			if err != nil {
				return true
			}
			c.Close()
			return false
		})
	})

	t.Run("a second client for the same node is refused", func(t *testing.T) {
		intruder := &client.Client{
			URL: fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort),
			Key: "secret-1",
			Log: log.With("side", "intruder"),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done := make(chan struct{})
		go func() { defer close(done); _ = intruder.Run(ctx) }()
		<-ctx.Done()
		<-done

		// The incumbent is untouched.
		if got, err := roundTrip(t, revPortA, "alive"); err != nil || got != "A:alive" {
			t.Fatalf("the incumbent session was disturbed: %q %v", got, err)
		}
	})

	t.Run("node offline closes its ports", func(t *testing.T) {
		stopCli()
		<-cliDone
		waitFor(t, "the ports to close after the node left", 10*time.Second, func() bool {
			c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", revPortA), 200*time.Millisecond)
			if err == nil {
				c.Close()
				return false
			}
			doc := fetchStatus(t, statusAddr)
			return len(doc.Nodes) == 0 && len(doc.OfflineNodes) == 1
		})
	})
}

func TestUnknownKeyIsRefused(t *testing.T) {
	wsPort := freePort(t)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")
	body := fmt.Sprintf("listen: \"127.0.0.1:%d\"\nnodes:\n  node1: {key: \"good\"}\n", wsPort)
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv, err := server.New(cfgPath, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = srv.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor(t, "the ws entry point", 5*time.Second, func() bool {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", wsPort), 200*time.Millisecond)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})

	cli := &client.Client{
		URL: fmt.Sprintf("ws://127.0.0.1:%d/ws", wsPort),
		Key: "wrong",
		Log: log,
	}
	cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ccancel()
	_ = cli.Run(cctx) // must keep retrying without ever coming online
	if cli.Session() != "" {
		t.Fatal("a bad key must never yield a session")
	}
}
