package client

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"ws-tunnel/internal/protocol"
)

func testClient() *Client {
	return &Client{
		URL: "ws://127.0.0.1:8443/ws",
		Key: "secret",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestStatusBeforeFirstHandshake(t *testing.T) {
	c := testClient()
	st := c.Status()

	if st.Connected {
		t.Error("connected = true before any handshake")
	}
	if st.Session != "" {
		t.Errorf("session = %q, want empty", st.Session)
	}
	if st.LastError != nil {
		t.Errorf("last_error = %v, want null", *st.LastError)
	}
	if st.Channels.Configured != 0 || st.Channels.Online != 0 {
		t.Errorf("channels = %+v, want zeroes", st.Channels)
	}
	// Ports must marshal as [] rather than null so a poller can iterate it
	// unconditionally.
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"ports":[]`) {
		t.Errorf("ports did not marshal as an empty array: %s", b)
	}
}

func TestStatusReflectsSessionState(t *testing.T) {
	c := testClient()
	c.setConfig(&protocol.NodeConfig{
		Channels: 4,
		Ports: map[string]string{
			"15432": "127.0.0.1:5432",
			"1980":  "127.0.0.1:80",
			"19080": "127.0.0.1:8080",
		},
	})
	c.setSession("sess-1")
	c.channelsOnline.Store(3)
	c.activeStreams.Store(47)
	c.bytesIn.Store(1024)
	c.bytesOut.Store(2048)
	c.localDialErrors.Store(2)
	c.setLastError("dial tcp: connection refused")
	c.draining.Store(true)

	st := c.Status()
	if st.Session != "sess-1" || !st.Draining {
		t.Errorf("session/draining = %q/%v", st.Session, st.Draining)
	}
	if st.Channels.Configured != 4 || st.Channels.Online != 3 {
		t.Errorf("channels = %+v, want 4 configured / 3 online", st.Channels)
	}
	if st.Streams.Active != 47 {
		t.Errorf("active streams = %d, want 47", st.Streams.Active)
	}
	if st.Traffic.BytesIn != 1024 || st.Traffic.BytesOut != 2048 || st.Traffic.LocalDialErrors != 2 {
		t.Errorf("traffic = %+v", st.Traffic)
	}
	if st.LastError == nil || *st.LastError != "dial tcp: connection refused" {
		t.Errorf("last_error = %v", st.LastError)
	}
	// Ports come from a map upstream; the output must be numerically ordered so
	// successive polls are comparable.
	want := []string{"1980", "15432", "19080"}
	if len(st.Ports) != len(want) {
		t.Fatalf("ports = %+v, want %d entries", st.Ports, len(want))
	}
	for i, p := range st.Ports {
		if p.Port != want[i] {
			t.Fatalf("port order = %+v, want %v", st.Ports, want)
		}
	}
}

func TestServeStatusRespondsAndShutsDown(t *testing.T) {
	c := testClient()
	c.setConfig(&protocol.NodeConfig{Channels: 2, Ports: map[string]string{"19080": "127.0.0.1:8080"}})
	c.setSession("sess-2")

	addr := freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- c.ServeStatus(ctx, addr) }()

	body := getWithRetry(t, "http://"+addr+"/status")
	var st ClientStatus
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	if st.Session != "sess-2" || st.Channels.Configured != 2 {
		t.Errorf("served document = %+v", st)
	}

	// Cancelling the context must shut the endpoint down cleanly, not report
	// an error.
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ServeStatus returned %v, want nil on context cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("ServeStatus did not return after the context was cancelled")
	}
}

func TestServeStatusReportsBindFailure(t *testing.T) {
	// Hold the address so the bind cannot succeed.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	err = testClient().ServeStatus(context.Background(), ln.Addr().String())
	if err == nil {
		t.Fatal("ServeStatus returned nil for an address already in use")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func getWithRetry(t *testing.T, url string) []byte {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s: status %d", url, resp.StatusCode)
			}
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			return b
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET %s never succeeded: %v", url, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
