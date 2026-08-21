package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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

// writeConfig drops a minimal server config and returns its path.
func writeConfig(t *testing.T, listen, statusListen string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := fmt.Sprintf("listen: %q\n", listen)
	if statusListen != "" {
		body += fmt.Sprintf("status_listen: %q\n", statusListen)
	}
	body += "nodes:\n  node1:\n    key: k1\n    ports: []\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// newTestServer starts a server on ephemeral addresses and returns it once both
// endpoints are answering.
func newTestServer(t *testing.T, statusListen string) (*Server, context.CancelFunc) {
	t.Helper()
	path := writeConfig(t, freeAddr(t), statusListen)
	srv, err := New(path, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("server did not shut down")
		}
	})
	return srv, cancel
}

func TestStatusEndpointServesTheDocument(t *testing.T) {
	addr := freeAddr(t)
	newTestServer(t, addr)

	body := getWithRetry(t, "http://"+addr+"/status")
	var doc statusDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("unmarshal %s: %v", body, err)
	}
	// node1 is configured but never connected, so it belongs in offline_nodes.
	if len(doc.Nodes) != 0 {
		t.Errorf("nodes = %+v, want empty with no client connected", doc.Nodes)
	}
	if len(doc.OfflineNodes) != 1 || doc.OfflineNodes[0].Node != "node1" {
		t.Fatalf("offline_nodes = %+v, want just node1", doc.OfflineNodes)
	}
	if doc.OfflineNodes[0].Up {
		t.Error("offline node reports up = true")
	}

	metrics := string(getWithRetry(t, "http://"+addr+"/metrics"))
	for _, want := range []string{
		"# TYPE tunnel_node_up gauge",
		"# TYPE tunnel_stream_open_total counter",
		"# TYPE tunnel_port_bind_errors_total counter",
		`tunnel_node_up{node="node1"} 0`,
	} {
		if !containsLine(metrics, want) {
			t.Errorf("/metrics is missing %q\n%s", want, metrics)
		}
	}
	// Nothing may be emitted under a family it was not declared as.
	if containsLine(metrics, "# ERROR") {
		t.Errorf("/metrics reported a type mismatch\n%s", metrics)
	}
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

// containsLine matches a whole line or a line prefix, so a metric can be found
// without pinning the label order of the rest of the line.
func containsLine(haystack, needle string) bool {
	for line := range strings.Lines(haystack) {
		if strings.HasPrefix(strings.TrimSuffix(line, "\n"), needle) {
			return true
		}
	}
	return false
}
