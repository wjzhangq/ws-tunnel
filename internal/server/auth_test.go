package server

import (
	"os"
	"path/filepath"
	"testing"

	"ws-tunnel/internal/config"
	"ws-tunnel/internal/protocol"
)

// authFixture loads a two-node config without starting any listener.
func authFixture(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	body := `listen: "127.0.0.1:0"
nodes:
  node1:
    key: key-one
    ports: []
  node2:
    key: key-two
    ports: []
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	srv, err := New(path, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv
}

// TestAuthenticateByKeyAlone covers §3.4 / HANDOFF §7: --node is optional, and
// the server resolves the node from the globally unique key.
func TestAuthenticateByKeyAlone(t *testing.T) {
	srv := authFixture(t)

	spec, err := srv.authenticate(&protocol.Message{Key: "key-two"})
	if err != nil {
		t.Fatalf("authenticate by key: %v", err)
	}
	if spec.Name != "node2" {
		t.Errorf("resolved node = %q, want node2", spec.Name)
	}
}

func TestAuthenticateByNameAndKey(t *testing.T) {
	srv := authFixture(t)

	spec, err := srv.authenticate(&protocol.Message{Node: "node1", Key: "key-one"})
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if spec.Name != "node1" {
		t.Errorf("resolved node = %q, want node1", spec.Name)
	}
}

func TestAuthenticateRejections(t *testing.T) {
	srv := authFixture(t)

	cases := []struct {
		name string
		msg  *protocol.Message
	}{
		{"empty key", &protocol.Message{Node: "node1"}},
		{"unknown node", &protocol.Message{Node: "nope", Key: "key-one"}},
		{"unknown key", &protocol.Message{Key: "key-three"}},
		// A valid key belonging to a different node must not authenticate the
		// named one.
		{"key of another node", &protocol.Message{Node: "node1", Key: "key-two"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if spec, err := srv.authenticate(tc.msg); err == nil {
				t.Fatalf("authenticate succeeded as %q, want an error", spec.Name)
			}
		})
	}
}

// TestAuthenticateUsesTheCurrentConfig checks a reloaded key takes effect for
// the next handshake, which is what makes KeyChangedNodes meaningful.
func TestAuthenticateUsesTheCurrentConfig(t *testing.T) {
	srv := authFixture(t)

	// Rotate node1's key, leaving node2 alone. The spec is copied rather than
	// mutated in place so the published config is replaced, not edited under
	// concurrent readers.
	next := *srv.Config()
	rotated := *next.Nodes["node1"]
	rotated.Key = "key-rotated"

	cloned := make(map[string]*config.NodeSpec, len(next.Nodes))
	for name, spec := range next.Nodes {
		cloned[name] = spec
	}
	cloned["node1"] = &rotated
	next.Nodes = cloned
	srv.setConfig(&next)

	if _, err := srv.authenticate(&protocol.Message{Node: "node1", Key: "key-one"}); err == nil {
		t.Error("the old key still authenticates after a rotation")
	}
	if _, err := srv.authenticate(&protocol.Message{Node: "node1", Key: "key-rotated"}); err != nil {
		t.Errorf("the rotated key was refused: %v", err)
	}
}
