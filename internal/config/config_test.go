package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaultsAndValidation(t *testing.T) {
	p := write(t, `
listen: ":8443"
status_listen: "127.0.0.1:8090"
settings:
  heartbeat: 5s
nodes:
  node1:
    key: "xx1"
    channels: 2
  node2:
    key: "xx1"        # duplicate key: dropped, node1 wins
    channels: 3
  node3:
    key: "xx3"
    channels: 0       # invalid: falls back to the default
ports:
  19080:
    node: node1
    remote: "127.0.0.1:8080"
  15432:
    node: node2       # node2 was dropped, so this goes too
    remote: "127.0.0.1:5432"
  8090:
    node: node1       # collides with status_listen
    remote: "127.0.0.1:1"
  70000:
    node: node1       # out of range
    remote: "127.0.0.1:1"
  1980:
    node: node1
    remote: "not-a-host-port"
`)
	cfg, warnings, err := Load(p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 6 {
		t.Fatalf("expected 6 warnings, got %d: %v", len(warnings), warnings)
	}
	if _, ok := cfg.Nodes["node2"]; ok {
		t.Error("node2 should have been dropped for reusing node1's key")
	}
	if cfg.Nodes["node3"].Channels != DefaultChannels {
		t.Errorf("channels=0 should fall back to %d, got %d", DefaultChannels, cfg.Nodes["node3"].Channels)
	}
	if cfg.Settings.Heartbeat != 5*time.Second {
		t.Errorf("heartbeat not parsed: %v", cfg.Settings.Heartbeat)
	}
	if cfg.Settings.DialTimeout != DefaultDialTimeout || cfg.Settings.MaxStreamsPerConn != DefaultMaxStreamsPerConn {
		t.Error("settings defaults not applied")
	}
	if len(cfg.Ports) != 1 || cfg.Ports[19080] == nil {
		t.Fatalf("only port 19080 should survive, got %v", cfg.SortedPorts())
	}
}

func TestDuplicateKeyKeepsDocumentOrder(t *testing.T) {
	// Document order must decide the winner, which is why parsing goes
	// through yaml.Node instead of a Go map (§3.3).
	for i := 0; i < 20; i++ {
		p := write(t, `
listen: ":8443"
nodes:
  alpha:
    key: "same"
  beta:
    key: "same"
`)
		cfg, _, err := Load(p)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := cfg.Nodes["alpha"]; !ok {
			t.Fatal("alpha (first in the document) must win")
		}
		if _, ok := cfg.Nodes["beta"]; ok {
			t.Fatal("beta must be dropped")
		}
	}
}

func TestListenRequired(t *testing.T) {
	if _, _, err := Load(write(t, "nodes: {}\n")); err == nil {
		t.Fatal("a missing `listen` must be an error")
	}
}

func TestNodeConfigAndByKey(t *testing.T) {
	p := write(t, `
listen: ":8443"
nodes:
  node1: {key: "xx1", channels: 3}
ports:
  19080: {node: node1, remote: "127.0.0.1:8080"}
  15432: {node: node1, remote: "127.0.0.1:5432"}
`)
	cfg, _, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	nc := cfg.NodeConfig("node1")
	if nc.Channels != 3 || len(nc.Ports) != 2 || nc.Ports["19080"] != "127.0.0.1:8080" {
		t.Fatalf("unexpected node config: %+v", nc)
	}
	if cfg.NodeByKey("xx1").Name != "node1" || cfg.NodeByKey("nope") != nil {
		t.Fatal("NodeByKey is broken")
	}
}

func TestDiff(t *testing.T) {
	base := `
listen: ":8443"
nodes:
  n1: {key: "k1", channels: 2}
  n2: {key: "k2"}
ports:
  1001: {node: n1, remote: "127.0.0.1:1"}
  1002: {node: n2, remote: "127.0.0.1:2"}
`
	next := `
listen: ":8443"
nodes:
  n1: {key: "k1-rotated", channels: 2}
  n3: {key: "k3"}
ports:
  1001: {node: n3, remote: "127.0.0.1:1"}
  1003: {node: n1, remote: "127.0.0.1:3"}
`
	oldCfg, _, err := Load(write(t, base))
	if err != nil {
		t.Fatal(err)
	}
	newCfg, _, err := Load(write(t, next))
	if err != nil {
		t.Fatal(err)
	}
	d := DiffConfig(oldCfg, newCfg)

	if len(d.RemovedNodes) != 1 || d.RemovedNodes[0] != "n2" {
		t.Errorf("removed nodes: %v", d.RemovedNodes)
	}
	if len(d.AddedNodes) != 1 || d.AddedNodes[0] != "n3" {
		t.Errorf("added nodes: %v", d.AddedNodes)
	}
	if len(d.KeyChangedNodes) != 1 || d.KeyChangedNodes[0] != "n1" {
		t.Errorf("key-changed nodes: %v", d.KeyChangedNodes)
	}
	// 1001 moved to another node ⇒ delete + add; 1002 gone; 1003 new.
	if got := d.RemovedPorts; len(got) != 2 || got[0] != 1001 || got[1] != 1002 {
		t.Errorf("removed ports: %v", got)
	}
	if got := d.AddedPorts; len(got) != 2 || got[0] != 1001 || got[1] != 1003 {
		t.Errorf("added ports: %v", got)
	}
	// n1's key changed, so it gets disconnected rather than a config push.
	for _, n := range d.ChangedNodes {
		if n == "n1" {
			t.Error("a key-changed node must not also get a reload_config push")
		}
	}
}

func TestDiffRemoteOnlyChange(t *testing.T) {
	base := "listen: \":8443\"\nnodes:\n  n1: {key: k1}\nports:\n  1001: {node: n1, remote: \"127.0.0.1:1\"}\n"
	next := "listen: \":8443\"\nnodes:\n  n1: {key: k1}\nports:\n  1001: {node: n1, remote: \"127.0.0.1:9\"}\n"
	oldCfg, _, _ := Load(write(t, base))
	newCfg, _, _ := Load(write(t, next))
	d := DiffConfig(oldCfg, newCfg)
	if len(d.AddedPorts) != 0 || len(d.RemovedPorts) != 0 {
		t.Error("a remote change must not touch the listener")
	}
	if len(d.ChangedNodes) != 1 || d.ChangedNodes[0] != "n1" {
		t.Errorf("the node should get a reload_config push: %v", d.ChangedNodes)
	}
}
