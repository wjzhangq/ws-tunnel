// Package config loads, validates and diffs the server's YAML configuration.
//
// Validation follows §3.3: apart from a YAML syntax error (or a missing
// `listen`), a bad entry never blocks startup — it is dropped and reported as
// a warning. Because "keep the first occurrence" is only meaningful with a
// stable order, `nodes` and `ports` are decoded through yaml.Node rather than
// a Go map.
package config

import (
	"crypto/subtle"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	"ws-tunnel/internal/protocol"
)

// Defaults for settings.* (§3.2).
const (
	DefaultHeartbeat         = 15 * time.Second
	DefaultDialTimeout       = 10 * time.Second
	DefaultQueueTimeout      = 5 * time.Second
	DefaultMaxStreamsPerConn = 256
	DefaultChannels          = 4

	// ListenHost is fixed by design: reverse listeners always bind loopback
	// and there is no config knob for it (§3.2, §16).
	ListenHost = "127.0.0.1"
)

// Settings holds the global settings block.
type Settings struct {
	Heartbeat         time.Duration
	DialTimeout       time.Duration
	QueueTimeout      time.Duration
	MaxStreamsPerConn int
}

// NodeSpec is one entry under `nodes`.
type NodeSpec struct {
	Name     string
	Key      string
	Channels int
}

// PortSpec is one entry under `ports`. Port doubles as the wire-level port id.
type PortSpec struct {
	Port   int
	Node   string
	Remote string
}

// Config is a validated snapshot of config.yaml.
type Config struct {
	Path         string
	Listen       string
	StatusListen string
	Settings     Settings

	Nodes     map[string]*NodeSpec
	NodeOrder []string

	Ports     map[int]*PortSpec
	PortOrder []int
}

type rawSettings struct {
	Heartbeat         string `yaml:"heartbeat"`
	DialTimeout       string `yaml:"dial_timeout"`
	QueueTimeout      string `yaml:"queue_timeout"`
	MaxStreamsPerConn int    `yaml:"max_streams_per_conn"`
}

type rawFile struct {
	Listen       string      `yaml:"listen"`
	StatusListen string      `yaml:"status_listen"`
	Settings     rawSettings `yaml:"settings"`
	Nodes        yaml.Node   `yaml:"nodes"`
	Ports        yaml.Node   `yaml:"ports"`
}

type rawNode struct {
	Key      string `yaml:"key"`
	Channels *int   `yaml:"channels"`
}

type rawPort struct {
	Node   string `yaml:"node"`
	Remote string `yaml:"remote"`
}

// Load reads and validates a config file. The returned warnings correspond to
// dropped or corrected entries; they are non-fatal by design. An error is
// returned only when the file is unreadable, the YAML is malformed, or
// `listen` is missing — in which case the caller keeps the previous config
// (reload) or exits (startup).
func Load(path string) (cfg *Config, warnings []string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read config: %w", err)
	}
	var raw rawFile
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, nil, fmt.Errorf("parse config: %w", err)
	}

	warn := func(format string, args ...any) {
		warnings = append(warnings, fmt.Sprintf(format, args...))
	}

	cfg = &Config{
		Path:         path,
		Listen:       raw.Listen,
		StatusListen: raw.StatusListen,
		Nodes:        map[string]*NodeSpec{},
		Ports:        map[int]*PortSpec{},
	}
	if cfg.Listen == "" {
		return nil, warnings, fmt.Errorf("`listen` is required")
	}

	cfg.Settings = Settings{
		Heartbeat:         parseDuration(raw.Settings.Heartbeat, DefaultHeartbeat, "settings.heartbeat", warn),
		DialTimeout:       parseDuration(raw.Settings.DialTimeout, DefaultDialTimeout, "settings.dial_timeout", warn),
		QueueTimeout:      parseDuration(raw.Settings.QueueTimeout, DefaultQueueTimeout, "settings.queue_timeout", warn),
		MaxStreamsPerConn: raw.Settings.MaxStreamsPerConn,
	}
	if cfg.Settings.MaxStreamsPerConn < 1 {
		if raw.Settings.MaxStreamsPerConn != 0 {
			warn("settings.max_streams_per_conn=%d is invalid, using %d",
				raw.Settings.MaxStreamsPerConn, DefaultMaxStreamsPerConn)
		}
		cfg.Settings.MaxStreamsPerConn = DefaultMaxStreamsPerConn
	}

	reserved := map[int]string{}
	if p, ok := portOf(cfg.Listen); ok {
		reserved[p] = "listen"
	}
	if p, ok := portOf(cfg.StatusListen); ok {
		reserved[p] = "status_listen"
	}

	// ── nodes ────────────────────────────────────────────────────────────
	keyOwner := map[string]string{} // key -> first node that claimed it
	for _, kv := range mappingPairs(&raw.Nodes, "nodes", warn) {
		name := kv.key
		var rn rawNode
		if err := kv.value.Decode(&rn); err != nil {
			warn("nodes.%s: %v — entry dropped", name, err)
			continue
		}
		if _, dup := cfg.Nodes[name]; dup {
			warn("nodes.%s: duplicate node name — later entry dropped", name)
			continue
		}
		if rn.Key == "" {
			warn("nodes.%s: `key` is required — entry dropped", name)
			continue
		}
		// §3.3: duplicate keys keep the first occurrence in document order.
		if owner, dup := keyOwner[rn.Key]; dup {
			warn("nodes.%s: key already used by node %q — entry dropped", name, owner)
			continue
		}
		channels := DefaultChannels
		if rn.Channels != nil {
			channels = *rn.Channels
			if channels < 1 {
				warn("nodes.%s: channels=%d is invalid, using %d", name, channels, DefaultChannels)
				channels = DefaultChannels
			}
		}
		keyOwner[rn.Key] = name
		cfg.Nodes[name] = &NodeSpec{Name: name, Key: rn.Key, Channels: channels}
		cfg.NodeOrder = append(cfg.NodeOrder, name)
	}

	// ── ports ────────────────────────────────────────────────────────────
	for _, kv := range mappingPairs(&raw.Ports, "ports", warn) {
		port, err := strconv.Atoi(kv.key)
		if err != nil {
			warn("ports.%s: key must be a plain port number — entry dropped", kv.key)
			continue
		}
		if port < 1 || port > 65535 {
			warn("ports.%d: port out of range 1..65535 — entry dropped", port)
			continue
		}
		if who, clash := reserved[port]; clash {
			warn("ports.%d: conflicts with %s — entry dropped", port, who)
			continue
		}
		if _, dup := cfg.Ports[port]; dup {
			warn("ports.%d: duplicate port — later entry dropped", port)
			continue
		}
		var rp rawPort
		if err := kv.value.Decode(&rp); err != nil {
			warn("ports.%d: %v — entry dropped", port, err)
			continue
		}
		if _, ok := cfg.Nodes[rp.Node]; !ok {
			warn("ports.%d: node %q does not exist (or was dropped) — entry dropped", port, rp.Node)
			continue
		}
		if err := validateHostPort(rp.Remote); err != nil {
			warn("ports.%d: remote %q is invalid (%v) — entry dropped", port, rp.Remote, err)
			continue
		}
		cfg.Ports[port] = &PortSpec{Port: port, Node: rp.Node, Remote: rp.Remote}
		cfg.PortOrder = append(cfg.PortOrder, port)
	}

	return cfg, warnings, nil
}

// NodeConfig builds the blob pushed to a node in `welcome` and
// `reload_config` (§6). Returns nil if the node is not configured.
func (c *Config) NodeConfig(node string) *protocol.NodeConfig {
	spec, ok := c.Nodes[node]
	if !ok {
		return nil
	}
	ports := map[string]string{}
	for _, p := range c.PortOrder {
		if ps := c.Ports[p]; ps.Node == node {
			ports[strconv.Itoa(p)] = ps.Remote
		}
	}
	return &protocol.NodeConfig{
		Ports:             ports,
		Channels:          spec.Channels,
		Heartbeat:         protocol.Duration(c.Settings.Heartbeat),
		DialTimeout:       protocol.Duration(c.Settings.DialTimeout),
		MaxStreamsPerConn: c.Settings.MaxStreamsPerConn,
	}
}

// PortsOf returns the node's port entries in document order.
func (c *Config) PortsOf(node string) []*PortSpec {
	var out []*PortSpec
	for _, p := range c.PortOrder {
		if ps := c.Ports[p]; ps.Node == node {
			out = append(out, ps)
		}
	}
	return out
}

// NodeByKey resolves a node from its key alone. Keys are globally unique
// (§3.2), which is what lets a client start with nothing but `url + key`.
//
// The comparison is constant-time and the loop does not exit early, matching the
// named-node path in the server's authenticate(): this is the path a client with
// no --node takes, so a plain == here would leave the timing side channel the
// other path was written to avoid.
func (c *Config) NodeByKey(key string) *NodeSpec {
	if key == "" {
		return nil
	}
	var match *NodeSpec
	for _, name := range c.NodeOrder {
		n := c.Nodes[name]
		if n == nil {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(n.Key), []byte(key)) == 1 && match == nil {
			match = n
		}
	}
	return match
}

// SortedPorts returns every configured port, ascending.
func (c *Config) SortedPorts() []int {
	out := make([]int, 0, len(c.Ports))
	for p := range c.Ports {
		out = append(out, p)
	}
	sort.Ints(out)
	return out
}

// ─── helpers ─────────────────────────────────────────────────────────────

type kvPair struct {
	key   string
	value *yaml.Node
}

// mappingPairs walks a YAML mapping in document order.
func mappingPairs(n *yaml.Node, section string, warn func(string, ...any)) []kvPair {
	if n == nil || n.Kind == 0 {
		return nil
	}
	if n.Kind != yaml.MappingNode {
		warn("%s: expected a mapping, got %s — section ignored", section, kindName(n.Kind))
		return nil
	}
	out := make([]kvPair, 0, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		out = append(out, kvPair{key: n.Content[i].Value, value: n.Content[i+1]})
	}
	return out
}

func kindName(k yaml.Kind) string {
	switch k {
	case yaml.DocumentNode:
		return "document"
	case yaml.SequenceNode:
		return "sequence"
	case yaml.MappingNode:
		return "mapping"
	case yaml.ScalarNode:
		return "scalar"
	case yaml.AliasNode:
		return "alias"
	}
	return "unknown"
}

func parseDuration(s string, def time.Duration, field string, warn func(string, ...any)) time.Duration {
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		warn("%s=%q is invalid, using %s", field, s, def)
		return def
	}
	return d
}

func validateHostPort(s string) error {
	if s == "" {
		return fmt.Errorf("empty")
	}
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return err
	}
	if host == "" {
		return fmt.Errorf("missing host")
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("bad port %q", port)
	}
	return nil
}

// portOf extracts the numeric port from a listen address such as ":8443".
func portOf(addr string) (int, bool) {
	if addr == "" {
		return 0, false
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, false
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return 0, false
	}
	return p, true
}

// ListenAddr is the bind address for a reverse port: always loopback.
func ListenAddr(port int) string {
	return net.JoinHostPort(ListenHost, strconv.Itoa(port))
}
