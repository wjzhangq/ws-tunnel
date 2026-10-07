// Package protocol defines the JSON control messages shared by tunnel-server
// and tunnel-client, plus the OPEN_ACK status codes. The binary stream frames
// live in package mux.
package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

// WSPath is the HTTP path clients connect to on the server's `listen` address.
const WSPath = "/ws"

// Version is carried in `hello` and echoed in `welcome`. Version 2 is the
// single-WebSocket mux; releases before it sent no version (0) and expected
// separate data-role WebSockets, so the two must refuse each other.
const Version = 2

// Control message types (§6).
const (
	TypeHello        = "hello"
	TypeWelcome      = "welcome"
	TypeReloadConfig = "reload_config"
	TypePing         = "ping"
	TypePong         = "pong"
	TypeStats        = "stats"
	TypeDrain        = "drain"
	TypeBye          = "bye"
	TypeError        = "error"
)

// RoleControl is the only role carried in `hello`.
const RoleControl = "control"

// Error codes carried in `error` (§6 / §4).
const (
	ErrAuthFailed = "auth_failed"
	ErrBadRequest = "bad_request"
	ErrInternal   = "internal"
	ErrDraining   = "draining"
	ErrVersion    = "unsupported_version"
)

// Duration marshals as a Go duration string ("15s") so the JSON `config`
// blob matches the shape documented in §6.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("parse duration %q: %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	// Tolerate a raw nanosecond count as well.
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration must be a string or integer")
	}
	*d = Duration(n)
	return nil
}

// NodeConfig is the full configuration the server pushes to a node, both in
// `welcome.config` and in every `reload_config` (§4, §6). It is always sent in
// full — the client replaces its view wholesale.
type NodeConfig struct {
	// Ports maps the server-side listening port (decimal string, which is also
	// the port id on the wire) to the client-local `host:port` to dial.
	Ports             map[string]string `json:"ports"`
	Heartbeat         Duration          `json:"heartbeat"`
	DialTimeout       Duration          `json:"dial_timeout"`
	MaxStreamsPerConn int               `json:"max_streams_per_conn"`
	StreamWindow      int               `json:"stream_window,omitempty"`
}

// Equal reports whether two node configs are identical. Used by the reload
// diff to decide whether a `reload_config` push is needed at all (§12.2).
func (c *NodeConfig) Equal(o *NodeConfig) bool {
	if c == nil || o == nil {
		return c == o
	}
	if c.Heartbeat != o.Heartbeat ||
		c.DialTimeout != o.DialTimeout || c.MaxStreamsPerConn != o.MaxStreamsPerConn ||
		c.StreamWindow != o.StreamWindow {
		return false
	}
	if len(c.Ports) != len(o.Ports) {
		return false
	}
	for k, v := range c.Ports {
		if o.Ports[k] != v {
			return false
		}
	}
	return true
}

// Stats is the client-side telemetry report, sent at the heartbeat interval
// (§6, §8).
type Stats struct {
	Channels        int   `json:"channels"`
	Streams         int   `json:"streams"`
	BytesIn         int64 `json:"bytes_in"`
	BytesOut        int64 `json:"bytes_out"`
	LocalDialErrors int64 `json:"local_dial_errors"`
}

// Message is the union of every control message. Unused fields are omitted so
// the wire stays readable when debugging with a WS inspector.
type Message struct {
	Type string `json:"type"`

	// hello / welcome
	Proto int `json:"proto,omitempty"`

	// hello
	Role string `json:"role,omitempty"`
	Node string `json:"node,omitempty"`
	Key  string `json:"key,omitempty"`

	// welcome
	Session   string      `json:"session,omitempty"`
	Heartbeat Duration    `json:"heartbeat,omitempty"`
	Config    *NodeConfig `json:"config,omitempty"`

	// ping / pong
	Nonce string `json:"nonce,omitempty"`
	TS    int64  `json:"ts,omitempty"` // unix nanos, echoed back in pong

	// stats
	Stats *Stats `json:"stats,omitempty"`

	// drain
	Deadline *time.Time `json:"deadline,omitempty"`

	// bye
	Reason string `json:"reason,omitempty"`

	// error
	Code string `json:"code,omitempty"`
	Msg  string `json:"msg,omitempty"`
	Ref  string `json:"ref,omitempty"`
}

// Errorf builds an `error` message.
func Errorf(code, format string, args ...any) *Message {
	return &Message{Type: TypeError, Code: code, Msg: fmt.Sprintf(format, args...)}
}
