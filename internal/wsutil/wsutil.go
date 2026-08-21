// Package wsutil holds the small amount of glue between coder/websocket,
// JSON control messages and smux.
package wsutil

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/coder/websocket"
	"github.com/xtaci/smux"

	"ws-tunnel/internal/protocol"
)

// ReadLimit must exceed smux's maximum frame size (64 KiB). coder/websocket
// defaults to 32 KiB, which would tear down the connection on the first large
// frame — see §7 "实现注意".
const ReadLimit = 1 << 20 // 1 MiB

// Prepare applies the settings every tunnel WS connection needs.
func Prepare(c *websocket.Conn) {
	c.SetReadLimit(ReadLimit)
}

// WriteJSON sends a control message as a text frame.
func WriteJSON(ctx context.Context, c *websocket.Conn, msg *protocol.Message) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, b)
}

// ReadJSON reads one control message. Binary frames are rejected: after the
// handshake a data channel is handed to smux, and the control channel never
// carries anything but JSON.
func ReadJSON(ctx context.Context, c *websocket.Conn) (*protocol.Message, error) {
	typ, b, err := c.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return nil, fmt.Errorf("expected a text control frame, got %v", typ)
	}
	var msg protocol.Message
	if err := json.Unmarshal(b, &msg); err != nil {
		return nil, fmt.Errorf("malformed control message: %w", err)
	}
	return &msg, nil
}

// NetConn wraps a websocket connection as a net.Conn carrying binary frames,
// which is what smux runs on.
func NetConn(ctx context.Context, c *websocket.Conn) net.Conn {
	return websocket.NetConn(ctx, c, websocket.MessageBinary)
}

// SmuxConfig returns the shared smux tuning. Both ends must agree on the
// version; keepalive here is what detects a dead data channel while the
// control channel is still up (§11).
func SmuxConfig(keepalive time.Duration) *smux.Config {
	cfg := smux.DefaultConfig()
	if keepalive > 0 {
		cfg.KeepAliveInterval = keepalive
		cfg.KeepAliveTimeout = 3 * keepalive
	}
	// MaxFrameSize must stay <= 65535 (smux hard limit) and below ReadLimit.
	cfg.MaxFrameSize = 32768
	return cfg
}

// Close closes a websocket connection, ignoring the "already closed" case.
func Close(c *websocket.Conn, code websocket.StatusCode, reason string) {
	if c == nil {
		return
	}
	_ = c.Close(code, reason)
}
