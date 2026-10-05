// Package wsutil holds the small amount of glue used for the handshake
// JSON frames before the connection is handed to mux.
package wsutil

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coder/websocket"

	"ws-tunnel/internal/protocol"
)

// ReadLimit must exceed mux.MaxPayload. coder/websocket defaults to 32 KiB.
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

// ReadJSON reads one control message. Used only for the pre-mux handshake;
// after that, mux owns the socket and accepts mixed text/binary frames.
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

// Close closes a websocket connection, ignoring the "already closed" case.
func Close(c *websocket.Conn, code websocket.StatusCode, reason string) {
	if c == nil {
		return
	}
	_ = c.Close(code, reason)
}
