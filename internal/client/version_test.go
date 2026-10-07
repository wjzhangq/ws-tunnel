package client

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"

	"ws-tunnel/internal/protocol"
	"ws-tunnel/internal/wsutil"
)

// TestWelcomeFromOldServerIsRefused: a pre-mux server accepts the hello but
// echoes no proto; carrying on would leave every stream silently dead.
func TestWelcomeFromOldServerIsRefused(t *testing.T) {
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		if _, err := wsutil.ReadJSON(r.Context(), c); err != nil {
			return
		}
		_ = wsutil.WriteJSON(r.Context(), c, &protocol.Message{
			Type: protocol.TypeWelcome, Session: "s1",
			Config: &protocol.NodeConfig{Ports: map[string]string{}},
		})
		_, _, _ = c.Read(r.Context())
	}))
	defer hs.Close()

	c := &Client{
		URL: "ws" + strings.TrimPrefix(hs.URL, "http") + protocol.WSPath,
		Key: "k",
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	err := c.runSession(t.Context())
	if err == nil || !strings.Contains(err.Error(), "upgrade the server") {
		t.Fatalf("runSession err = %v, want a protocol version error", err)
	}
	if c.Session() != "" {
		t.Fatal("no session may be recorded from an incompatible server")
	}
}
