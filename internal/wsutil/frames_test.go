package wsutil

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"ws-tunnel/internal/protocol"
)

// wsPair returns a connected client/server websocket pair over a test server.
func wsPair(t *testing.T) (client, server *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		Prepare(c)
		accepted <- c
		<-r.Context().Done()
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cc, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		srv.Close()
		t.Fatalf("dial: %v", err)
	}
	Prepare(cc)

	select {
	case sc := <-accepted:
		t.Cleanup(func() {
			cc.CloseNow()
			sc.CloseNow()
			srv.Close()
		})
		return cc, sc
	case <-time.After(5 * time.Second):
		srv.Close()
		t.Fatal("server never accepted the connection")
		return nil, nil
	}
}

func TestWriteReadJSONRoundTrip(t *testing.T) {
	cc, sc := wsPair(t)
	ctx := context.Background()

	sent := &protocol.Message{
		Type: protocol.TypeHello,
		Role: protocol.RoleControl,
		Node: "node1",
		Key:  "k1",
	}
	if err := WriteJSON(ctx, cc, sent); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	got, err := ReadJSON(ctx, sc)
	if err != nil {
		t.Fatalf("ReadJSON: %v", err)
	}
	if got.Type != sent.Type || got.Role != sent.Role || got.Node != sent.Node || got.Key != sent.Key {
		t.Errorf("round trip lost data: got %+v, sent %+v", got, sent)
	}
}

// TestReadJSONRejectsBinaryFrames covers the control/data split: the control
// channel carries JSON text frames only, and binary belongs to smux.
func TestReadJSONRejectsBinaryFrames(t *testing.T) {
	cc, sc := wsPair(t)
	ctx := context.Background()

	if err := cc.Write(ctx, websocket.MessageBinary, []byte(`{"type":"hello"}`)); err != nil {
		t.Fatalf("write binary: %v", err)
	}
	_, err := ReadJSON(ctx, sc)
	if err == nil {
		t.Fatal("ReadJSON accepted a binary frame")
	}
	if !strings.Contains(err.Error(), "text control frame") {
		t.Errorf("err = %v, want it to name the frame-type mismatch", err)
	}
}

func TestReadJSONRejectsMalformedPayload(t *testing.T) {
	cc, sc := wsPair(t)
	ctx := context.Background()

	if err := cc.Write(ctx, websocket.MessageText, []byte("{not json")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, err := ReadJSON(ctx, sc)
	if err == nil {
		t.Fatal("ReadJSON accepted malformed JSON")
	}
	if !strings.Contains(err.Error(), "malformed control message") {
		t.Errorf("err = %v, want it to name the malformed payload", err)
	}
}

// TestPrepareRaisesTheReadLimit is the behavioural half of the ReadLimit
// invariant: a frame larger than the library default must survive.
func TestPrepareRaisesTheReadLimit(t *testing.T) {
	cc, sc := wsPair(t)
	ctx := context.Background()

	// 64 KiB — over coder/websocket's 32 KiB default, under our 1 MiB limit.
	big := make([]byte, 64*1024)
	for i := range big {
		big[i] = byte(i)
	}
	if err := cc.Write(ctx, websocket.MessageBinary, big); err != nil {
		t.Fatalf("write: %v", err)
	}
	typ, got, err := sc.Read(ctx)
	if err != nil {
		t.Fatalf("read a %d-byte frame: %v", len(big), err)
	}
	if typ != websocket.MessageBinary || len(got) != len(big) {
		t.Fatalf("read %v frame of %d bytes, want binary of %d", typ, len(got), len(big))
	}
}

func TestCloseIsNilSafe(t *testing.T) {
	Close(nil, websocket.StatusNormalClosure, "no conn") // must not panic
	cc, _ := wsPair(t)
	Close(cc, websocket.StatusNormalClosure, "bye")
	Close(cc, websocket.StatusNormalClosure, "bye again") // already closed
}
