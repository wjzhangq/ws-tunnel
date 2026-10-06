package server

import (
	"io"
	"net"
	"testing"
	"time"

	"ws-tunnel/internal/config"
	"ws-tunnel/internal/mux"
	"ws-tunnel/internal/protocol"
)

type forwardFixture struct {
	srv    *Server
	sess   *NodeSession
	stats  *NodeStats
	pl     *portListener
	accept func() *mux.Stream
}

func newForwardFixture(t *testing.T, dialTimeout time.Duration) *forwardFixture {
	t.Helper()

	srv := &Server{log: testLogger(), cfg: &config.Config{}}
	srv.registry = newRegistry(srv.log)
	srv.listeners = newListenerManager(srv)
	srv.baseCtx = t.Context()

	nodeCfg := &protocol.NodeConfig{
		Channels:          1,
		MaxStreamsPerConn: 8,
		DialTimeout:       protocol.Duration(dialTimeout),
	}
	sess := newNodeSession("node1", nodeCfg, time.Second, srv.registry.Stats("node1"), srv.log)
	a, b := mux.MemPair()
	srvMux := mux.New(t.Context(), a, mux.Server)
	cliMux := mux.New(t.Context(), b, mux.Client)
	sess.AttachMux(srvMux)
	if err := srv.registry.Register(sess); err != nil {
		t.Fatalf("register: %v", err)
	}
	t.Cleanup(func() {
		srvMux.Close()
		cliMux.Close()
	})

	return &forwardFixture{
		srv:   srv,
		sess:  sess,
		stats: srv.registry.Stats("node1"),
		pl:    &portListener{m: srv.listeners, port: 19080, node: "node1"},
		accept: func() *mux.Stream {
			st, err := cliMux.Accept(t.Context())
			if err != nil {
				t.Errorf("accept stream: %v", err)
				return nil
			}
			return st
		},
	}
}

func TestForwardDoesNotWaitForTheAck(t *testing.T) {
	fx := newForwardFixture(t, 5*time.Second)
	ext, caller := net.Pipe()
	defer ext.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fx.srv.forward(fx.pl, ext)
	}()

	st := fx.accept()
	if st == nil {
		return
	}
	if st.Port() != 19080 {
		t.Fatalf("port id = %d, want 19080", st.Port())
	}

	go func() { _, _ = caller.Write([]byte("EHLO first\n")) }()

	buf := make([]byte, 11)
	_ = st.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("payload did not arrive before the ack: %v", err)
	}
	if string(buf) != "EHLO first\n" {
		t.Fatalf("payload = %q", buf)
	}

	if err := st.Ack(protocol.AckOK); err != nil {
		t.Fatalf("write ack: %v", err)
	}
	if _, err := st.Write([]byte("250 OK\n")); err != nil {
		t.Fatalf("write response: %v", err)
	}
	reply := make([]byte, 7)
	_ = caller.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(caller, reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	if string(reply) != "250 OK\n" {
		t.Fatalf("reply = %q", reply)
	}

	_ = st.CloseWrite()
	_ = caller.Close()
	<-done

	if got := fx.stats.Result("ok"); got != 1 {
		t.Errorf("ok result = %d, want 1", got)
	}
	if got := fx.stats.BytesIn.Load(); got != 11 {
		t.Errorf("bytes_in = %d, want 11", got)
	}
	if got := fx.stats.BytesOut.Load(); got != 7 {
		t.Errorf("bytes_out = %d, want 7", got)
	}
}
