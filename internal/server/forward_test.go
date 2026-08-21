package server

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtaci/smux"

	"ws-tunnel/internal/config"
	"ws-tunnel/internal/protocol"
)

// forwardFixture wires a server with one live node session whose single data
// channel is a real smux pair, so forward() can be driven end to end without a
// client process.
type forwardFixture struct {
	srv    *Server
	sess   *NodeSession
	stats  *NodeStats
	pl     *portListener
	accept func() *smux.Stream // accepts the stream forward() opens
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
	sess := newNodeSession("node1", nil, nodeCfg, time.Second, srv.registry.Stats("node1"), srv.log)
	if err := srv.registry.Register(sess); err != nil {
		t.Fatalf("register: %v", err)
	}

	a, b := net.Pipe()
	scfg := smux.DefaultConfig()
	scfg.KeepAliveDisabled = true
	cli, err := smux.Client(a, scfg) // server side opens streams
	if err != nil {
		t.Fatalf("smux.Client: %v", err)
	}
	remote, err := smux.Server(b, scfg) // stands in for tunnel-client
	if err != nil {
		t.Fatalf("smux.Server: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
		_ = remote.Close()
	})
	sess.AddChannel(&DataChannel{ID: 1, sess: cli})

	return &forwardFixture{
		srv:   srv,
		sess:  sess,
		stats: srv.registry.Stats("node1"),
		pl:    &portListener{m: srv.listeners, port: 19080, node: "node1"},
		accept: func() *smux.Stream {
			st, err := remote.AcceptStream()
			if err != nil {
				t.Errorf("accept stream: %v", err)
				return nil
			}
			return st
		},
	}
}

// TestForwardDoesNotWaitForTheAck is the regression test for §7.1 / HANDOFF §3:
// the server must pipeline the external bytes as soon as the header is written.
// Waiting for the ack first deadlocks any client-speaks-first protocol, which is
// the bug the design explicitly fixed.
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

	port, err := protocol.ReadStreamHeader(st)
	if err != nil {
		t.Fatalf("read stream header: %v", err)
	}
	if port != 19080 {
		t.Fatalf("port id = %d, want 19080", port)
	}

	// Deliberately do NOT ack yet. The external side speaks first, and those
	// bytes must arrive anyway.
	go func() { _, _ = caller.Write([]byte("EHLO first\n")) }()

	// The payload rides inside a length-delimited frame (§7.1, StreamVersion
	// 0x02), so read it the way a real tunnel-client would.
	fr := protocol.NewFrameReader(st)
	buf := make([]byte, 11)
	_ = st.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.ReadFull(fr, buf); err != nil {
		t.Fatalf("payload did not arrive before the ack: %v", err)
	}
	_ = st.SetReadDeadline(time.Time{})
	if string(buf) != "EHLO first\n" {
		t.Fatalf("payload = %q", buf)
	}

	// Now ack and answer; the response must reach the external side.
	if err := protocol.WriteAck(st, protocol.AckOK); err != nil {
		t.Fatalf("write ack: %v", err)
	}
	fw := protocol.NewFrameWriter(st)
	if _, err := fw.Write([]byte("250 OK\n")); err != nil {
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

	// End-of-direction in band first, so forward()'s FrameReader sees a clean
	// io.EOF at a frame boundary rather than a truncated frame.
	_ = fw.CloseWrite()
	_ = st.Close()
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
