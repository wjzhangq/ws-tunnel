package mux

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"ws-tunnel/internal/protocol"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []Frame{
		{Type: TypeOpen, StreamID: 7, Port: 19080, Window: DefaultWindow},
		{Type: TypeOpenAck, StreamID: 7, Status: protocol.AckOK, Window: 4096},
		{Type: TypeData, StreamID: 7, Payload: []byte("hello")},
		{Type: TypeFin, StreamID: 7},
		{Type: TypeRst, StreamID: 7, Reason: protocol.AckDialFailed},
		{Type: TypeWindow, StreamID: 7, Window: 32},
	}
	for _, want := range cases {
		b, err := encodeFrame(want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		got, err := decodeFrame(b)
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got.Type != want.Type || got.StreamID != want.StreamID ||
			got.Port != want.Port || got.Status != want.Status ||
			got.Window != want.Window || got.Reason != want.Reason ||
			!bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
}

func TestDecodeRejectsBadFrames(t *testing.T) {
	if _, err := decodeFrame([]byte{0x02, TypeFin, 0, 0, 0, 1}); err != ErrBadVersion {
		t.Fatalf("wrong version: %v", err)
	}
	if _, err := decodeFrame([]byte{Version, 99, 0, 0, 0, 1}); err == nil {
		t.Fatal("unknown type must fail")
	}
	if _, err := decodeFrame([]byte{Version, TypeData, 0, 0, 0, 1, 0, 3, 1}); err != ErrBadFrame {
		t.Fatalf("truncated data: %v", err)
	}
}

func TestStreamPipesBytesAndHalfClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, b := MemPair()
	srv := New(ctx, a, Server)
	cli := New(ctx, b, Client)
	defer srv.Close()
	defer cli.Close()

	accepted := make(chan *Stream, 1)
	go func() {
		st, err := cli.Accept(ctx)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- st
	}()

	out, err := srv.Open(ctx, 19080)
	if err != nil {
		t.Fatal(err)
	}
	in := <-accepted
	if in.Port() != 19080 {
		t.Fatalf("port = %d", in.Port())
	}
	if err := in.Ack(protocol.AckOK); err != nil {
		t.Fatal(err)
	}
	status, err := out.WaitAck(ctx)
	if err != nil || status != protocol.AckOK {
		t.Fatalf("ack %d %v", status, err)
	}

	if _, err := out.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	if _, err := io.ReadFull(in, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("got %q", buf)
	}

	if _, err := in.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(out, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "pong" {
		t.Fatalf("got %q", buf)
	}

	if err := out.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := in.Read(buf); err != io.EOF {
		t.Fatalf("want EOF after FIN, got %v", err)
	}
	if _, err := in.Write([]byte("still")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if _, err := io.ReadFull(out, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "still" {
		t.Fatalf("reply after FIN = %q", got)
	}
}

func TestControlJSONOnSameConn(t *testing.T) {
	ctx := context.Background()
	a, b := MemPair()
	srv := New(ctx, a, Server)
	cli := New(ctx, b, Client)
	defer srv.Close()
	defer cli.Close()

	if err := srv.SendControl(ctx, &protocol.Message{Type: protocol.TypePing, Nonce: "n1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-cli.Controls():
		if msg.Type != protocol.TypePing || msg.Nonce != "n1" {
			t.Fatalf("got %+v", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no control message")
	}
}

func TestControlNotDroppedUnderLoad(t *testing.T) {
	ctx := t.Context()
	a, b := MemPair()
	srv := New(ctx, a, Server)
	cli := New(ctx, b, Client)
	defer srv.Close()
	defer cli.Close()

	const n = 1000
	go func() {
		for i := range n {
			if err := srv.SendControl(ctx, &protocol.Message{Type: protocol.TypePing, TS: int64(i)}); err != nil {
				t.Errorf("send %d: %v", i, err)
				return
			}
		}
	}()
	// Let the sender run far ahead of the consumer before draining.
	time.Sleep(100 * time.Millisecond)
	for i := range n {
		select {
		case msg := <-cli.Controls():
			if msg.TS != int64(i) {
				t.Fatalf("message %d: got ts %d", i, msg.TS)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("message %d never arrived", i)
		}
	}
}

func TestBothSidesOpenWithoutIDClash(t *testing.T) {
	ctx := t.Context()
	a, b := MemPair()
	srv := New(ctx, a, Server)
	cli := New(ctx, b, Client)
	defer srv.Close()
	defer cli.Close()

	const n = 20
	for _, side := range []struct {
		open, accept *Conn
		parity       uint32
	}{{srv, cli, 1}, {cli, srv, 0}} {
		go func() {
			for i := range n {
				st, err := side.open.Open(ctx, 1000+i)
				if err != nil {
					t.Errorf("open: %v", err)
					return
				}
				if st.ID()%2 != side.parity {
					t.Errorf("id %d has the wrong parity", st.ID())
				}
			}
		}()
	}
	for _, c := range []*Conn{srv, cli} {
		seen := map[int]bool{}
		for range n {
			st, err := c.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seen[st.Port()] = true
		}
		if len(seen) != n {
			t.Fatalf("accepted %d distinct ports, want %d", len(seen), n)
		}
	}
}

func TestOpenWithOwnParityIsReset(t *testing.T) {
	a, b := MemPair()
	cli := New(t.Context(), b, Client)
	defer cli.Close()
	peer := rawPeer{t, a}

	peer.send(Frame{Type: TypeOpen, StreamID: 2, Port: 80, Window: DefaultWindow})
	f, ok := peer.next(2 * time.Second)
	if !ok || f.Type != TypeRst || f.StreamID != 2 {
		t.Fatalf("want RST for even id from the server, got %+v", f)
	}
}

func TestOpenAckNotOK(t *testing.T) {
	ctx := context.Background()
	a, b := MemPair()
	srv := New(ctx, a, Server)
	cli := New(ctx, b, Client)
	defer srv.Close()
	defer cli.Close()

	go func() {
		st, err := cli.Accept(ctx)
		if err != nil {
			return
		}
		_ = st.Ack(protocol.AckDialFailed)
	}()
	out, err := srv.Open(ctx, 80)
	if err != nil {
		t.Fatal(err)
	}
	status, err := out.WaitAck(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status != protocol.AckDialFailed {
		t.Fatalf("status = %d", status)
	}
}
