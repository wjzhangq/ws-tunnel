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
	srv := New(ctx, a)
	cli := New(ctx, b)
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
	srv := New(ctx, a)
	cli := New(ctx, b)
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

func TestOpenAckNotOK(t *testing.T) {
	ctx := context.Background()
	a, b := MemPair()
	srv := New(ctx, a)
	cli := New(ctx, b)
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
