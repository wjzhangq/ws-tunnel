package mux

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"

	"ws-tunnel/internal/protocol"
)

// rawPeer speaks the binary frame format directly so tests can play a peer
// that advertises odd windows or misbehaves.
type rawPeer struct {
	t  *testing.T
	tr Transport
}

func (p rawPeer) send(f Frame) {
	p.t.Helper()
	b, err := encodeFrame(f)
	if err != nil {
		p.t.Fatalf("encode %+v: %v", f, err)
	}
	if err := p.tr.Write(context.Background(), websocket.MessageBinary, b); err != nil {
		p.t.Fatalf("write %+v: %v", f, err)
	}
}

func (p rawPeer) next(timeout time.Duration) (Frame, bool) {
	p.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	typ, data, err := p.tr.Read(ctx)
	if err != nil {
		return Frame{}, false
	}
	if typ != websocket.MessageBinary {
		p.t.Fatalf("unexpected ws message type %v", typ)
	}
	f, err := decodeFrame(data)
	if err != nil {
		p.t.Fatalf("decode: %v", err)
	}
	return f, true
}

// readData collects exactly want bytes of DATA, skipping other frame types.
func (p rawPeer) readData(want int) []byte {
	p.t.Helper()
	var got []byte
	for len(got) < want {
		f, ok := p.next(2 * time.Second)
		if !ok {
			p.t.Fatalf("got %d of %d data bytes", len(got), want)
		}
		if f.Type == TypeData {
			got = append(got, f.Payload...)
		}
	}
	if len(got) != want {
		p.t.Fatalf("got %d data bytes, want exactly %d", len(got), want)
	}
	return got
}

// expectNoData fails if any DATA frame shows up within d.
func (p rawPeer) expectNoData(d time.Duration) {
	p.t.Helper()
	deadline := time.Now().Add(d)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return
		}
		f, ok := p.next(left)
		if !ok {
			return
		}
		if f.Type == TypeData {
			p.t.Fatalf("sent %d bytes beyond the peer's window", len(f.Payload))
		}
	}
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

func TestSmallPeerWindowMakesProgress(t *testing.T) {
	a, b := MemPair()
	cli := New(t.Context(), b, Client)
	defer cli.Close()
	peer := rawPeer{t, a}

	peer.send(Frame{Type: TypeOpen, StreamID: 1, Port: 80, Window: 1000})
	st, err := cli.Accept(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	want := pattern(5000)
	done := make(chan error, 1)
	go func() {
		_, err := st.Write(want)
		done <- err
	}()

	got := peer.readData(1000)
	peer.expectNoData(200 * time.Millisecond)
	peer.send(Frame{Type: TypeWindow, StreamID: 1, Window: 4000})
	got = append(got, peer.readData(4000)...)

	if err := <-done; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("payload corrupted")
	}
}

func TestPeerOverrunningWindowIsReset(t *testing.T) {
	a, b := MemPair()
	cli := New(t.Context(), b, Client)
	defer cli.Close()
	peer := rawPeer{t, a}

	peer.send(Frame{Type: TypeOpen, StreamID: 1, Port: 80, Window: DefaultWindow})
	st, err := cli.Accept(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	limit := max(DefaultWindow, InitialWindow)
	for sent := 0; sent < limit; sent += MaxPayload {
		peer.send(Frame{Type: TypeData, StreamID: 1, Payload: pattern(min(MaxPayload, limit-sent))})
	}
	peer.send(Frame{Type: TypeData, StreamID: 1, Payload: []byte("x")})

	for {
		f, ok := peer.next(2 * time.Second)
		if !ok {
			t.Fatal("no RST after window overrun")
		}
		if f.Type == TypeRst && f.StreamID == 1 {
			break
		}
	}
	buf := make([]byte, limit+1)
	var n int
	for {
		m, err := st.Read(buf[n:])
		n += m
		if err != nil {
			if err != ErrWindowExceeded {
				t.Fatalf("read err = %v, want ErrWindowExceeded", err)
			}
			break
		}
	}
	if n > limit {
		t.Fatalf("buffered %d bytes past the %d window", n, limit)
	}
}

func TestWindowUpdatesAreBatched(t *testing.T) {
	a, b := MemPair()
	cli := New(t.Context(), b, Client, WithWindow(MinWindow))
	defer cli.Close()
	peer := rawPeer{t, a}

	peer.send(Frame{Type: TypeOpen, StreamID: 1, Port: 80, Window: MinWindow})
	st, err := cli.Accept(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(protocol.AckOK); err != nil {
		t.Fatal(err)
	}
	if f, ok := peer.next(2 * time.Second); !ok || f.Type != TypeOpenAck || f.Window != MinWindow {
		t.Fatalf("want OPEN_ACK advertising %d, got %+v", MinWindow, f)
	}

	peer.send(Frame{Type: TypeData, StreamID: 1, Payload: pattern(MinWindow)})
	buf := make([]byte, 64)
	for read := 0; read < MinWindow; {
		n, err := st.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		read += n
	}

	var credited, frames int
	for {
		f, ok := peer.next(200 * time.Millisecond)
		if !ok {
			break
		}
		if f.Type == TypeWindow {
			credited += int(f.Window)
			frames++
		}
	}
	if credited != MinWindow {
		t.Fatalf("credited %d bytes, want %d", credited, MinWindow)
	}
	if frames > 2 {
		t.Fatalf("%d WINDOW frames for %d one-shot reads; want them batched", frames, MinWindow/len(buf))
	}
}

func TestOpenAckSmallWindowLimitsOpener(t *testing.T) {
	a, b := MemPair()
	srv := New(t.Context(), a, Server)
	defer srv.Close()
	peer := rawPeer{t, b}

	st, err := srv.Open(t.Context(), 80)
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := peer.next(2 * time.Second); !ok || f.Type != TypeOpen {
		t.Fatalf("want OPEN, got %+v", f)
	}
	peer.send(Frame{Type: TypeOpenAck, StreamID: st.ID(), Status: protocol.AckOK, Window: 1000})
	if status, err := st.WaitAck(t.Context()); err != nil || status != protocol.AckOK {
		t.Fatalf("ack %d %v", status, err)
	}

	go func() { _, _ = st.Write(pattern(3000)) }()
	peer.readData(1000)
	peer.expectNoData(200 * time.Millisecond)
	peer.send(Frame{Type: TypeWindow, StreamID: st.ID(), Window: 2000})
	peer.readData(2000)
}

func TestOpenAckDoesNotRefillPipelinedCredit(t *testing.T) {
	a, b := MemPair()
	srv := New(t.Context(), a, Server)
	defer srv.Close()
	peer := rawPeer{t, b}

	st, err := srv.Open(t.Context(), 80)
	if err != nil {
		t.Fatal(err)
	}
	if f, ok := peer.next(2 * time.Second); !ok || f.Type != TypeOpen {
		t.Fatalf("want OPEN, got %+v", f)
	}
	// Spend the whole pre-ack credit, then ack with the same window: no new
	// credit has been granted, so nothing more may be sent.
	go func() { _, _ = st.Write(pattern(InitialWindow)) }()
	peer.readData(InitialWindow)
	peer.send(Frame{Type: TypeOpenAck, StreamID: st.ID(), Status: protocol.AckOK, Window: InitialWindow})
	if _, err := st.WaitAck(t.Context()); err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = st.Write([]byte("over")) }()
	peer.expectNoData(200 * time.Millisecond)
}
