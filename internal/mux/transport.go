package mux

import (
	"context"
	"io"
	"sync"

	"github.com/coder/websocket"
)

// Transport is the tiny WS surface mux needs: mixed text/binary frames.
type Transport interface {
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Close(code websocket.StatusCode, reason string) error
}

// WS adapts a coder/websocket connection.
type WS struct{ C *websocket.Conn }

func (w WS) Write(ctx context.Context, typ websocket.MessageType, p []byte) error {
	return w.C.Write(ctx, typ, p)
}

func (w WS) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	return w.C.Read(ctx)
}

func (w WS) Close(code websocket.StatusCode, reason string) error {
	if w.C == nil {
		return nil
	}
	return w.C.Close(code, reason)
}

type memMsg struct {
	typ websocket.MessageType
	p   []byte
}

// memEnd is one side of an in-memory Transport pair, used by tests.
type memEnd struct {
	send      chan memMsg
	recv      chan memMsg
	closed    chan struct{}
	closeOnce sync.Once
}

func (m *memEnd) Write(ctx context.Context, typ websocket.MessageType, p []byte) error {
	cp := append([]byte(nil), p...)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.closed:
		return io.ErrClosedPipe
	case m.send <- memMsg{typ: typ, p: cp}:
		return nil
	}
}

func (m *memEnd) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	select {
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-m.closed:
		return 0, nil, io.EOF
	case msg, ok := <-m.recv:
		if !ok {
			return 0, nil, io.EOF
		}
		return msg.typ, msg.p, nil
	}
}

func (m *memEnd) Close(websocket.StatusCode, string) error {
	m.closeOnce.Do(func() { close(m.closed) })
	return nil
}

// MemPair returns two Transports that speak to each other. Closing one side
// makes the other Read return EOF.
func MemPair() (Transport, Transport) {
	ab := make(chan memMsg, 64)
	ba := make(chan memMsg, 64)
	a := &memEnd{send: ab, recv: ba, closed: make(chan struct{})}
	b := &memEnd{send: ba, recv: ab, closed: make(chan struct{})}
	return a, b
}
