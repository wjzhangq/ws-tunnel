package mux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"ws-tunnel/internal/protocol"
)

var (
	ErrClosed     = errors.New("mux connection closed")
	ErrUnknownStr = errors.New("unknown stream id")
)

// Conn multiplexes JSON control messages and binary streams on one Transport.
type Conn struct {
	t Transport

	writeMu sync.Mutex

	mu       sync.Mutex
	streams  map[uint32]*Stream
	nextID   uint32
	acceptCh chan *Stream

	controlCh chan *protocol.Message

	ctx    context.Context
	cancel context.CancelFunc

	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error

	lastRead atomic.Int64 // unix nanos; any inbound frame
}

// New starts the read loop immediately.
func New(parent context.Context, t Transport) *Conn {
	ctx, cancel := context.WithCancel(parent)
	c := &Conn{
		t:         t,
		streams:   map[uint32]*Stream{},
		nextID:    1,
		acceptCh:  make(chan *Stream, 64),
		controlCh: make(chan *protocol.Message, 32),
		ctx:       ctx,
		cancel:    cancel,
	}
	c.lastRead.Store(time.Now().UnixNano())
	go c.readLoop()
	return c
}

func (c *Conn) Controls() <-chan *protocol.Message { return c.controlCh }

func (c *Conn) Done() <-chan struct{} { return c.ctx.Done() }

func (c *Conn) LastRead() time.Time { return time.Unix(0, c.lastRead.Load()) }

func (c *Conn) Closed() bool { return c.closed.Load() }

// SendControl writes one JSON text frame.
func (c *Conn) SendControl(ctx context.Context, msg *protocol.Message) error {
	if c.closed.Load() {
		return ErrClosed
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.t.Write(ctx, websocket.MessageText, b)
}

// Open allocates a stream id, sends OPEN, and returns the stream immediately
// so the caller can pipeline DATA before OPEN_ACK (matching the old ack
// semantics).
func (c *Conn) Open(ctx context.Context, port int) (*Stream, error) {
	if port < 1 || port > 65535 {
		return nil, protocol.ErrBadPortID
	}
	if c.closed.Load() {
		return nil, ErrClosed
	}
	c.mu.Lock()
	id := c.nextID
	c.nextID++
	st := newStream(c, id, uint16(port), InitialWindow)
	c.streams[id] = st
	c.mu.Unlock()

	err := c.writeFrame(ctx, Frame{
		Type: TypeOpen, StreamID: id, Port: uint16(port), Window: DefaultWindow,
	})
	if err != nil {
		c.remove(id)
		st.fail(err)
		return nil, err
	}
	return st, nil
}

// Accept waits for a peer OPEN.
func (c *Conn) Accept(ctx context.Context) (*Stream, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, ErrClosed
	case st, ok := <-c.acceptCh:
		if !ok {
			return nil, ErrClosed
		}
		return st, nil
	}
}

func (c *Conn) Close() {
	c.closeWith(ErrClosed)
}

func (c *Conn) closeWith(err error) {
	c.closeOnce.Do(func() {
		c.closeErr = err
		c.closed.Store(true)
		c.cancel()
		c.mu.Lock()
		for _, st := range c.streams {
			st.fail(err)
		}
		c.streams = map[uint32]*Stream{}
		c.mu.Unlock()
		_ = c.t.Close(websocket.StatusNormalClosure, "mux closed")
	})
}

func (c *Conn) writeFrame(ctx context.Context, f Frame) error {
	b, err := encodeFrame(f)
	if err != nil || b == nil {
		return err
	}
	if c.closed.Load() {
		return ErrClosed
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.t.Write(ctx, websocket.MessageBinary, b)
}

func (c *Conn) readLoop() {
	defer c.closeWith(io.EOF)
	for {
		typ, data, err := c.t.Read(c.ctx)
		if err != nil {
			if c.ctx.Err() != nil {
				c.closeWith(ErrClosed)
			} else {
				c.closeWith(err)
			}
			return
		}
		c.lastRead.Store(time.Now().UnixNano())
		switch typ {
		case websocket.MessageText:
			var msg protocol.Message
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			// Never drop: a lost bye/reload_config/pong desyncs the session.
			// Consumers drain Controls() until Done(), so this cannot wedge.
			select {
			case c.controlCh <- &msg:
			case <-c.ctx.Done():
				return
			}
		case websocket.MessageBinary:
			f, err := decodeFrame(data)
			if err != nil {
				c.closeWith(fmt.Errorf("mux: %w", err))
				return
			}
			c.handleFrame(f)
		default:
			c.closeWith(fmt.Errorf("mux: unexpected ws type %v", typ))
			return
		}
	}
}

func (c *Conn) handleFrame(f Frame) {
	switch f.Type {
	case TypeOpen:
		st := newStream(c, f.StreamID, f.Port, f.Window)
		c.mu.Lock()
		if _, exists := c.streams[f.StreamID]; exists {
			c.mu.Unlock()
			_ = c.writeFrame(c.ctx, Frame{Type: TypeRst, StreamID: f.StreamID, Reason: protocol.AckRejected})
			return
		}
		c.streams[f.StreamID] = st
		c.mu.Unlock()
		select {
		case c.acceptCh <- st:
		default:
			c.remove(f.StreamID)
			st.fail(errors.New("accept queue full"))
			_ = c.writeFrame(c.ctx, Frame{Type: TypeRst, StreamID: f.StreamID, Reason: protocol.AckRejected})
		}
	case TypeOpenAck:
		st := c.stream(f.StreamID)
		if st == nil {
			return
		}
		st.gotAck(f.Status, f.Window)
	case TypeData:
		st := c.stream(f.StreamID)
		if st == nil {
			_ = c.writeFrame(c.ctx, Frame{Type: TypeRst, StreamID: f.StreamID, Reason: protocol.AckRejected})
			return
		}
		st.gotData(f.Payload)
	case TypeFin:
		st := c.stream(f.StreamID)
		if st != nil {
			st.gotFin()
		}
	case TypeRst:
		st := c.stream(f.StreamID)
		if st != nil {
			st.fail(io.ErrClosedPipe)
			c.remove(f.StreamID)
		}
	case TypeWindow:
		st := c.stream(f.StreamID)
		if st != nil {
			st.addSendWindow(int(f.Window))
		}
	}
}

func (c *Conn) stream(id uint32) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[id]
}

func (c *Conn) remove(id uint32) {
	c.mu.Lock()
	delete(c.streams, id)
	c.mu.Unlock()
}
