package mux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"ws-tunnel/internal/protocol"
)

var (
	ErrClosed         = errors.New("mux connection closed")
	ErrWindowExceeded = errors.New("peer sent more data than the stream window allows")
	ErrIDsExhausted   = errors.New("mux stream ids exhausted; reconnect")
)

// Conn multiplexes JSON control messages and binary streams on one Transport.
type Conn struct {
	t    Transport
	side Side

	writeMu sync.Mutex

	mu       sync.Mutex
	streams  map[uint32]*Stream
	nextID   uint32
	acceptCh chan *Stream

	window int // receive window advertised for every stream

	controlCh chan *protocol.Message

	// asyncOut carries frames the read loop wants sent. The read loop must
	// never block on a write, or two peers under backpressure can deadlock.
	asyncOut chan Frame

	ctx    context.Context
	cancel context.CancelFunc

	closed    atomic.Bool
	closeOnce sync.Once

	lastRead atomic.Int64 // unix nanos; any inbound frame
}

// Side fixes stream-id parity so both ends may open streams without
// colliding: the server allocates odd ids, the client even ones.
type Side uint8

const (
	Server Side = iota
	Client
)

// Option tunes a Conn.
type Option func(*Conn)

// WithWindow sets the per-stream receive window, clamped to
// [MinWindow, MaxWindow]. Zero keeps DefaultWindow.
func WithWindow(n int) Option {
	return func(c *Conn) {
		if n > 0 {
			c.window = min(max(n, MinWindow), MaxWindow)
		}
	}
}

// New starts the read loop immediately.
func New(parent context.Context, t Transport, side Side, opts ...Option) *Conn {
	ctx, cancel := context.WithCancel(parent)
	c := &Conn{
		t:         t,
		side:      side,
		streams:   map[uint32]*Stream{},
		nextID:    1 + uint32(side),
		acceptCh:  make(chan *Stream, 64),
		window:    DefaultWindow,
		controlCh: make(chan *protocol.Message, 32),
		asyncOut:  make(chan Frame, 256),
		ctx:       ctx,
		cancel:    cancel,
	}
	for _, o := range opts {
		o(c)
	}
	c.lastRead.Store(time.Now().UnixNano())
	go c.readLoop()
	go c.asyncWriteLoop()
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
	if id > math.MaxUint32-2 {
		c.mu.Unlock()
		return nil, ErrIDsExhausted
	}
	c.nextID += 2
	st := newStream(c, id, uint16(port), InitialWindow, c.window, c.window)
	c.streams[id] = st
	c.mu.Unlock()

	err := c.writeFrame(ctx, Frame{
		Type: TypeOpen, StreamID: id, Port: uint16(port), Window: uint32(c.window),
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

// sendAsync queues a best-effort frame (RST) from the read loop. When the
// queue is full the frame is dropped; the peer will learn of the reset from
// the next frame it sends for that stream.
func (c *Conn) sendAsync(f Frame) {
	select {
	case c.asyncOut <- f:
	default:
	}
}

func (c *Conn) asyncWriteLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case f := <-c.asyncOut:
			if err := c.writeFrame(c.ctx, f); err != nil {
				return
			}
		}
	}
}

func (c *Conn) rst(id uint32) {
	c.sendAsync(Frame{Type: TypeRst, StreamID: id, Reason: protocol.AckRejected})
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
		if !c.peerID(f.StreamID) {
			c.rst(f.StreamID)
			return
		}
		// The opener may pipeline InitialWindow bytes before it sees our
		// OPEN_ACK, so we must buffer at least that much.
		st := newStream(c, f.StreamID, f.Port, f.Window, c.window, max(c.window, InitialWindow))
		c.mu.Lock()
		if _, exists := c.streams[f.StreamID]; exists {
			c.mu.Unlock()
			c.rst(f.StreamID)
			return
		}
		c.streams[f.StreamID] = st
		c.mu.Unlock()
		select {
		case c.acceptCh <- st:
		default:
			c.remove(f.StreamID)
			st.fail(errors.New("accept queue full"))
			c.rst(f.StreamID)
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
			c.rst(f.StreamID)
			return
		}
		if !st.gotData(f.Payload) {
			c.remove(f.StreamID)
			st.fail(ErrWindowExceeded)
			c.rst(f.StreamID)
		}
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

// peerID reports whether id has the parity the peer is allowed to open.
func (c *Conn) peerID(id uint32) bool {
	return id != 0 && id%2 != (1+uint32(c.side))%2
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
