package mux

import (
	"context"
	"io"
	"sync"
	"time"

	"ws-tunnel/internal/protocol"
)

// Stream is one multiplexed L4 pipe. Read returns io.EOF after a peer FIN.
// CloseWrite sends FIN; Close sends RST unless both sides already finished.
type Stream struct {
	id   uint32
	port uint16
	c    *Conn

	ackOnce   sync.Once
	ackCh     chan struct{}
	ackStatus byte

	mu       sync.Mutex
	cond     *sync.Cond
	buf      []byte
	readEOF  bool
	readErr  error
	recvUsed int

	sendMu     sync.Mutex
	sendCond   *sync.Cond
	sendWin    int
	writeEOF   bool
	writeErr   error
	localDone  bool
	remoteDone bool
}

func newStream(c *Conn, id uint32, port uint16, sendWin uint32) *Stream {
	st := &Stream{
		id:      id,
		port:    port,
		c:       c,
		ackCh:   make(chan struct{}),
		sendWin: int(sendWin),
	}
	if st.sendWin <= 0 {
		st.sendWin = InitialWindow
	}
	st.cond = sync.NewCond(&st.mu)
	st.sendCond = sync.NewCond(&st.sendMu)
	return st
}

func (s *Stream) ID() uint32 { return s.id }
func (s *Stream) Port() int  { return int(s.port) }

// WaitAck blocks until OPEN_ACK, the stream fails, or ctx ends.
func (s *Stream) WaitAck(ctx context.Context) (byte, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.ackCh:
		s.mu.Lock()
		err := s.readErr
		s.mu.Unlock()
		if err != nil && s.ackStatus == 0 {
			return 0, err
		}
		return s.ackStatus, nil
	}
}

// Ack writes OPEN_ACK. The receiver may pipeline DATA before this returns.
func (s *Stream) Ack(status byte) error {
	return s.c.writeFrame(s.c.ctx, Frame{
		Type: TypeOpenAck, StreamID: s.id, Status: status, Window: DefaultWindow,
	})
}

func (s *Stream) Read(p []byte) (int, error) {
	s.mu.Lock()
	for len(s.buf) == 0 && !s.readEOF && s.readErr == nil {
		s.cond.Wait()
	}
	if len(s.buf) > 0 {
		n := copy(p, s.buf)
		s.buf = s.buf[n:]
		s.recvUsed -= n
		if s.recvUsed < 0 {
			s.recvUsed = 0
		}
		s.mu.Unlock()
		if n > 0 {
			_ = s.c.writeFrame(s.c.ctx, Frame{Type: TypeWindow, StreamID: s.id, Window: uint32(n)})
		}
		return n, nil
	}
	err := s.readErr
	s.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return 0, io.EOF
}

func (s *Stream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	wrote := 0
	for len(p) > 0 {
		n, err := s.writeSome(p)
		wrote += n
		if err != nil {
			return wrote, err
		}
		p = p[n:]
	}
	return wrote, nil
}

// writeSome sends one DATA frame sized to whatever credit the peer has left,
// so a peer window smaller than MaxPayload still makes progress.
func (s *Stream) writeSome(p []byte) (int, error) {
	s.sendMu.Lock()
	for s.sendWin <= 0 && s.writeErr == nil && !s.writeEOF {
		s.sendCond.Wait()
	}
	if s.writeErr != nil {
		err := s.writeErr
		s.sendMu.Unlock()
		return 0, err
	}
	if s.writeEOF {
		s.sendMu.Unlock()
		return 0, io.ErrClosedPipe
	}
	n := min(len(p), s.sendWin, MaxPayload)
	s.sendWin -= n
	s.sendMu.Unlock()
	if err := s.c.writeFrame(s.c.ctx, Frame{Type: TypeData, StreamID: s.id, Payload: p[:n]}); err != nil {
		return 0, err
	}
	return n, nil
}

// CloseWrite signals end-of-direction (TCP FIN).
func (s *Stream) CloseWrite() error {
	s.sendMu.Lock()
	if s.writeEOF {
		s.sendMu.Unlock()
		return nil
	}
	s.writeEOF = true
	s.sendMu.Unlock()
	err := s.c.writeFrame(s.c.ctx, Frame{Type: TypeFin, StreamID: s.id})
	s.maybeRemove()
	return err
}

// Close aborts the stream. If both directions are already finished it is a
// local-only teardown; otherwise a RST is sent.
func (s *Stream) Close() error {
	s.sendMu.Lock()
	needRST := !s.writeEOF
	s.writeEOF = true
	if s.writeErr == nil {
		s.writeErr = io.ErrClosedPipe
	}
	s.sendCond.Broadcast()
	s.sendMu.Unlock()

	s.mu.Lock()
	if !s.readEOF && s.readErr == nil {
		s.readErr = io.ErrClosedPipe
		s.cond.Broadcast()
	}
	s.mu.Unlock()

	var err error
	if needRST {
		err = s.c.writeFrame(s.c.ctx, Frame{Type: TypeRst, StreamID: s.id, Reason: protocol.AckRejected})
	}
	s.c.remove(s.id)
	return err
}

func (s *Stream) SetDeadline(t time.Time) error {
	_ = t
	return nil
}
func (s *Stream) SetReadDeadline(t time.Time) error {
	_ = t
	return nil
}
func (s *Stream) SetWriteDeadline(t time.Time) error {
	_ = t
	return nil
}

func (s *Stream) gotAck(status byte, window uint32) {
	// The opener started with InitialWindow of credit and may already have
	// spent some of it, so the advertised window adjusts the balance rather
	// than replacing it. A balance below zero just blocks until WINDOW frames.
	if window > 0 && int(window) != InitialWindow {
		s.sendMu.Lock()
		s.sendWin += int(window) - InitialWindow
		s.sendCond.Broadcast()
		s.sendMu.Unlock()
	}
	s.ackOnce.Do(func() {
		s.ackStatus = status
		close(s.ackCh)
	})
	if status != protocol.AckOK {
		s.fail(io.ErrClosedPipe)
	}
}

func (s *Stream) gotData(p []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readEOF || s.readErr != nil {
		return
	}
	s.buf = append(s.buf, p...)
	s.recvUsed += len(p)
	s.cond.Broadcast()
}

func (s *Stream) gotFin() {
	s.mu.Lock()
	s.readEOF = true
	s.remoteDone = true
	s.cond.Broadcast()
	s.mu.Unlock()
	s.maybeRemove()
}

func (s *Stream) addSendWindow(n int) {
	if n <= 0 {
		return
	}
	s.sendMu.Lock()
	s.sendWin += n
	s.sendCond.Broadcast()
	s.sendMu.Unlock()
}

func (s *Stream) fail(err error) {
	s.mu.Lock()
	if s.readErr == nil {
		s.readErr = err
	}
	s.readEOF = true
	s.cond.Broadcast()
	s.mu.Unlock()

	s.sendMu.Lock()
	if s.writeErr == nil {
		s.writeErr = err
	}
	s.sendCond.Broadcast()
	s.sendMu.Unlock()

	s.ackOnce.Do(func() { close(s.ackCh) })
}

func (s *Stream) maybeRemove() {
	s.mu.Lock()
	remote := s.readEOF || s.readErr != nil
	s.mu.Unlock()
	s.sendMu.Lock()
	local := s.writeEOF
	s.sendMu.Unlock()
	if remote && local {
		s.c.remove(s.id)
	}
}
