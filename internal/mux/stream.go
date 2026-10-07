package mux

import (
	"context"
	"io"
	"os"
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

	// ackCh closes on OPEN_ACK or on failure; acked tells the two apart.
	// Both fields are written before the close, so readers after it see them.
	ackOnce   sync.Once
	ackCh     chan struct{}
	acked     bool
	ackStatus byte
	ackErr    error

	mu      sync.Mutex
	cond    *sync.Cond
	buf     []byte
	readEOF bool
	readErr error
	// recvUsed is bytes received but not yet credited back with WINDOW;
	// the peer may never push it past recvLimit. recvPending is the part of
	// recvUsed already consumed by Read, returned once it reaches half of
	// recvWindow so WINDOW frames are batched rather than one per Read.
	recvUsed     int
	recvPending  int
	recvWindow   int
	recvLimit    int
	readDeadline deadline

	sendMu        sync.Mutex
	sendCond      *sync.Cond
	sendWin       int
	writeEOF      bool
	writeErr      error
	writeDeadline deadline
}

// deadline wakes a cond when it expires. It is guarded by the mutex that
// backs that cond.
type deadline struct {
	at    time.Time
	timer *time.Timer
}

func (d *deadline) set(t time.Time, mu sync.Locker, cond *sync.Cond) {
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.at = t
	if !t.IsZero() {
		if wait := time.Until(t); wait > 0 {
			d.timer = time.AfterFunc(wait, func() {
				mu.Lock()
				cond.Broadcast()
				mu.Unlock()
			})
		}
	}
	cond.Broadcast()
}

func (d *deadline) expired() bool {
	return !d.at.IsZero() && !time.Now().Before(d.at)
}

func newStream(c *Conn, id uint32, port uint16, sendWin uint32, recvWindow, recvLimit int) *Stream {
	st := &Stream{
		id:         id,
		port:       port,
		c:          c,
		ackCh:      make(chan struct{}),
		sendWin:    int(sendWin),
		recvWindow: recvWindow,
		recvLimit:  recvLimit,
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

// WaitAck blocks until OPEN_ACK, the stream fails, or ctx ends. An ack that
// arrived is reported even if the stream failed afterwards.
func (s *Stream) WaitAck(ctx context.Context) (byte, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.ackCh:
		if s.acked {
			return s.ackStatus, nil
		}
		return 0, s.ackErr
	}
}

// Ack writes OPEN_ACK. The receiver may pipeline DATA before this returns.
func (s *Stream) Ack(status byte) error {
	return s.c.writeFrame(s.c.ctx, Frame{
		Type: TypeOpenAck, StreamID: s.id, Status: status, Window: uint32(s.recvWindow),
	})
}

func (s *Stream) Read(p []byte) (int, error) {
	s.mu.Lock()
	for {
		if s.readDeadline.expired() {
			s.mu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
		if len(s.buf) > 0 || s.readEOF || s.readErr != nil {
			break
		}
		s.cond.Wait()
	}
	if len(s.buf) > 0 {
		n := copy(p, s.buf)
		s.buf = s.buf[n:]
		s.recvPending += n
		credit := 0
		if s.recvPending >= max(s.recvWindow/2, 1) {
			credit = s.recvPending
			s.recvPending = 0
			s.recvUsed -= credit
		}
		s.mu.Unlock()
		if credit > 0 {
			_ = s.c.writeFrame(s.c.ctx, Frame{Type: TypeWindow, StreamID: s.id, Window: uint32(credit)})
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
		if s.writeDeadline.expired() {
			s.sendMu.Unlock()
			return 0, os.ErrDeadlineExceeded
		}
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
	_ = s.SetReadDeadline(t)
	return s.SetWriteDeadline(t)
}

// SetReadDeadline makes a pending or future Read fail with
// os.ErrDeadlineExceeded once t passes. The zero time clears it.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline.set(t, &s.mu, s.cond)
	s.mu.Unlock()
	return nil
}

// SetWriteDeadline bounds how long Write waits for peer credit. It does not
// interrupt a frame already handed to the WebSocket: cancelling that write
// would close the whole connection, not just this stream.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.sendMu.Lock()
	s.writeDeadline.set(t, &s.sendMu, s.sendCond)
	s.sendMu.Unlock()
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
		s.acked = true
		s.ackStatus = status
		close(s.ackCh)
	})
	if status != protocol.AckOK {
		s.fail(io.ErrClosedPipe)
	}
}

// gotData buffers a DATA payload. It returns false when the peer overran the
// window it was granted; the caller resets the stream.
func (s *Stream) gotData(p []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recvUsed+len(p) > s.recvLimit {
		return false
	}
	s.recvUsed += len(p)
	if s.readEOF || s.readErr != nil {
		return true
	}
	s.buf = append(s.buf, p...)
	s.cond.Broadcast()
	return true
}

func (s *Stream) gotFin() {
	s.mu.Lock()
	s.readEOF = true
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

	s.ackOnce.Do(func() {
		s.ackErr = err
		close(s.ackCh)
	})
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
