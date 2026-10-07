package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"ws-tunnel/internal/mux"
	"ws-tunnel/internal/protocol"
)

var (
	// ErrSaturated means the session was at max_streams_per_conn for longer
	// than queue_timeout (§8).
	ErrSaturated = errors.New("node session saturated")
	// ErrNoChannel means the node has no live WebSocket right now.
	ErrNoChannel = errors.New("node session not connected")
	// ErrSessionClosed means the node went away while we were queued.
	ErrSessionClosed = errors.New("node session closed")
	// ErrDraining means the node is being drained and must not take new streams.
	ErrDraining = errors.New("node is draining")
)

// NodeStats survives reconnects and lives for the whole process (§13.1:
// counters reset on restart, not on a reconnect).
type NodeStats struct {
	Opened           atomic.Int64
	ResultOK         atomic.Int64
	ResultTimeout    atomic.Int64
	ResultNotAllowed atomic.Int64
	ResultDialFailed atomic.Int64
	ResultRejected   atomic.Int64
	Saturated        atomic.Int64
	LocalDialErrors  atomic.Int64
	Peak             atomic.Int64
	BytesIn          atomic.Int64
	BytesOut         atomic.Int64

	mu             sync.Mutex
	rateIn         int64
	rateOut        int64
	lastIn         int64
	lastOut        int64
	lastSample     time.Time
	connectedAt    time.Time
	disconnectedAt time.Time
	lastError      string
	// bindErrors counts failed binds per reverse port. It lives here rather
	// than on the portListener so the count survives a listener being torn
	// down and rebuilt when the node reconnects.
	bindErrors map[int]int64
}

// RecordResult counts one stream-open outcome for tunnel_stream_open_total.
func (s *NodeStats) RecordResult(result string) {
	switch result {
	case "ok":
		s.ResultOK.Add(1)
	case "timeout":
		s.ResultTimeout.Add(1)
	case "not_allowed":
		s.ResultNotAllowed.Add(1)
	case "dial_failed":
		s.ResultDialFailed.Add(1)
	default:
		s.ResultRejected.Add(1)
	}
}

// Result reads back one stream-open outcome. Anything unrecognised reads as
// "rejected", mirroring RecordResult's default arm.
func (s *NodeStats) Result(result string) int64 {
	switch result {
	case "ok":
		return s.ResultOK.Load()
	case "timeout":
		return s.ResultTimeout.Load()
	case "not_allowed":
		return s.ResultNotAllowed.Load()
	case "dial_failed":
		return s.ResultDialFailed.Load()
	default:
		return s.ResultRejected.Load()
	}
}

// RecordBindError counts one failed bind of a reverse port (§11: the port is
// held by another process and we are backing off).
func (s *NodeStats) RecordBindError(port int) {
	s.mu.Lock()
	if s.bindErrors == nil {
		s.bindErrors = map[int]int64{}
	}
	s.bindErrors[port]++
	s.mu.Unlock()
}

// BindErrors reports the failed-bind count for one port.
func (s *NodeStats) BindErrors(port int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bindErrors[port]
}

// ObserveDemand raises peak_demand, whose window is "since process start"
// (§8) — it is never decayed or reset.
func (s *NodeStats) ObserveDemand(v int64) {
	for {
		cur := s.Peak.Load()
		if v <= cur || s.Peak.CompareAndSwap(cur, v) {
			return
		}
	}
}

// SampleRates converts the byte counters into a bytes-per-second view.
func (s *NodeStats) SampleRates(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, out := s.BytesIn.Load(), s.BytesOut.Load()
	if !s.lastSample.IsZero() {
		if secs := now.Sub(s.lastSample).Seconds(); secs > 0 {
			s.rateIn = int64(float64(in-s.lastIn) / secs)
			s.rateOut = int64(float64(out-s.lastOut) / secs)
		}
	}
	s.lastSample, s.lastIn, s.lastOut = now, in, out
}

func (s *NodeStats) Rates() (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rateIn, s.rateOut
}

func (s *NodeStats) SetLastError(msg string) {
	s.mu.Lock()
	s.lastError = msg
	s.mu.Unlock()
}

func (s *NodeStats) Times() (connected, disconnected time.Time, lastErr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connectedAt, s.disconnectedAt, s.lastError
}

// NodeSession is one live WebSocket (JSON control + muxed L4 streams).
// Its lifetime is the node's online lifetime: when it ends, the node's
// reverse listeners go with it.
type NodeSession struct {
	Name        string
	ID          string
	ConnectedAt time.Time

	log   *slog.Logger
	stats *NodeStats

	mux *mux.Conn

	cfgMu        sync.RWMutex
	cfg          *protocol.NodeConfig
	queueTimeout time.Duration

	chMu sync.Mutex
	// waiters is the arrival-ordered queue of requests parked because the
	// session was at max_streams_per_conn. A freed slot is handed to
	// waiters[0] under chMu, and a newcomer that finds the queue non-empty
	// joins the tail instead of competing for the slot — FIFO.
	waiters []*slotWaiter

	maxStreams atomic.Int64

	activeStreams atomic.Int64
	queueDepth    atomic.Int64
	draining      atomic.Bool
	lastSeen      atomic.Int64 // unix nanos
	rttMicros     atomic.Int64

	clientStats atomic.Pointer[protocol.Stats]

	closeOnce sync.Once
	done      chan struct{}
	reasonMu  sync.Mutex
	reason    string
}

// slotWaiter is one parked request. ready is buffered so a handoff under chMu
// never blocks on a waiter that has already given up; handed records that the
// slot was transferred, so a waiter losing the race against its own timeout
// still finds the slot instead of leaking it.
type slotWaiter struct {
	ready  chan struct{}
	handed bool
}

func newNodeSession(name string, cfg *protocol.NodeConfig,
	queueTimeout time.Duration, stats *NodeStats, log *slog.Logger) *NodeSession {

	n := &NodeSession{
		Name:         name,
		ID:           randomID(),
		ConnectedAt:  time.Now(),
		log:          log,
		stats:        stats,
		cfg:          cfg,
		queueTimeout: queueTimeout,
		done:         make(chan struct{}),
	}
	n.maxStreams.Store(int64(cfg.MaxStreamsPerConn))
	n.lastSeen.Store(time.Now().UnixNano())
	return n
}

func (n *NodeSession) AttachMux(m *mux.Conn) { n.mux = m }

func (n *NodeSession) Done() <-chan struct{} { return n.done }

func (n *NodeSession) Config() *protocol.NodeConfig {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.cfg
}

func (n *NodeSession) SetConfig(cfg *protocol.NodeConfig, queueTimeout time.Duration) {
	n.cfgMu.Lock()
	n.cfg, n.queueTimeout = cfg, queueTimeout
	n.cfgMu.Unlock()
	n.maxStreams.Store(int64(cfg.MaxStreamsPerConn))
	// A raised limit may have made room for waiters that are already parked.
	n.chMu.Lock()
	n.handoffLocked()
	n.chMu.Unlock()
}

func (n *NodeSession) MaxStreams() int {
	return int(n.maxStreams.Load())
}

func (n *NodeSession) DialTimeout() time.Duration {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.cfg.DialTimeout.D()
}

func (n *NodeSession) QueueTimeout() time.Duration {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.queueTimeout
}

func (n *NodeSession) Heartbeat() time.Duration {
	n.cfgMu.RLock()
	defer n.cfgMu.RUnlock()
	return n.cfg.Heartbeat.D()
}

// SendControl writes one JSON message on the WebSocket.
func (n *NodeSession) SendControl(msg *protocol.Message) error {
	if n.mux == nil {
		return errors.New("no mux session")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return n.mux.SendControl(ctx, msg)
}

func (n *NodeSession) Mux() *mux.Conn { return n.mux }

func (n *NodeSession) Online() bool {
	return n.mux != nil && !n.mux.Closed()
}

func (n *NodeSession) OnlineChannels() int {
	if n.Online() {
		return 1
	}
	return 0
}

func (n *NodeSession) ActiveStreams() int64 { return n.activeStreams.Load() }
func (n *NodeSession) QueueDepth() int64    { return n.queueDepth.Load() }

func (n *NodeSession) Touch(rtt time.Duration) {
	n.lastSeen.Store(time.Now().UnixNano())
	if rtt > 0 {
		n.rttMicros.Store(rtt.Microseconds())
	}
}

func (n *NodeSession) LastSeen() time.Time { return time.Unix(0, n.lastSeen.Load()) }
func (n *NodeSession) RTT() time.Duration {
	return time.Duration(n.rttMicros.Load()) * time.Microsecond
}

func (n *NodeSession) SetClientStats(s *protocol.Stats) { n.clientStats.Store(s) }
func (n *NodeSession) ClientStats() *protocol.Stats     { return n.clientStats.Load() }

func (n *NodeSession) SetDraining(v bool) { n.draining.Store(v) }
func (n *NodeSession) Draining() bool     { return n.draining.Load() }

// OpenStream reserves a slot up to max_streams_per_conn and opens a mux
// stream, queueing up to queue_timeout when the session is full. The queue is
// FIFO: while anyone is parked, an arriving request joins the tail rather than
// racing them for the next freed slot.
func (n *NodeSession) OpenStream(ctx context.Context, port int) (*mux.Stream, error) {
	if n.draining.Load() {
		return nil, ErrDraining
	}
	if n.mux == nil || n.mux.Closed() {
		return nil, ErrNoChannel
	}
	timer := time.NewTimer(n.QueueTimeout())
	defer timer.Stop()

	n.queueDepth.Add(1)
	queued := true
	defer func() {
		if queued {
			n.queueDepth.Add(-1)
		}
	}()
	n.observeDemand()

	for {
		ok, waiter := n.acquire()
		if !ok {
			select {
			case <-waiter.ready:
			case <-timer.C:
				if !n.abandon(waiter) {
					n.stats.Saturated.Add(1)
					return nil, ErrSaturated
				}
			case <-n.done:
				if !n.abandon(waiter) {
					return nil, ErrSessionClosed
				}
			case <-ctx.Done():
				if !n.abandon(waiter) {
					return nil, ctx.Err()
				}
			}
		}

		st, err := n.mux.Open(ctx, port)
		if err != nil {
			n.release()
			if n.mux == nil || n.mux.Closed() {
				return nil, ErrSessionClosed
			}
			return nil, err
		}
		n.queueDepth.Add(-1)
		queued = false
		n.stats.Opened.Add(1)
		n.observeDemand()
		return st, nil
	}
}

func (n *NodeSession) acquire() (bool, *slotWaiter) {
	n.chMu.Lock()
	defer n.chMu.Unlock()
	if len(n.waiters) == 0 {
		if n.reserveLocked() {
			return true, nil
		}
	}
	w := &slotWaiter{ready: make(chan struct{}, 1)}
	n.waiters = append(n.waiters, w)
	return false, w
}

func (n *NodeSession) abandon(w *slotWaiter) bool {
	n.chMu.Lock()
	if w.handed {
		n.chMu.Unlock()
		<-w.ready
		return true
	}
	for i, cur := range n.waiters {
		if cur == w {
			n.waiters = append(n.waiters[:i], n.waiters[i+1:]...)
			break
		}
	}
	n.chMu.Unlock()
	return false
}

func (n *NodeSession) CloseStream(st *mux.Stream) {
	if st != nil {
		_ = st.Close()
	}
	n.release()
}

func (n *NodeSession) reserveLocked() bool {
	max := n.maxStreams.Load()
	if n.activeStreams.Load() >= max {
		return false
	}
	n.activeStreams.Add(1)
	return true
}

func (n *NodeSession) release() {
	n.chMu.Lock()
	n.activeStreams.Add(-1)
	n.handoffLocked()
	n.chMu.Unlock()
}

func (n *NodeSession) handoffLocked() {
	for len(n.waiters) > 0 {
		if !n.reserveLocked() {
			return
		}
		w := n.waiters[0]
		n.waiters = n.waiters[1:]
		w.handed = true
		w.ready <- struct{}{}
	}
}

func (n *NodeSession) observeDemand() {
	n.stats.ObserveDemand(n.activeStreams.Load() + n.queueDepth.Load())
}

// Close tears the session down and RST every in-flight stream.
func (n *NodeSession) Close(reason string) {
	n.closeOnce.Do(func() {
		n.reasonMu.Lock()
		n.reason = reason
		n.reasonMu.Unlock()

		close(n.done)
		if n.mux != nil {
			n.mux.Close()
		}
	})
}

func (n *NodeSession) Reason() string {
	n.reasonMu.Lock()
	defer n.reasonMu.Unlock()
	return n.reason
}

// Registry tracks the online session per node plus process-lifetime stats.
type Registry struct {
	log   *slog.Logger
	mu    sync.RWMutex
	live  map[string]*NodeSession
	stats map[string]*NodeStats
}

func newRegistry(log *slog.Logger) *Registry {
	return &Registry{log: log, live: map[string]*NodeSession{}, stats: map[string]*NodeStats{}}
}

// Stats returns (creating if needed) the persistent stats for a node.
func (r *Registry) Stats(node string) *NodeStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.stats[node]
	if !ok {
		s = &NodeStats{}
		r.stats[node] = s
	}
	return s
}

// Takeover installs sess as the live session, returning the previous one if
// any. Listeners stay up; the caller is responsible for closing the old
// session so its in-flight streams die without dropping the reverse ports.
func (r *Registry) Takeover(sess *NodeSession) *NodeSession {
	r.mu.Lock()
	old := r.live[sess.Name]
	r.installLocked(sess)
	r.mu.Unlock()
	return old
}

func (r *Registry) installLocked(sess *NodeSession) {
	r.live[sess.Name] = sess
	s := r.stats[sess.Name]
	if s == nil {
		s = &NodeStats{}
		r.stats[sess.Name] = s
	}
	s.mu.Lock()
	s.connectedAt = sess.ConnectedAt
	s.disconnectedAt = time.Time{}
	s.mu.Unlock()
}

// Unregister removes the session if it is still the current one.
func (r *Registry) Unregister(sess *NodeSession, reason string) {
	r.mu.Lock()
	cur, ok := r.live[sess.Name]
	if ok && cur == sess {
		delete(r.live, sess.Name)
	}
	s := r.stats[sess.Name]
	r.mu.Unlock()
	if s != nil && ok && cur == sess {
		s.mu.Lock()
		s.disconnectedAt = time.Now()
		if reason != "" {
			s.lastError = reason
		}
		s.mu.Unlock()
	}
}

func (r *Registry) Get(node string) *NodeSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.live[node]
}

func (r *Registry) List() []*NodeSession {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*NodeSession, 0, len(r.live))
	for _, s := range r.live {
		out = append(out, s)
	}
	return out
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b[:])
}
