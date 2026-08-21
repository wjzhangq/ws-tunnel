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
	"unicode/utf8"

	"github.com/coder/websocket"
	"github.com/xtaci/smux"

	"ws-tunnel/internal/protocol"
	"ws-tunnel/internal/wsutil"
)

var (
	// ErrNodeBusy is returned when a node already has a live control channel;
	// the newcomer is refused (§4).
	ErrNodeBusy = errors.New("node already has a control channel")
	// ErrSaturated means every data channel was at max_streams_per_conn for
	// longer than queue_timeout (§8).
	ErrSaturated = errors.New("all data channels saturated")
	// ErrNoChannel means the node has no usable data channel right now.
	ErrNoChannel = errors.New("no data channel online")
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

// DataChannel is one WS carrying a smux session (§5.2).
type DataChannel struct {
	ID     int
	sess   *smux.Session
	ws     *websocket.Conn
	active atomic.Int64
	closed atomic.Bool
}

func (c *DataChannel) Close() {
	if c.closed.Swap(true) {
		return
	}
	if c.sess != nil {
		_ = c.sess.Close()
	}
	wsutil.Close(c.ws, websocket.StatusNormalClosure, "channel closed")
}

// NodeSession is one live control channel plus the fixed pool of data
// channels behind it. Its lifetime is the node's online lifetime: when it
// ends, the node's reverse listeners go with it (§5.1, §9).
type NodeSession struct {
	Name        string
	ID          string
	ConnectedAt time.Time

	log   *slog.Logger
	stats *NodeStats

	ctrl   *websocket.Conn
	ctrlMu sync.Mutex

	cfgMu        sync.RWMutex
	cfg          *protocol.NodeConfig
	queueTimeout time.Duration

	chMu     sync.Mutex
	channels map[int]*DataChannel
	// waiters is the arrival-ordered queue of requests parked because every
	// channel was full. A freed slot is handed to waiters[0] under chMu, and a
	// newcomer that finds the queue non-empty joins the tail instead of
	// competing for the slot — that is what makes the wait FIFO (§8).
	waiters []*slotWaiter

	// maxStreams mirrors cfg.MaxStreamsPerConn so a slot handoff can read the
	// current limit without taking cfgMu while holding chMu.
	maxStreams atomic.Int64

	chanSeq       atomic.Int64
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
	ready  chan *DataChannel
	handed bool
}

func newNodeSession(name string, ctrl *websocket.Conn, cfg *protocol.NodeConfig,
	queueTimeout time.Duration, stats *NodeStats, log *slog.Logger) *NodeSession {

	n := &NodeSession{
		Name:         name,
		ID:           randomID(),
		ConnectedAt:  time.Now(),
		log:          log,
		stats:        stats,
		ctrl:         ctrl,
		cfg:          cfg,
		queueTimeout: queueTimeout,
		channels:     map[int]*DataChannel{},
		done:         make(chan struct{}),
	}
	n.maxStreams.Store(int64(cfg.MaxStreamsPerConn))
	n.lastSeen.Store(time.Now().UnixNano())
	return n
}

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

// SendControl writes one JSON message on the control channel.
func (n *NodeSession) SendControl(msg *protocol.Message) error {
	n.ctrlMu.Lock()
	defer n.ctrlMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return wsutil.WriteJSON(ctx, n.ctrl, msg)
}

// NextChannelID hands out the channel_id echoed back in the data-channel
// welcome; ids are unique within a session, not across reconnects.
func (n *NodeSession) NextChannelID() int { return int(n.chanSeq.Add(1)) }

func (n *NodeSession) AddChannel(ch *DataChannel) {
	n.chMu.Lock()
	n.channels[ch.ID] = ch
	// Fresh capacity: give it to whoever has been waiting longest.
	n.handoffLocked()
	n.chMu.Unlock()
}

func (n *NodeSession) RemoveChannel(ch *DataChannel) {
	n.chMu.Lock()
	if cur, ok := n.channels[ch.ID]; ok && cur == ch {
		delete(n.channels, ch.ID)
	}
	n.chMu.Unlock()
	ch.Close()
}

func (n *NodeSession) OnlineChannels() int {
	n.chMu.Lock()
	defer n.chMu.Unlock()
	return len(n.channels)
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

// OpenStream reserves a slot on the least-loaded channel and opens a smux
// stream, queueing up to queue_timeout when every channel is full (§8). The
// queue is FIFO: while anyone is parked, an arriving request joins the tail
// rather than racing them for the next freed slot, which bounds the wait
// instead of leaving a long tail under sustained saturation.
func (n *NodeSession) OpenStream(ctx context.Context) (*smux.Stream, *DataChannel, error) {
	if n.draining.Load() {
		return nil, nil, ErrDraining
	}
	timer := time.NewTimer(n.QueueTimeout())
	defer timer.Stop()

	// A request counts towards demand from the moment it arrives: queued
	// while it waits, active once it holds a slot — never both at once.
	n.queueDepth.Add(1)
	queued := true
	defer func() {
		if queued {
			n.queueDepth.Add(-1)
		}
	}()
	n.observeDemand()

	for {
		ch, waiter := n.acquire()
		if ch == nil {
			// Parked at the tail. Whoever frees a slot hands it to us.
			select {
			case ch = <-waiter.ready:
			case <-timer.C:
				if ch = n.abandon(waiter); ch == nil {
					if n.OnlineChannels() == 0 {
						return nil, nil, ErrNoChannel
					}
					n.stats.Saturated.Add(1)
					return nil, nil, ErrSaturated
				}
			case <-n.done:
				if ch = n.abandon(waiter); ch == nil {
					return nil, nil, ErrSessionClosed
				}
			case <-ctx.Done():
				if ch = n.abandon(waiter); ch == nil {
					return nil, nil, ctx.Err()
				}
			}
		}

		st, err := ch.sess.OpenStream()
		if err != nil {
			n.release(ch)
			n.log.Warn("data channel unusable, dropping it",
				"node", n.Name, "channel", ch.ID, "err", err)
			n.RemoveChannel(ch)
			continue
		}
		n.queueDepth.Add(-1)
		queued = false
		n.stats.Opened.Add(1)
		n.observeDemand()
		return st, ch, nil
	}
}

// acquire returns a reserved channel, or parks the caller and returns the
// waiter it was queued as. Reserving and parking happen under one lock hold so
// a slot freed in between cannot be missed.
func (n *NodeSession) acquire() (*DataChannel, *slotWaiter) {
	n.chMu.Lock()
	defer n.chMu.Unlock()
	if len(n.waiters) == 0 {
		if ch := n.reserveLocked(); ch != nil {
			return ch, nil
		}
	}
	w := &slotWaiter{ready: make(chan *DataChannel, 1)}
	n.waiters = append(n.waiters, w)
	return nil, w
}

// abandon takes a waiter out of the queue. It returns non-nil when a slot was
// already handed over — the handoff won the race against the caller's timeout,
// and dropping it on the floor would lose the slot for good.
func (n *NodeSession) abandon(w *slotWaiter) *DataChannel {
	n.chMu.Lock()
	if w.handed {
		n.chMu.Unlock()
		return <-w.ready
	}
	for i, cur := range n.waiters {
		if cur == w {
			n.waiters = append(n.waiters[:i], n.waiters[i+1:]...)
			break
		}
	}
	n.chMu.Unlock()
	return nil
}

// CloseStream releases the reserved slot.
func (n *NodeSession) CloseStream(ch *DataChannel, st *smux.Stream) {
	if st != nil {
		_ = st.Close()
	}
	n.release(ch)
}

// reserveLocked picks the least-loaded usable channel and books a slot on it.
// Caller holds chMu.
func (n *NodeSession) reserveLocked() *DataChannel {
	max := int64(n.maxStreams.Load())
	var best *DataChannel
	var bestActive int64
	for _, ch := range n.channels {
		if ch.closed.Load() || ch.sess.IsClosed() {
			continue
		}
		a := ch.active.Load()
		if a >= max {
			continue
		}
		if best == nil || a < bestActive {
			best, bestActive = ch, a
		}
	}
	if best != nil {
		best.active.Add(1)
		n.activeStreams.Add(1)
	}
	return best
}

func (n *NodeSession) release(ch *DataChannel) {
	if ch == nil {
		return
	}
	n.chMu.Lock()
	ch.active.Add(-1)
	n.activeStreams.Add(-1)
	n.handoffLocked()
	n.chMu.Unlock()
}

// handoffLocked passes freed capacity to the head of the queue, in order, for
// as long as both a waiter and a free slot exist. Caller holds chMu.
func (n *NodeSession) handoffLocked() {
	for len(n.waiters) > 0 {
		ch := n.reserveLocked()
		if ch == nil {
			return
		}
		w := n.waiters[0]
		n.waiters = n.waiters[1:]
		w.handed = true
		w.ready <- ch // buffered, and each waiter is served once
	}
}

func (n *NodeSession) observeDemand() {
	n.stats.ObserveDemand(n.activeStreams.Load() + n.queueDepth.Load())
}

// Close tears the session down: control channel, every data channel, and
// every stream riding on them.
func (n *NodeSession) Close(reason string) {
	n.closeOnce.Do(func() {
		n.reasonMu.Lock()
		n.reason = reason
		n.reasonMu.Unlock()

		close(n.done)

		n.chMu.Lock()
		chans := make([]*DataChannel, 0, len(n.channels))
		for _, ch := range n.channels {
			chans = append(chans, ch)
		}
		n.channels = map[int]*DataChannel{}
		n.chMu.Unlock()
		for _, ch := range chans {
			ch.Close()
		}

		wsutil.Close(n.ctrl, websocket.StatusNormalClosure, truncate(reason, 100))
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

// Register installs a session, refusing the newcomer if one is already live.
func (r *Registry) Register(sess *NodeSession) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.live[sess.Name]; ok {
		return ErrNodeBusy
	}
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
	return nil
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

// truncate caps s at n bytes without splitting a UTF-8 rune, which matters
// because the result goes into a WebSocket close reason.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
