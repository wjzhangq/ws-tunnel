package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"ws-tunnel/internal/config"
	"ws-tunnel/internal/protocol"
)

// PortStatus is the per-port view exposed by /status (§13.1). BindErrors is an
// additive extension: §13.1 has no field for it, but a port that is quietly
// failing to bind is otherwise indistinguishable from one that just came up.
type PortStatus struct {
	Port        int     `json:"port"`
	Remote      string  `json:"remote"`
	Listening   bool    `json:"listening"`
	ActiveConns int64   `json:"active_conns"`
	BindErrors  int64   `json:"bind_errors"`
	Error       *string `json:"error"`
}

// listenerManager owns every reverse TCP listener. A listener exists only
// while its node has a live control channel (§9).
type listenerManager struct {
	srv *Server

	mu    sync.Mutex
	items map[int]*portListener
}

func newListenerManager(srv *Server) *listenerManager {
	return &listenerManager{srv: srv, items: map[int]*portListener{}}
}

type portListener struct {
	m    *listenerManager
	port int
	node string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	ln        net.Listener
	listening bool
	lastErr   string
	conns     map[net.Conn]struct{}

	activeConns atomic.Int64
}

// StartNode brings up every port belonging to a node that just came online.
func (m *listenerManager) StartNode(node string) {
	cfg := m.srv.Config()
	for _, spec := range cfg.PortsOf(node) {
		m.StartPort(spec)
	}
}

// StartPort starts one listener, but only if the owning node is online.
func (m *listenerManager) StartPort(spec *config.PortSpec) {
	if m.srv.registry.Get(spec.Node) == nil {
		m.srv.log.Debug("port not started: node offline", "port", spec.Port, "node", spec.Node)
		return
	}
	m.mu.Lock()
	if cur, ok := m.items[spec.Port]; ok {
		if cur.node == spec.Node {
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		m.StopPort(spec.Port, false)
		m.mu.Lock()
	}
	ctx, cancel := context.WithCancel(m.srv.baseCtx)
	pl := &portListener{
		m: m, port: spec.Port, node: spec.Node,
		ctx: ctx, cancel: cancel,
		conns: map[net.Conn]struct{}{},
	}
	m.items[spec.Port] = pl
	m.mu.Unlock()

	pl.wg.Add(1)
	go pl.run()
}

// StopPort stops a listener. Graceful keeps established forwards alive until
// they end naturally (config reload removing a port, §12.2); non-graceful
// also tears them down (node went offline, §5.1).
func (m *listenerManager) StopPort(port int, graceful bool) {
	m.mu.Lock()
	pl, ok := m.items[port]
	if ok {
		delete(m.items, port)
	}
	m.mu.Unlock()
	if !ok {
		return
	}
	pl.stop(graceful)
}

// StopNode releases every listener owned by a node.
func (m *listenerManager) StopNode(node string, graceful bool) {
	m.mu.Lock()
	var victims []*portListener
	for port, pl := range m.items {
		if pl.node == node {
			victims = append(victims, pl)
			delete(m.items, port)
		}
	}
	m.mu.Unlock()
	for _, pl := range victims {
		pl.stop(graceful)
	}
}

// StopAll stops accepting everywhere (used by graceful shutdown, step 1).
func (m *listenerManager) StopAll(graceful bool) {
	m.mu.Lock()
	victims := make([]*portListener, 0, len(m.items))
	for port, pl := range m.items {
		victims = append(victims, pl)
		delete(m.items, port)
	}
	m.mu.Unlock()
	for _, pl := range victims {
		pl.stop(graceful)
	}
}

// Snapshot reports the state of a node's ports, including ports that are
// configured but not listening (node offline, or the bind failed).
func (m *listenerManager) Snapshot(node string, cfg *config.Config) []PortStatus {
	m.mu.Lock()
	live := map[int]*portListener{}
	for port, pl := range m.items {
		if pl.node == node {
			live[port] = pl
		}
	}
	m.mu.Unlock()

	stats := m.srv.registry.Stats(node)
	out := []PortStatus{}
	for _, spec := range cfg.PortsOf(node) {
		st := PortStatus{Port: spec.Port, Remote: spec.Remote, BindErrors: stats.BindErrors(spec.Port)}
		if pl, ok := live[spec.Port]; ok {
			listening, errMsg := pl.state()
			st.Listening = listening
			st.ActiveConns = pl.activeConns.Load()
			if errMsg != "" {
				e := errMsg
				st.Error = &e
			}
		} else {
			e := "node offline"
			st.Error = &e
		}
		out = append(out, st)
	}
	return out
}

func (m *listenerManager) listening() []*portListener {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*portListener, 0, len(m.items))
	for _, pl := range m.items {
		out = append(out, pl)
	}
	return out
}

func (pl *portListener) state() (bool, string) {
	pl.mu.Lock()
	defer pl.mu.Unlock()
	return pl.listening, pl.lastErr
}

// run binds the port and accepts until told to stop. A bind failure (port
// taken by another process) is retried with backoff and surfaced in /status
// as listening:false + error (§11).
func (pl *portListener) run() {
	defer pl.wg.Done()
	backoff := time.Second
	for {
		if pl.ctx.Err() != nil {
			return
		}
		var lc net.ListenConfig
		ln, err := lc.Listen(pl.ctx, "tcp", config.ListenAddr(pl.port))
		if err != nil {
			pl.mu.Lock()
			pl.listening, pl.lastErr = false, err.Error()
			pl.mu.Unlock()
			pl.m.srv.registry.Stats(pl.node).RecordBindError(pl.port)
			pl.m.srv.log.Error("bind reverse port failed, will retry",
				"port", pl.port, "node", pl.node, "err", err, "retry_in", backoff)
			select {
			case <-pl.ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		pl.mu.Lock()
		pl.ln, pl.listening, pl.lastErr = ln, true, ""
		pl.mu.Unlock()
		pl.m.srv.log.Info("reverse port listening",
			"port", pl.port, "node", pl.node, "addr", ln.Addr().String())

		pl.accept(ln)

		pl.mu.Lock()
		pl.listening = false
		pl.mu.Unlock()
		if pl.ctx.Err() != nil {
			return
		}
	}
}

func (pl *portListener) accept(ln net.Listener) {
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if pl.ctx.Err() == nil {
				pl.mu.Lock()
				pl.lastErr = err.Error()
				pl.mu.Unlock()
				pl.m.srv.log.Error("accept failed", "port", pl.port, "err", err)
			}
			return
		}
		pl.track(conn)
		pl.wg.Add(1)
		go func() {
			defer pl.wg.Done()
			defer pl.untrack(conn)
			pl.m.srv.forward(pl, conn)
		}()
	}
}

func (pl *portListener) track(c net.Conn) {
	pl.mu.Lock()
	pl.conns[c] = struct{}{}
	pl.mu.Unlock()
	pl.activeConns.Add(1)
}

func (pl *portListener) untrack(c net.Conn) {
	pl.mu.Lock()
	delete(pl.conns, c)
	pl.mu.Unlock()
	pl.activeConns.Add(-1)
	_ = c.Close()
}

func (pl *portListener) stop(graceful bool) {
	pl.cancel()
	pl.mu.Lock()
	ln := pl.ln
	pl.listening = false
	var conns []net.Conn
	if !graceful {
		for c := range pl.conns {
			conns = append(conns, c)
		}
	}
	pl.mu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	for _, c := range conns {
		_ = c.Close()
	}
	pl.m.srv.log.Info("reverse port released", "port", pl.port, "node", pl.node, "graceful", graceful)
}

// forward runs one external connection through the tunnel (§9, steps 5-10).
func (s *Server) forward(pl *portListener, conn net.Conn) {
	stats := s.registry.Stats(pl.node)
	sess := s.registry.Get(pl.node)
	if sess == nil {
		stats.RecordResult("rejected")
		s.log.Warn("dropping connection: node offline", "port", pl.port, "node", pl.node)
		return
	}

	st, ch, err := sess.OpenStream(s.baseCtx)
	if err != nil {
		switch {
		case errors.Is(err, ErrSaturated):
			stats.RecordResult("rejected")
			s.log.Warn("dropping connection: all channels saturated",
				"port", pl.port, "node", pl.node)
		default:
			stats.RecordResult("rejected")
			s.log.Warn("dropping connection", "port", pl.port, "node", pl.node, "err", err)
		}
		return
	}
	defer sess.CloseStream(ch, st)

	if err := protocol.WriteStreamHeader(st, pl.port); err != nil {
		stats.RecordResult("rejected")
		s.log.Warn("write stream header failed", "port", pl.port, "node", pl.node, "err", err)
		return
	}

	// Pipeline the external bytes immediately — we do not wait for the ack,
	// so nothing is added to first-byte latency (§7.1).
	upDone := make(chan struct{})
	go func() {
		defer close(upDone)
		n, _ := io.Copy(st, conn)
		stats.BytesIn.Add(n)
		// Half-close so the client's local service sees EOF.
		_ = st.Close()
	}()

	_ = st.SetReadDeadline(time.Now().Add(sess.DialTimeout()))
	status, err := protocol.ReadAck(st)
	if err != nil {
		stats.RecordResult("timeout")
		s.log.Warn("no ack from client", "port", pl.port, "node", pl.node, "err", err)
		return
	}
	_ = st.SetReadDeadline(time.Time{})

	if status != protocol.AckOK {
		result := protocol.AckResult(status)
		stats.RecordResult(result)
		if status == protocol.AckDialFailed {
			stats.LocalDialErrors.Add(1)
		}
		stats.SetLastError(protocol.AckReason(status))
		s.log.Warn("client refused the stream",
			"port", pl.port, "node", pl.node, "status", status, "reason", protocol.AckReason(status))
		return
	}

	stats.RecordResult("ok")
	n, _ := io.Copy(conn, st)
	stats.BytesOut.Add(n)
	_ = conn.Close()
	<-upDone
}
