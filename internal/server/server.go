// Package server implements tunnel-server: one WS entry point for clients,
// one reverse TCP listener per configured port, incremental config reload and
// graceful shutdown.
package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/fsnotify/fsnotify"
	"github.com/xtaci/smux"

	"ws-tunnel/internal/config"
	"ws-tunnel/internal/protocol"
	"ws-tunnel/internal/wsutil"
)

// ShutdownGrace is the hard-coded ceiling for draining on SIGTERM (§14).
const ShutdownGrace = 30 * time.Second

// handshakeTimeout bounds how long an unauthenticated WS may stay open.
const handshakeTimeout = 15 * time.Second

// Server is the whole tunnel-server process.
type Server struct {
	log     *slog.Logger
	cfgPath string

	baseCtx    context.Context
	baseCancel context.CancelFunc

	cfgMu sync.RWMutex
	cfg   *config.Config

	registry  *Registry
	listeners *listenerManager

	// srvMu guards the two HTTP servers and errCh: Run starts them, a config
	// reload can replace them, and shutdown stops them.
	srvMu     sync.Mutex
	wsSrv     *http.Server
	statusSrv *http.Server
	errCh     chan error

	shuttingDown chan struct{}
	closeOnce    sync.Once
	wg           sync.WaitGroup
}

// New loads the config and prepares the server. A config error here is fatal
// by design (§3.3).
func New(cfgPath string, log *slog.Logger) (*Server, error) {
	cfg, warnings, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(cfgPath)
	if err == nil {
		cfgPath = abs
	}
	s := &Server{
		log:          log,
		cfgPath:      cfgPath,
		cfg:          cfg,
		shuttingDown: make(chan struct{}),
	}
	for _, w := range warnings {
		log.Warn("config", "detail", w)
	}
	s.registry = newRegistry(log)
	s.listeners = newListenerManager(s)
	return s, nil
}

// Config returns the current snapshot.
func (s *Server) Config() *config.Config {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

func (s *Server) setConfig(cfg *config.Config) {
	s.cfgMu.Lock()
	s.cfg = cfg
	s.cfgMu.Unlock()
}

// Run blocks until ctx is cancelled or a fatal listen error occurs.
func (s *Server) Run(ctx context.Context) error {
	s.baseCtx, s.baseCancel = context.WithCancel(context.Background())
	defer s.baseCancel()

	cfg := s.Config()

	errCh := make(chan error, 2)
	s.srvMu.Lock()
	s.errCh = errCh
	s.srvMu.Unlock()

	wsSrv, err := s.serve("ws entry point", cfg.Listen, s.wsHandler())
	if err != nil {
		return fmt.Errorf("ws listener: %w", err)
	}
	s.srvMu.Lock()
	s.wsSrv = wsSrv
	s.srvMu.Unlock()

	if cfg.StatusListen != "" {
		statusSrv, err := s.serve("status endpoint", cfg.StatusListen, s.statusHandler())
		if err != nil {
			return fmt.Errorf("status listener: %w", err)
		}
		s.srvMu.Lock()
		s.statusSrv = statusSrv
		s.srvMu.Unlock()
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.watchConfig()
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.sampleRates()
	}()

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errCh:
		s.log.Error("fatal listener error", "err", runErr)
	}

	s.shutdown()
	if runErr != nil {
		return runErr
	}
	return nil
}

func (s *Server) wsHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc(protocol.WSPath, s.handleWS)
	return mux
}

// statusHandler serves §13's read-only telemetry. It carries no authentication,
// which is why config.go pins it to loopback.
func (s *Server) statusHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

// serve binds addr and starts serving it. The bind is done here rather than
// inside ListenAndServe so the caller learns synchronously whether the address
// was actually claimed — that is what lets a reload keep the old listener when
// the new address is unavailable.
func (s *Server) serve(what, addr string, h http.Handler) (*http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{Addr: addr, Handler: h}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.log.Info(what+" listening", "addr", ln.Addr().String())
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.srvMu.Lock()
			ch := s.errCh
			s.srvMu.Unlock()
			select {
			case ch <- fmt.Errorf("%s: %w", what, err):
			default:
			}
		}
	}()
	return srv, nil
}

// ─── WS entry point ──────────────────────────────────────────────────────

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // plain ws in a trusted network; no origin model
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		s.log.Warn("ws accept failed", "remote", r.RemoteAddr, "err", err)
		return
	}
	wsutil.Prepare(c)

	hsCtx, cancel := context.WithTimeout(r.Context(), handshakeTimeout)
	msg, err := wsutil.ReadJSON(hsCtx, c)
	cancel()
	if err != nil {
		wsutil.Close(c, websocket.StatusPolicyViolation, "handshake read failed")
		return
	}
	if msg.Type != protocol.TypeHello {
		s.reject(c, protocol.ErrBadRequest, "expected hello")
		return
	}

	spec, err := s.authenticate(msg)
	if err != nil {
		s.log.Warn("authentication failed", "remote", r.RemoteAddr, "node", msg.Node, "err", err)
		s.reject(c, protocol.ErrAuthFailed, "authentication failed")
		return
	}

	switch msg.Role {
	case protocol.RoleControl:
		s.serveControl(r.Context(), c, spec, r.RemoteAddr)
	case protocol.RoleData:
		s.serveData(r.Context(), c, spec, msg)
	default:
		s.reject(c, protocol.ErrBadRequest, "unknown role %q", msg.Role)
	}
}

// authenticate resolves the node either by explicit name, or — when the
// client was started with nothing but url+key — by its globally unique key.
func (s *Server) authenticate(msg *protocol.Message) (*config.NodeSpec, error) {
	cfg := s.Config()
	if msg.Key == "" {
		return nil, errors.New("empty key")
	}
	if msg.Node != "" {
		spec, ok := cfg.Nodes[msg.Node]
		if !ok {
			return nil, fmt.Errorf("unknown node %q", msg.Node)
		}
		if subtle.ConstantTimeCompare([]byte(spec.Key), []byte(msg.Key)) != 1 {
			return nil, fmt.Errorf("key mismatch for node %q", msg.Node)
		}
		return spec, nil
	}
	spec := cfg.NodeByKey(msg.Key)
	if spec == nil {
		return nil, errors.New("key does not match any node")
	}
	return spec, nil
}

func (s *Server) reject(c *websocket.Conn, code, format string, args ...any) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = wsutil.WriteJSON(ctx, c, protocol.Errorf(code, format, args...))
	wsutil.Close(c, websocket.StatusPolicyViolation, code)
}

// ─── control channel ─────────────────────────────────────────────────────

func (s *Server) serveControl(ctx context.Context, c *websocket.Conn, spec *config.NodeSpec, remote string) {
	cfg := s.Config()
	nodeCfg := cfg.NodeConfig(spec.Name)
	if nodeCfg == nil {
		s.reject(c, protocol.ErrInternal, "node vanished from config")
		return
	}
	stats := s.registry.Stats(spec.Name)
	sess := newNodeSession(spec.Name, c, nodeCfg, cfg.Settings.QueueTimeout, stats, s.log)

	if err := s.registry.Register(sess); err != nil {
		s.log.Warn("refusing duplicate control channel", "node", spec.Name, "remote", remote)
		s.reject(c, protocol.ErrNodeBusy, "node %s already has a control channel", spec.Name)
		return
	}

	if err := sess.SendControl(&protocol.Message{
		Type:      protocol.TypeWelcome,
		Session:   sess.ID,
		Heartbeat: nodeCfg.Heartbeat,
		Channels:  nodeCfg.Channels,
		Config:    nodeCfg,
	}); err != nil {
		s.registry.Unregister(sess, "welcome write failed: "+err.Error())
		sess.Close("welcome write failed")
		return
	}

	s.log.Info("node online", "node", spec.Name, "session", sess.ID,
		"remote", remote, "channels", nodeCfg.Channels, "ports", len(nodeCfg.Ports))
	s.listeners.StartNode(spec.Name)

	ctrlCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-sess.Done():
			cancel()
		case <-ctrlCtx.Done():
		}
	}()

	go s.pingLoop(ctrlCtx, sess)
	reason := s.controlReadLoop(ctrlCtx, sess)

	// Control channel gone ⇒ the node goes offline as a whole: listeners are
	// released, data channels dropped, in-flight forwards closed (§5.1).
	s.listeners.StopNode(spec.Name, false)
	s.registry.Unregister(sess, reason)
	sess.Close(reason)
	s.log.Info("node offline", "node", spec.Name, "session", sess.ID, "reason", reason)
}

func (s *Server) controlReadLoop(ctx context.Context, sess *NodeSession) string {
	for {
		timeout := 3 * sess.Heartbeat()
		rctx, cancel := context.WithTimeout(ctx, timeout)
		msg, err := wsutil.ReadJSON(rctx, sess.ctrl)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return "server closed the session"
			}
			return "control channel closed: " + err.Error()
		}
		sess.Touch(0)

		switch msg.Type {
		case protocol.TypePing:
			_ = sess.SendControl(&protocol.Message{Type: protocol.TypePong, Nonce: msg.Nonce, TS: msg.TS})
		case protocol.TypePong:
			if msg.TS > 0 {
				sess.Touch(time.Since(time.Unix(0, msg.TS)))
			}
		case protocol.TypeStats:
			if msg.Stats != nil {
				sess.SetClientStats(msg.Stats)
				s.registry.Stats(sess.Name).LocalDialErrors.Store(msg.Stats.LocalDialErrors)
			}
		case protocol.TypeBye:
			return "client said bye: " + msg.Reason
		case protocol.TypeError:
			s.log.Warn("client reported an error", "node", sess.Name, "code", msg.Code, "msg", msg.Msg)
		default:
			s.log.Debug("ignoring unexpected control message", "node", sess.Name, "type", msg.Type)
		}
	}
}

func (s *Server) pingLoop(ctx context.Context, sess *NodeSession) {
	interval := sess.Heartbeat()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-sess.Done():
			return
		case <-ticker.C:
		}
		if cur := sess.Heartbeat(); cur != interval && cur > 0 {
			interval = cur
			ticker.Reset(interval)
		}
		// heartbeat × 3 without a reply ⇒ the control channel is dead (§11).
		if time.Since(sess.LastSeen()) > 3*interval {
			sess.Close("control heartbeat timeout")
			return
		}
		if err := sess.SendControl(&protocol.Message{
			Type: protocol.TypePing, Nonce: randomID(), TS: time.Now().UnixNano(),
		}); err != nil {
			sess.Close("ping write failed: " + err.Error())
			return
		}
	}
}

// ─── data channels ───────────────────────────────────────────────────────

func (s *Server) serveData(ctx context.Context, c *websocket.Conn, spec *config.NodeSpec, hello *protocol.Message) {
	sess := s.registry.Get(spec.Name)
	if sess == nil || hello.Session == "" || hello.Session != sess.ID {
		s.log.Warn("rejecting data channel with a stale session", "node", spec.Name)
		s.reject(c, protocol.ErrBadSession, "session is unknown or superseded")
		return
	}

	id := sess.NextChannelID()
	hsCtx, hsCancel := context.WithTimeout(ctx, handshakeTimeout)
	err := wsutil.WriteJSON(hsCtx, c, &protocol.Message{
		Type: protocol.TypeWelcome, ChannelID: id, Session: sess.ID,
	})
	hsCancel()
	if err != nil {
		wsutil.Close(c, websocket.StatusInternalError, "welcome write failed")
		return
	}

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	nc := wsutil.NetConn(connCtx, c)
	// The server is the side that opens streams, so it takes the smux client
	// role; the client takes the server role.
	msess, err2 := smux.Client(nc, wsutil.SmuxConfig(sess.Heartbeat()))
	if err2 != nil {
		s.log.Warn("smux setup failed", "node", spec.Name, "err", err2)
		wsutil.Close(c, websocket.StatusInternalError, "smux setup failed")
		return
	}

	ch := &DataChannel{ID: id, sess: msess, ws: c}
	sess.AddChannel(ch)
	s.log.Info("data channel up", "node", spec.Name, "channel", id,
		"online", sess.OnlineChannels(), "configured", sess.Config().Channels)

	go func() {
		select {
		case <-sess.Done():
			ch.Close()
		case <-connCtx.Done():
		}
	}()

	// The client never opens streams; accepting is only how we notice the
	// session dying, and it defends against a misbehaving peer.
	for {
		st, err := msess.AcceptStream()
		if err != nil {
			break
		}
		_ = st.Close()
	}

	sess.RemoveChannel(ch)
	s.log.Info("data channel down", "node", spec.Name, "channel", id, "online", sess.OnlineChannels())
}

// ─── config reload ───────────────────────────────────────────────────────

func (s *Server) watchConfig() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	defer signal.Stop(sig)

	var events chan fsnotify.Event
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		s.log.Error("fsnotify unavailable, reload only via SIGHUP", "err", err)
	} else {
		defer watcher.Close()
		// Watch the directory: editors replace the file rather than write in
		// place, which would otherwise drop the watch.
		if err := watcher.Add(filepath.Dir(s.cfgPath)); err != nil {
			s.log.Error("watching config directory failed", "err", err)
		}
		events = watcher.Events
	}

	debounce := time.NewTimer(time.Hour)
	if !debounce.Stop() {
		<-debounce.C
	}
	base := filepath.Base(s.cfgPath)

	for {
		select {
		case <-s.baseCtx.Done():
			return
		case <-sig:
			s.log.Info("SIGHUP received, reloading config")
			debounce.Reset(0)
		case ev := <-events:
			if filepath.Base(ev.Name) != base {
				continue
			}
			debounce.Reset(200 * time.Millisecond)
		case <-debounce.C:
			newCfg, warnings, err := config.Load(s.cfgPath)
			if err != nil {
				s.log.Error("reload failed, keeping the old config", "err", err)
				continue
			}
			for _, w := range warnings {
				s.log.Warn("config", "detail", w)
			}
			s.applyReload(newCfg)
		}
	}
}

// applyReload applies the diff incrementally: connections not covered by the
// diff never notice (§12.2).
func (s *Server) applyReload(newCfg *config.Config) {
	old := s.Config()
	diff := config.DiffConfig(old, newCfg)

	endpointsChanged := old.Listen != newCfg.Listen || old.StatusListen != newCfg.StatusListen
	if diff.Empty() && !diff.SettingsChanged && !endpointsChanged {
		s.setConfig(newCfg)
		s.log.Info("reload applied, nothing changed")
		return
	}

	for _, name := range diff.RemovedNodes {
		s.drainNode(name, ShutdownGrace, "node removed from config")
	}
	for _, name := range diff.KeyChangedNodes {
		if sess := s.registry.Get(name); sess != nil {
			s.log.Info("key changed, forcing reconnect", "node", name)
			s.listeners.StopNode(name, false)
			sess.Close("node key changed")
		}
	}
	for _, port := range diff.RemovedPorts {
		// Established forwards run to completion; the port stops accepting.
		s.listeners.StopPort(port, true)
	}

	// Publish the new config before starting anything that reads from it.
	s.setConfig(newCfg)

	if endpointsChanged {
		s.rebindEndpoints(old, newCfg)
	}

	for _, port := range diff.AddedPorts {
		if spec, ok := newCfg.Ports[port]; ok {
			s.listeners.StartPort(spec)
		}
	}

	pushed := map[string]bool{}
	for _, name := range diff.ChangedNodes {
		s.pushConfig(name, newCfg)
		pushed[name] = true
	}
	if diff.SettingsChanged {
		// heartbeat / max_streams_per_conn are global; everyone gets them.
		for _, sess := range s.registry.List() {
			if !pushed[sess.Name] {
				s.pushConfig(sess.Name, newCfg)
			}
		}
	}

	s.log.Info("reload applied", "diff", diff.String())
}

// rebindEndpoints moves the WS entry point and/or the status endpoint to a new
// address without restarting the process (§9 gap: previously restart-only).
//
// The design doc leaves the semantics open, so this is the conservative reading:
// bind the new address before releasing the old one, and on failure keep the old
// address serving and report the error rather than leaving the server deaf. The
// old WS endpoint is drained, not cut, so control channels and their reverse
// ports survive the move and clients reconnect to the new address on their own
// schedule.
func (s *Server) rebindEndpoints(old, newCfg *config.Config) {
	if old.Listen != newCfg.Listen {
		next, err := s.serve("ws entry point", newCfg.Listen, s.wsHandler())
		if err != nil {
			s.log.Error("rebind listen failed, keeping the current address",
				"from", old.Listen, "to", newCfg.Listen, "err", err)
		} else {
			s.srvMu.Lock()
			prev := s.wsSrv
			s.wsSrv = next
			s.srvMu.Unlock()
			s.log.Info("listen rebound", "from", old.Listen, "to", newCfg.Listen)
			s.retireServer(prev)
		}
	}

	if old.StatusListen == newCfg.StatusListen {
		return
	}
	var next *http.Server
	if newCfg.StatusListen != "" {
		var err error
		next, err = s.serve("status endpoint", newCfg.StatusListen, s.statusHandler())
		if err != nil {
			s.log.Error("rebind status_listen failed, keeping the current address",
				"from", old.StatusListen, "to", newCfg.StatusListen, "err", err)
			return
		}
	}
	s.srvMu.Lock()
	prev := s.statusSrv
	s.statusSrv = next
	s.srvMu.Unlock()
	s.log.Info("status_listen rebound", "from", old.StatusListen, "to", newCfg.StatusListen)
	s.retireServer(prev)
}

// retireServer stops accepting on a replaced endpoint and lets its in-flight
// requests finish, mirroring how a removed reverse port is stopped gracefully.
func (s *Server) retireServer(prev *http.Server) {
	if prev == nil {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), ShutdownGrace)
		defer cancel()
		if err := prev.Shutdown(ctx); err != nil {
			s.log.Warn("retiring old endpoint timed out, closing it", "addr", prev.Addr, "err", err)
			_ = prev.Close()
		}
	}()
}

func (s *Server) pushConfig(node string, cfg *config.Config) {
	sess := s.registry.Get(node)
	if sess == nil {
		return
	}
	nodeCfg := cfg.NodeConfig(node)
	if nodeCfg == nil {
		return
	}
	sess.SetConfig(nodeCfg, cfg.Settings.QueueTimeout)
	if err := sess.SendControl(&protocol.Message{Type: protocol.TypeReloadConfig, Config: nodeCfg}); err != nil {
		s.log.Warn("reload_config push failed", "node", node, "err", err)
		return
	}
	s.log.Info("reload_config pushed", "node", node, "ports", len(nodeCfg.Ports), "channels", nodeCfg.Channels)
}

// drainNode asks a node to stop taking new streams, waits for the in-flight
// ones, then disconnects it.
func (s *Server) drainNode(name string, grace time.Duration, reason string) {
	sess := s.registry.Get(name)
	if sess == nil {
		return
	}
	sess.SetDraining(true)
	deadline := time.Now().Add(grace)
	_ = sess.SendControl(&protocol.Message{Type: protocol.TypeDrain, Deadline: &deadline})
	s.listeners.StopNode(name, true)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.waitDrained(sess, deadline)
		_ = sess.SendControl(&protocol.Message{Type: protocol.TypeBye, Reason: reason})
		s.listeners.StopNode(name, false)
		sess.Close(reason)
	}()
}

func (s *Server) waitDrained(sess *NodeSession, deadline time.Time) {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		if sess.ActiveStreams() == 0 {
			return
		}
		select {
		case <-t.C:
			if time.Now().After(deadline) {
				s.log.Warn("drain deadline reached, closing anyway",
					"node", sess.Name, "active_streams", sess.ActiveStreams())
				return
			}
		case <-sess.Done():
			return
		}
	}
}

// sampleRates keeps the bytes-per-second figures in /status fresh.
func (s *Server) sampleRates() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.baseCtx.Done():
			return
		case now := <-t.C:
			for _, sess := range s.registry.List() {
				s.registry.Stats(sess.Name).SampleRates(now)
			}
		}
	}
}

// ─── shutdown ────────────────────────────────────────────────────────────

func (s *Server) shutdown() {
	s.closeOnce.Do(func() {
		close(s.shuttingDown)
		s.log.Info("shutting down", "grace", ShutdownGrace)

		// 1. stop accepting on every reverse port, keep in-flight forwards
		s.listeners.StopAll(true)

		// 2. tell every node to drain
		deadline := time.Now().Add(ShutdownGrace)
		sessions := s.registry.List()
		for _, sess := range sessions {
			sess.SetDraining(true)
			_ = sess.SendControl(&protocol.Message{Type: protocol.TypeDrain, Deadline: &deadline})
		}

		// 3. wait for in-flight streams
		var wg sync.WaitGroup
		for _, sess := range sessions {
			wg.Add(1)
			go func(ns *NodeSession) {
				defer wg.Done()
				s.waitDrained(ns, deadline)
			}(sess)
		}
		wg.Wait()

		// 4. bye, then close everything
		for _, sess := range sessions {
			_ = sess.SendControl(&protocol.Message{Type: protocol.TypeBye, Reason: "server shutting down"})
			sess.Close("server shutting down")
		}
		s.listeners.StopAll(false)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.srvMu.Lock()
		wsSrv, statusSrv := s.wsSrv, s.statusSrv
		s.srvMu.Unlock()
		if wsSrv != nil {
			_ = wsSrv.Shutdown(ctx)
		}
		if statusSrv != nil {
			_ = statusSrv.Shutdown(ctx)
		}
		if s.baseCancel != nil {
			s.baseCancel()
		}
		s.wg.Wait()
		s.log.Info("bye")
	})
}
