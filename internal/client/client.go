// Package client implements tunnel-client: it knows nothing but `url + key`
// and receives everything else — port allow-list, channel count, timeouts —
// from the server at handshake time and on every reload (§3.4, §4).
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"ws-tunnel/internal/mux"
	"ws-tunnel/internal/protocol"
	"ws-tunnel/internal/wsutil"
)

const (
	// ShutdownGrace mirrors the server's 30s ceiling (§14).
	ShutdownGrace = 30 * time.Second

	dialTimeout    = 15 * time.Second
	minBackoff     = time.Second
	maxBackoff     = 30 * time.Second
	stableSession  = 60 * time.Second // a session this long resets the backoff
	defaultRetryHB = 15 * time.Second
)

// Client is one tunnel-client process.
type Client struct {
	URL  string
	Key  string
	Node string // optional; when empty the server resolves the node by key
	Log  *slog.Logger

	cfgMu   sync.RWMutex
	cfg     *protocol.NodeConfig
	session string

	muxMu sync.Mutex
	mux   *mux.Conn

	draining atomic.Bool

	// lastErr is the most recent session failure, kept so a local /status can
	// answer "why am I not connected" without the operator reading the log.
	lastErr atomic.Pointer[string]

	activeStreams   atomic.Int64
	channelsOnline  atomic.Int64
	bytesIn         atomic.Int64
	bytesOut        atomic.Int64
	localDialErrors atomic.Int64
}

// Run keeps a session alive, reconnecting with exponential backoff
// (1s → 30s). Every reconnect is a brand new session: new session id, all
// data channels rebuilt, nothing carried over (§11).
func (c *Client) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		if ctx.Err() != nil {
			return nil
		}
		started := time.Now()
		err := c.runSession(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			c.setLastError(err.Error())
			c.Log.Warn("session ended", "err", err)
		} else {
			c.Log.Info("session ended")
		}
		if time.Since(started) > stableSession {
			backoff = minBackoff
		}
		c.Log.Info("reconnecting", "in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// runSession owns one control channel from handshake to teardown.
func (c *Client) runSession(ctx context.Context) error {
	dctx, dcancel := context.WithTimeout(ctx, dialTimeout)
	conn, _, err := websocket.Dial(dctx, c.URL, &websocket.DialOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	dcancel()
	if err != nil {
		return err
	}
	wsutil.Prepare(conn)
	defer conn.CloseNow()

	hctx, hcancel := context.WithTimeout(ctx, dialTimeout)
	err = wsutil.WriteJSON(hctx, conn, &protocol.Message{
		Type: protocol.TypeHello, Proto: protocol.Version, Role: protocol.RoleControl, Node: c.Node, Key: c.Key,
	})
	if err == nil {
		var msg *protocol.Message
		msg, err = wsutil.ReadJSON(hctx, conn)
		if err == nil {
			switch msg.Type {
			case protocol.TypeWelcome:
				if msg.Proto != protocol.Version {
					err = fmt.Errorf("server speaks protocol %d, client needs %d; upgrade the server",
						msg.Proto, protocol.Version)
					break
				}
				if msg.Config == nil {
					err = errors.New("welcome carried no config")
					break
				}
				c.setConfig(msg.Config)
				c.setSession(msg.Session)
			case protocol.TypeError:
				err = errors.New("server refused the handshake: " + msg.Code + " " + msg.Msg)
			default:
				err = errors.New("unexpected reply to hello: " + msg.Type)
			}
		}
	}
	hcancel()
	if err != nil {
		return err
	}

	cfg := c.Config()
	c.draining.Store(false)

	sess := mux.New(ctx, mux.WS{C: conn}, mux.Client, mux.WithWindow(cfg.StreamWindow))
	c.setMux(sess)
	defer func() {
		c.setMux(nil)
		sess.Close()
	}()

	c.channelsOnline.Store(1)
	defer c.channelsOnline.Store(0)

	c.Log.Info("session up",
		"node", c.Node, "session", c.Session(),
		"ports", len(cfg.Ports), "heartbeat", cfg.Heartbeat.D())

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go c.statsLoop(sctx)
	go c.acceptLoop(sctx, sess)

	return c.controlLoop(sctx, sess)
}

func (c *Client) controlLoop(ctx context.Context, sess *mux.Conn) error {
	for {
		hb := c.Config().Heartbeat.D()
		if hb <= 0 {
			hb = defaultRetryHB
		}
		timer := time.NewTimer(3 * hb)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-sess.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return nil
			}
			return errors.New("websocket closed")
		case <-timer.C:
			if time.Since(sess.LastRead()) > 3*hb {
				return errors.New("server heartbeat timeout")
			}
		case msg := <-sess.Controls():
			timer.Stop()
			switch msg.Type {
			case protocol.TypePing:
				_ = c.sendControl(&protocol.Message{Type: protocol.TypePong, Nonce: msg.Nonce, TS: msg.TS})
			case protocol.TypePong:
			case protocol.TypeReloadConfig:
				if msg.Config == nil {
					continue
				}
				c.setConfig(msg.Config)
				c.Log.Info("config reloaded in place",
					"ports", len(msg.Config.Ports))
			case protocol.TypeDrain:
				c.draining.Store(true)
				c.Log.Info("draining on server request", "deadline", msg.Deadline)
			case protocol.TypeBye:
				c.Log.Info("server said bye", "reason", msg.Reason)
				return nil
			case protocol.TypeError:
				c.Log.Warn("server error", "code", msg.Code, "msg", msg.Msg)
				if msg.Code == protocol.ErrAuthFailed {
					return errors.New(msg.Code + ": " + msg.Msg)
				}
			}
		}
	}
}

func (c *Client) statsLoop(ctx context.Context) {
	hb := c.Config().Heartbeat.D()
	if hb <= 0 {
		hb = defaultRetryHB
	}
	t := time.NewTicker(hb)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if cur := c.Config().Heartbeat.D(); cur > 0 && cur != hb {
			hb = cur
			t.Reset(hb)
		}
		_ = c.sendControl(&protocol.Message{
			Type: protocol.TypeStats,
			Stats: &protocol.Stats{
				Channels:        int(c.channelsOnline.Load()),
				Streams:         int(c.activeStreams.Load()),
				BytesIn:         c.bytesIn.Load(),
				BytesOut:        c.bytesOut.Load(),
				LocalDialErrors: c.localDialErrors.Load(),
			},
		})
	}
}

// Shutdown implements the client half of §14: stop taking new streams, let
// the in-flight ones finish (30s ceiling), say bye.
func (c *Client) Shutdown() {
	c.draining.Store(true)
	deadline := time.Now().Add(ShutdownGrace)
	c.Log.Info("draining before exit", "active_streams", c.activeStreams.Load())
	for c.activeStreams.Load() > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	_ = c.sendControl(&protocol.Message{Type: protocol.TypeBye, Reason: "client shutting down"})
}

// ─── shared state ────────────────────────────────────────────────────────

func (c *Client) Config() *protocol.NodeConfig {
	c.cfgMu.RLock()
	defer c.cfgMu.RUnlock()
	return c.cfg
}

func (c *Client) setConfig(cfg *protocol.NodeConfig) {
	c.cfgMu.Lock()
	c.cfg = cfg
	c.cfgMu.Unlock()
}

func (c *Client) setSession(id string) {
	c.cfgMu.Lock()
	c.session = id
	c.cfgMu.Unlock()
}

func (c *Client) Session() string {
	c.cfgMu.RLock()
	defer c.cfgMu.RUnlock()
	return c.session
}

func (c *Client) setLastError(msg string) {
	c.lastErr.Store(&msg)
}

func (c *Client) lastError() string {
	if p := c.lastErr.Load(); p != nil {
		return *p
	}
	return ""
}

func (c *Client) setMux(m *mux.Conn) {
	c.muxMu.Lock()
	c.mux = m
	c.muxMu.Unlock()
}

func (c *Client) sendControl(msg *protocol.Message) error {
	c.muxMu.Lock()
	sess := c.mux
	c.muxMu.Unlock()
	if sess == nil {
		return errors.New("no session")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return sess.SendControl(ctx, msg)
}

// remoteFor resolves a port id against the pushed allow-list (§10).
func (c *Client) remoteFor(port int) string {
	cfg := c.Config()
	if cfg == nil {
		return ""
	}
	return cfg.Ports[strconv.Itoa(port)]
}

func (c *Client) acceptLoop(ctx context.Context, sess *mux.Conn) {
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		st, err := sess.Accept(ctx)
		if err != nil {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.handleStream(ctx, st)
		}()
	}
}

func (c *Client) handleStream(ctx context.Context, st *mux.Stream) {
	defer st.Close()
	c.activeStreams.Add(1)
	defer c.activeStreams.Add(-1)

	port := st.Port()
	if c.draining.Load() {
		_ = st.Ack(protocol.AckRejected)
		return
	}

	remote := c.remoteFor(port)
	if remote == "" {
		c.Log.Warn("port id not in the allow-list", "port", port)
		_ = st.Ack(protocol.AckPortNotAllowed)
		return
	}

	timeout := c.Config().DialTimeout.D()
	if timeout <= 0 {
		timeout = dialTimeout
	}
	d := net.Dialer{Timeout: timeout}
	local, err := d.DialContext(ctx, "tcp", remote)
	if err != nil {
		c.localDialErrors.Add(1)
		c.Log.Warn("dialing the local service failed", "port", port, "remote", remote, "err", err)
		_ = st.Ack(protocol.AckDialFailed)
		return
	}
	defer local.Close()

	if err := st.Ack(protocol.AckOK); err != nil {
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		n, _ := io.Copy(local, st)
		c.bytesIn.Add(n)
		if tc, ok := local.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		} else {
			_ = local.Close()
		}
	}()
	n, _ := io.Copy(st, local)
	c.bytesOut.Add(n)
	_ = st.CloseWrite()
	wg.Wait()
}
