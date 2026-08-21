package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// The client's own telemetry endpoint. §3.4 keeps the client's own
// configuration down to `url + key`, and §13 only specifies /status on the
// server — so this is strictly opt-in: without StatusListen the client opens no
// port at all and the only telemetry path stays the `stats` control message.
//
// It exists because the reported-to-server path is useless in exactly the case
// you most want data: when the client cannot reach the server. Then `stats` goes
// nowhere and there is nothing local to look at.
//
// The endpoint is unauthenticated, like the server's, so it belongs on loopback.

// ClientStatus is the document served at /status.
type ClientStatus struct {
	Node      string       `json:"node"`
	URL       string       `json:"url"`
	Session   string       `json:"session"`
	Connected bool         `json:"connected"`
	Draining  bool         `json:"draining"`
	Channels  chanStatus   `json:"channels"`
	Streams   streamStatus `json:"streams"`
	Traffic   trafficView  `json:"traffic"`
	Ports     []portView   `json:"ports"`
	LastError *string      `json:"last_error"`
}

type chanStatus struct {
	Configured int `json:"configured"`
	Online     int `json:"online"`
}

type streamStatus struct {
	Active int64 `json:"active"`
}

type trafficView struct {
	BytesIn         int64 `json:"bytes_in"`
	BytesOut        int64 `json:"bytes_out"`
	LocalDialErrors int64 `json:"local_dial_errors"`
}

// portView is one entry of the server-pushed allow-list (§10), which is the
// client's whole view of what it is allowed to forward to.
type portView struct {
	Port   string `json:"port"`
	Remote string `json:"remote"`
}

// Status assembles the current view. Safe to call at any time, including before
// the first handshake.
func (c *Client) Status() ClientStatus {
	st := ClientStatus{
		Node:     c.Node,
		URL:      c.URL,
		Session:  c.Session(),
		Draining: c.draining.Load(),
		Streams:  streamStatus{Active: c.activeStreams.Load()},
		Traffic: trafficView{
			BytesIn:         c.bytesIn.Load(),
			BytesOut:        c.bytesOut.Load(),
			LocalDialErrors: c.localDialErrors.Load(),
		},
		Ports: []portView{},
	}

	c.ctrlMu.Lock()
	st.Connected = c.ctrl != nil
	c.ctrlMu.Unlock()

	if msg := c.lastError(); msg != "" {
		st.LastError = &msg
	}

	st.Channels.Online = int(c.channelsOnline.Load())
	if cfg := c.Config(); cfg != nil {
		st.Channels.Configured = cfg.Channels
		for port, remote := range cfg.Ports {
			st.Ports = append(st.Ports, portView{Port: port, Remote: remote})
		}
	}
	sortPorts(st.Ports)
	return st
}

// ServeStatus runs the /status endpoint until ctx is cancelled. A bind failure
// is returned rather than retried: an observability endpoint must not silently
// end up serving nothing.
func (c *Client) ServeStatus(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("status listener: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(c.Status())
	})
	srv := &http.Server{Handler: mux}

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	c.Log.Info("client status endpoint listening", "addr", ln.Addr().String())
	if !isLoopback(ln.Addr()) {
		c.Log.Warn("client status endpoint is unauthenticated and not on loopback; "+
			"anyone who can reach it can read this node's telemetry", "addr", ln.Addr().String())
	}
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func isLoopback(addr net.Addr) bool {
	ta, ok := addr.(*net.TCPAddr)
	return ok && ta.IP != nil && ta.IP.IsLoopback()
}

// sortPorts keeps the JSON byte-stable across polls; Ports is a map upstream.
func sortPorts(p []portView) {
	sort.Slice(p, func(i, j int) bool {
		a, aerr := strconv.Atoi(p[i].Port)
		b, berr := strconv.Atoi(p[j].Port)
		if aerr == nil && berr == nil {
			return a < b
		}
		return p[i].Port < p[j].Port
	})
}
