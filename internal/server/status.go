package server

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The /status document (§13.1). Runtime state lives here only — it is never
// written back into config.yaml.
type statusDoc struct {
	Nodes        []nodeStatus `json:"nodes"`
	OfflineNodes []nodeStatus `json:"offline_nodes"`
}

type nodeStatus struct {
	Node           string          `json:"node"`
	Up             bool            `json:"up"`
	ConnectedAt    *time.Time      `json:"connected_at"`
	DisconnectedAt *time.Time      `json:"disconnected_at"`
	Control        *controlStatus  `json:"control,omitempty"`
	Channels       *channelStatus  `json:"channels,omitempty"`
	Streams        *streamStatus   `json:"streams,omitempty"`
	Capacity       *capacityStatus `json:"capacity,omitempty"`
	Traffic        *trafficStatus  `json:"traffic,omitempty"`
	Ports          []PortStatus    `json:"ports"`
	LastError      *string         `json:"last_error"`
}

type controlStatus struct {
	Connected bool      `json:"connected"`
	LastSeen  time.Time `json:"last_seen"`
	RTTms     int64     `json:"rtt_ms"`
}

type channelStatus struct {
	Configured int `json:"configured"`
	Online     int `json:"online"`
}

type streamStatus struct {
	Active      int64 `json:"active"`
	OpenedTotal int64 `json:"opened_total"`
	QueueDepth  int64 `json:"queue_depth"`
}

type capacityStatus struct {
	MaxStreamsPerConn    int   `json:"max_streams_per_conn"`
	MaxConcurrent        int   `json:"max_concurrent"`
	PeakDemand           int64 `json:"peak_demand"`
	ChannelsNeededAtPeak int   `json:"channels_needed_at_peak"`
	SaturatedTotal       int64 `json:"saturated_total"`
}

type trafficStatus struct {
	BytesIn    int64 `json:"bytes_in"`
	BytesOut   int64 `json:"bytes_out"`
	RateInBps  int64 `json:"rate_in_bps"`
	RateOutBps int64 `json:"rate_out_bps"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	filter := r.URL.Query().Get("node")
	doc := s.buildStatus(filter)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(doc)
}

func (s *Server) buildStatus(filter string) statusDoc {
	cfg := s.Config()
	doc := statusDoc{Nodes: []nodeStatus{}, OfflineNodes: []nodeStatus{}}

	names := append([]string{}, cfg.NodeOrder...)
	sort.Strings(names)
	for _, name := range names {
		if filter != "" && filter != name {
			continue
		}
		stats := s.registry.Stats(name)
		connectedAt, disconnectedAt, lastErr := stats.Times()
		ns := nodeStatus{
			Node:  name,
			Ports: s.listeners.Snapshot(name, cfg),
		}
		if !connectedAt.IsZero() {
			t := connectedAt
			ns.ConnectedAt = &t
		}
		if !disconnectedAt.IsZero() {
			t := disconnectedAt
			ns.DisconnectedAt = &t
		}
		if lastErr != "" {
			e := lastErr
			ns.LastError = &e
		}

		rateIn, rateOut := stats.Rates()
		ns.Traffic = &trafficStatus{
			BytesIn:    stats.BytesIn.Load(),
			BytesOut:   stats.BytesOut.Load(),
			RateInBps:  rateIn,
			RateOutBps: rateOut,
		}

		sess := s.registry.Get(name)
		if sess == nil {
			ns.Up = false
			doc.OfflineNodes = append(doc.OfflineNodes, ns)
			continue
		}

		nodeCfg := sess.Config()
		maxStreams := nodeCfg.MaxStreamsPerConn
		peak := stats.Peak.Load()
		needed := 0
		if maxStreams > 0 {
			needed = int(math.Ceil(float64(peak) / float64(maxStreams)))
		}
		ns.Up = true
		ns.Control = &controlStatus{
			Connected: true,
			LastSeen:  sess.LastSeen().UTC(),
			RTTms:     sess.RTT().Milliseconds(),
		}
		ns.Channels = &channelStatus{Configured: nodeCfg.Channels, Online: sess.OnlineChannels()}
		ns.Streams = &streamStatus{
			Active:      sess.ActiveStreams(),
			OpenedTotal: stats.Opened.Load(),
			QueueDepth:  sess.QueueDepth(),
		}
		ns.Capacity = &capacityStatus{
			MaxStreamsPerConn:    maxStreams,
			MaxConcurrent:        nodeCfg.Channels * maxStreams,
			PeakDemand:           peak,
			ChannelsNeededAtPeak: needed,
			SaturatedTotal:       stats.Saturated.Load(),
		}
		doc.Nodes = append(doc.Nodes, ns)
	}
	return doc
}

// metricSet renders the Prometheus text exposition format directly. The metric
// set is the one listed in §13.2; writing it by hand keeps the binary free of
// the client_golang dependency tree.
//
// Counters and gauges are kept apart deliberately. Every family is declared
// once with its type, and each sample states the type it is being emitted as;
// a mismatch is a programming error and shows up in the output instead of
// silently mislabelling a series. Counters are also formatted from int64
// directly, because a byte counter past 2^53 loses precision once it is routed
// through float64.
type metricSet struct {
	b     strings.Builder
	types map[string]string
}

func newMetricSet() *metricSet {
	return &metricSet{types: map[string]string{}}
}

func (m *metricSet) declare(name, typ, doc string) {
	m.types[name] = typ
	fmt.Fprintf(&m.b, "# HELP %s %s\n# TYPE %s %s\n", name, doc, name, typ)
}

func (m *metricSet) gauge(name string, labels map[string]string, v float64) {
	m.emit("gauge", name, labels, formatFloat(v))
}

func (m *metricSet) gaugeInt(name string, labels map[string]string, v int64) {
	m.emit("gauge", name, labels, strconv.FormatInt(v, 10))
}

// counter emits a monotonic sample. Everything counted here is integral.
func (m *metricSet) counter(name string, labels map[string]string, v int64) {
	m.emit("counter", name, labels, strconv.FormatInt(v, 10))
}

func (m *metricSet) emit(typ, name string, labels map[string]string, v string) {
	if got, ok := m.types[name]; !ok || got != typ {
		fmt.Fprintf(&m.b, "# ERROR %s is declared %q but was emitted as %q\n", name, got, typ)
		return
	}
	fmt.Fprintf(&m.b, "%s%s %s\n", name, renderLabels(labels), v)
}

func (m *metricSet) String() string { return m.b.String() }

// streamResults is the fixed label set of tunnel_stream_open_total. Iterating a
// slice rather than a map keeps the exposition byte-stable across scrapes.
var streamResults = []string{"ok", "timeout", "not_allowed", "dial_failed", "rejected"}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	cfg := s.Config()
	m := newMetricSet()

	m.declare("tunnel_node_up", "gauge", "1 when the node has a live control channel")
	m.declare("tunnel_channels", "gauge", "Configured vs online data channels")
	m.declare("tunnel_streams_active", "gauge", "Streams currently forwarding")
	m.declare("tunnel_streams_peak", "gauge", "Peak concurrent stream demand since start")
	m.declare("tunnel_channels_needed_peak", "gauge", "ceil(peak_demand / max_streams_per_conn)")
	m.declare("tunnel_stream_open_total", "counter", "Stream open attempts by result")
	m.declare("tunnel_bytes_total", "counter", "Bytes forwarded by direction")
	m.declare("tunnel_node_saturated_total", "counter", "Times every channel was full past queue_timeout")
	m.declare("tunnel_local_dial_errors_total", "counter", "Client-side dial failures to the local service")
	m.declare("tunnel_port_listening", "gauge", "1 when the reverse port is bound")
	m.declare("tunnel_port_bind_errors_total", "counter", "Failed binds of the reverse port since start")

	names := append([]string{}, cfg.NodeOrder...)
	sort.Strings(names)
	for _, name := range names {
		stats := s.registry.Stats(name)
		sess := s.registry.Get(name)
		l := map[string]string{"node": name}

		up := 0.0
		configured, online, maxStreams := cfg.Nodes[name].Channels, 0, cfg.Settings.MaxStreamsPerConn
		var active int64
		if sess != nil {
			up = 1
			online = sess.OnlineChannels()
			active = sess.ActiveStreams()
			configured = sess.Config().Channels
			maxStreams = sess.Config().MaxStreamsPerConn
		}
		m.gauge("tunnel_node_up", l, up)
		m.gaugeInt("tunnel_channels", map[string]string{"node": name, "state": "configured"}, int64(configured))
		m.gaugeInt("tunnel_channels", map[string]string{"node": name, "state": "online"}, int64(online))
		m.gaugeInt("tunnel_streams_active", l, active)

		peak := stats.Peak.Load()
		m.gaugeInt("tunnel_streams_peak", l, peak)
		needed := 0.0
		if maxStreams > 0 {
			needed = math.Ceil(float64(peak) / float64(maxStreams))
		}
		m.gauge("tunnel_channels_needed_peak", l, needed)

		for _, result := range streamResults {
			m.counter("tunnel_stream_open_total",
				map[string]string{"node": name, "result": result}, stats.Result(result))
		}

		m.counter("tunnel_bytes_total", map[string]string{"node": name, "dir": "in"}, stats.BytesIn.Load())
		m.counter("tunnel_bytes_total", map[string]string{"node": name, "dir": "out"}, stats.BytesOut.Load())
		m.counter("tunnel_node_saturated_total", l, stats.Saturated.Load())
		m.counter("tunnel_local_dial_errors_total", l, stats.LocalDialErrors.Load())

		for _, ps := range s.listeners.Snapshot(name, cfg) {
			portLabels := map[string]string{"node": name, "port": strconv.Itoa(ps.Port)}
			v := 0.0
			if ps.Listening {
				v = 1
			}
			m.gauge("tunnel_port_listening", portLabels, v)
			// Bind failures live in NodeStats, so the count survives the
			// listener being torn down and rebuilt on every reconnect.
			m.counter("tunnel_port_bind_errors_total", portLabels, ps.BindErrors)
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(m.String()))
}

func renderLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		// escapeLabel already produces the escaped form; %q would escape it a
		// second time and emit \\\" for a single quote.
		parts = append(parts, k+`="`+escapeLabel(labels[k])+`"`)
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// escapeLabel escapes the three characters the exposition format reserves in a
// label value, leaving everything else (UTF-8 included) as-is.
func escapeLabel(v string) string {
	r := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"")
	return r.Replace(v)
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}
