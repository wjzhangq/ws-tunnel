package server

import (
	"strings"
	"testing"
)

// TestMetricSetKeepsTypesApart is the regression test for the counter/gauge gap:
// a family declared as one type must not be emitted as the other.
func TestMetricSetKeepsTypesApart(t *testing.T) {
	m := newMetricSet()
	m.declare("tunnel_thing_total", "counter", "a counter")
	m.declare("tunnel_thing_now", "gauge", "a gauge")

	m.counter("tunnel_thing_total", map[string]string{"node": "n1"}, 7)
	m.gauge("tunnel_thing_now", nil, 1.5)

	// Wrong type for a declared family, and a family never declared at all.
	m.gauge("tunnel_thing_total", nil, 1)
	m.counter("tunnel_undeclared_total", nil, 1)

	out := m.String()
	for _, want := range []string{
		"# TYPE tunnel_thing_total counter\n",
		"tunnel_thing_total{node=\"n1\"} 7\n",
		"# TYPE tunnel_thing_now gauge\n",
		"tunnel_thing_now 1.5\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output is missing %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "tunnel_thing_total 1\n") {
		t.Errorf("a counter family was emitted as a gauge\n%s", out)
	}
	if n := strings.Count(out, "# ERROR"); n != 2 {
		t.Errorf("# ERROR lines = %d, want 2 (wrong type + undeclared)\n%s", n, out)
	}
}

// TestCounterKeepsLargeIntegersExact guards the reason counters are formatted
// from int64: a byte counter past 2^53 loses precision through float64.
func TestCounterKeepsLargeIntegersExact(t *testing.T) {
	const big = int64(1) << 60 // 1152921504606846976
	m := newMetricSet()
	m.declare("tunnel_bytes_total", "counter", "bytes")
	m.counter("tunnel_bytes_total", map[string]string{"dir": "in"}, big+1)

	out := m.String()
	if !strings.Contains(out, "1152921504606846977") {
		t.Errorf("large counter was rounded\n%s", out)
	}
}

func TestRenderLabelsIsSortedAndEscaped(t *testing.T) {
	got := renderLabels(map[string]string{"z": "1", "a": `he said "hi"`})
	want := `{a="he said \"hi\"",z="1"}`
	if got != want {
		t.Errorf("renderLabels() = %s, want %s", got, want)
	}
	if got := renderLabels(nil); got != "" {
		t.Errorf("renderLabels(nil) = %q, want empty", got)
	}
}

func TestNodeStatsResultRoundTrips(t *testing.T) {
	s := &NodeStats{}
	for _, r := range streamResults {
		s.RecordResult(r)
	}
	for _, r := range streamResults {
		if got := s.Result(r); got != 1 {
			t.Errorf("Result(%q) = %d, want 1", r, got)
		}
	}
	// Anything unrecognised must land in — and read back from — "rejected",
	// so the two halves cannot drift apart.
	s.RecordResult("something-new")
	if got := s.Result("something-new"); got != 2 {
		t.Errorf("unknown result = %d, want it counted as rejected (2)", got)
	}
}

func TestNodeStatsBindErrorsArePerPort(t *testing.T) {
	s := &NodeStats{}
	if got := s.BindErrors(19080); got != 0 {
		t.Fatalf("fresh stats: BindErrors = %d, want 0", got)
	}
	s.RecordBindError(19080)
	s.RecordBindError(19080)
	s.RecordBindError(15432)
	if got := s.BindErrors(19080); got != 2 {
		t.Errorf("BindErrors(19080) = %d, want 2", got)
	}
	if got := s.BindErrors(15432); got != 1 {
		t.Errorf("BindErrors(15432) = %d, want 1", got)
	}
}
