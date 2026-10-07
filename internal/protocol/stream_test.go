package protocol

import (
	"bytes"
	"testing"
)

func TestAckResultLabels(t *testing.T) {
	for status, want := range map[byte]string{
		AckOK:             "ok",
		AckPortNotAllowed: "not_allowed",
		AckDialFailed:     "dial_failed",
		AckRejected:       "rejected",
		0x7f:              "rejected",
	} {
		if got := AckResult(status); got != want {
			t.Errorf("AckResult(%#x) = %q, want %q", status, got, want)
		}
	}
}

func TestDurationJSON(t *testing.T) {
	var c NodeConfig
	if err := unmarshal(`{"heartbeat":"15s","dial_timeout":"10s"}`, &c); err != nil {
		t.Fatal(err)
	}
	if c.Heartbeat.D().String() != "15s" || c.DialTimeout.D().String() != "10s" {
		t.Fatalf("durations not parsed: %+v", c)
	}
	b, err := marshal(&c)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"heartbeat":"15s"`)) {
		t.Fatalf("durations must marshal as strings: %s", b)
	}
}
