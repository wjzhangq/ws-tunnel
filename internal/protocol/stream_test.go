package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestStreamHeaderRoundTrip(t *testing.T) {
	for _, port := range []int{1, 80, 127, 128, 1980, 19080, 65535} {
		var buf bytes.Buffer
		if err := WriteStreamHeader(&buf, port); err != nil {
			t.Fatal(err)
		}
		payload := []byte("GET / HTTP/1.1\r\n")
		buf.Write(payload)

		got, err := ReadStreamHeader(&buf)
		if err != nil {
			t.Fatalf("port %d: %v", port, err)
		}
		if got != port {
			t.Fatalf("port %d decoded as %d", port, got)
		}
		// The header reader must not swallow any payload byte.
		rest, _ := io.ReadAll(&buf)
		if !bytes.Equal(rest, payload) {
			t.Fatalf("payload corrupted: %q", rest)
		}
	}
}

func TestStreamHeaderRejects(t *testing.T) {
	// One past the current version: still unknown, and it stays unknown when
	// StreamVersion is bumped again.
	if _, err := ReadStreamHeader(bytes.NewReader([]byte{StreamVersion + 1, 0x01})); !errors.Is(err, ErrBadVersion) {
		t.Fatalf("expected ErrBadVersion, got %v", err)
	}
	// Port id 0 is out of the 1..65535 range.
	var buf bytes.Buffer
	buf.WriteByte(StreamVersion)
	buf.WriteByte(0x00)
	if _, err := ReadStreamHeader(&buf); !errors.Is(err, ErrBadPortID) {
		t.Fatalf("expected ErrBadPortID, got %v", err)
	}
}

func TestAckRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteAck(&buf, AckDialFailed); err != nil {
		t.Fatal(err)
	}
	buf.WriteString("payload")
	got, err := ReadAck(&buf)
	if err != nil || got != AckDialFailed {
		t.Fatalf("got %v %v", got, err)
	}
	if AckResult(got) != "dial_failed" {
		t.Fatalf("unexpected result label %q", AckResult(got))
	}
	if rest, _ := io.ReadAll(&buf); string(rest) != "payload" {
		t.Fatalf("payload corrupted: %q", rest)
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
