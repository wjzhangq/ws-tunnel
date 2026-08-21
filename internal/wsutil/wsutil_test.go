package wsutil

import (
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// TestReadLimitExceedsMaxFrameSize pins the invariant from §7 "实现注意": the WS
// read limit must be larger than any smux frame, or the first big frame tears
// the connection down. coder/websocket defaults to 32 KiB, which is exactly the
// frame size we configure — hence the explicit raise.
func TestReadLimitExceedsMaxFrameSize(t *testing.T) {
	cfg := SmuxConfig(0)
	if int64(cfg.MaxFrameSize) >= ReadLimit {
		t.Fatalf("MaxFrameSize %d must stay below ReadLimit %d", cfg.MaxFrameSize, ReadLimit)
	}
	// smux itself refuses a frame size above 65535.
	if cfg.MaxFrameSize > 65535 {
		t.Fatalf("MaxFrameSize = %d, above the smux hard limit", cfg.MaxFrameSize)
	}
}

// TestSmuxConfigKeepalive covers the §11 dead-channel detection: keepalive is
// derived from the heartbeat, and a zero heartbeat must leave smux's defaults
// intact rather than disabling the timer.
func TestSmuxConfigKeepalive(t *testing.T) {
	hb := 15 * time.Second
	cfg := SmuxConfig(hb)
	if cfg.KeepAliveInterval != hb {
		t.Errorf("KeepAliveInterval = %v, want %v", cfg.KeepAliveInterval, hb)
	}
	if cfg.KeepAliveTimeout != 3*hb {
		t.Errorf("KeepAliveTimeout = %v, want 3x the interval", cfg.KeepAliveTimeout)
	}
	if err := smux.VerifyConfig(cfg); err != nil {
		t.Errorf("smux rejected the config: %v", err)
	}

	zero := SmuxConfig(0)
	if zero.KeepAliveInterval <= 0 || zero.KeepAliveTimeout <= zero.KeepAliveInterval {
		t.Errorf("zero heartbeat produced interval=%v timeout=%v, want smux defaults",
			zero.KeepAliveInterval, zero.KeepAliveTimeout)
	}
	if err := smux.VerifyConfig(zero); err != nil {
		t.Errorf("smux rejected the zero-heartbeat config: %v", err)
	}
}
