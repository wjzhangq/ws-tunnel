package server

import (
	"net"
	"net/http"
	"testing"
	"time"

	"ws-tunnel/internal/config"
	"ws-tunnel/internal/protocol"
)

// TestRebindListenMovesTheWSEntryPoint covers the §9 gap: a changed `listen`
// used to need a process restart.
func TestRebindListenMovesTheWSEntryPoint(t *testing.T) {
	statusAddr := freeAddr(t)
	srv, _ := newTestServer(t, statusAddr)
	oldAddr := srv.Config().Listen

	// The WS path answers on the old address before the move. A plain GET is
	// refused by the websocket handshake, which is fine: a response at all
	// proves something is listening there.
	waitReachable(t, oldAddr)

	newAddr := freeAddr(t)
	next := withListen(t, srv.Config(), newAddr, statusAddr)
	srv.applyReload(next)

	waitReachable(t, newAddr)
	waitUnreachable(t, oldAddr)
	if got := srv.Config().Listen; got != newAddr {
		t.Errorf("published listen = %q, want %q", got, newAddr)
	}
}

// TestRebindListenKeepsOldAddressWhenNewOneIsTaken is the failure path: the
// server must stay reachable rather than end up deaf.
func TestRebindListenKeepsOldAddressWhenNewOneIsTaken(t *testing.T) {
	srv, _ := newTestServer(t, "")
	oldAddr := srv.Config().Listen
	waitReachable(t, oldAddr)

	// Hold the target address so the rebind cannot succeed.
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer blocker.Close()

	next := withListen(t, srv.Config(), blocker.Addr().String(), "")
	srv.applyReload(next)

	// Still serving the old address.
	waitReachable(t, oldAddr)
	srv.srvMu.Lock()
	serving := srv.wsSrv.Addr
	srv.srvMu.Unlock()
	if serving != oldAddr {
		t.Errorf("serving %q, want the original %q after a failed rebind", serving, oldAddr)
	}
}

// TestRebindStatusListenStartsStopsAndMoves covers all three transitions of an
// endpoint that can also be absent entirely.
func TestRebindStatusListenStartsStopsAndMoves(t *testing.T) {
	srv, _ := newTestServer(t, "") // starts with no status endpoint
	if srv.statusSrv != nil {
		t.Fatal("status endpoint is up despite an empty status_listen")
	}

	// off → on
	first := freeAddr(t)
	srv.applyReload(withListen(t, srv.Config(), srv.Config().Listen, first))
	getWithRetry(t, "http://"+first+"/status")

	// on → moved
	second := freeAddr(t)
	srv.applyReload(withListen(t, srv.Config(), srv.Config().Listen, second))
	getWithRetry(t, "http://"+second+"/status")
	waitUnreachable(t, first)

	// on → off
	srv.applyReload(withListen(t, srv.Config(), srv.Config().Listen, ""))
	waitUnreachable(t, second)
	srv.srvMu.Lock()
	got := srv.statusSrv
	srv.srvMu.Unlock()
	if got != nil {
		t.Error("status endpoint still tracked after status_listen was cleared")
	}
}

// TestReloadWithOnlyListenChangedIsNotSwallowed guards the early-return path:
// an address-only change produces an empty node/port diff, and used to fall
// through to "nothing changed".
func TestReloadWithOnlyListenChangedIsNotSwallowed(t *testing.T) {
	srv, _ := newTestServer(t, "")
	oldAddr := srv.Config().Listen
	waitReachable(t, oldAddr)

	newAddr := freeAddr(t)
	next := withListen(t, srv.Config(), newAddr, "")
	if diff := config.DiffConfig(srv.Config(), next); !diff.Empty() {
		t.Fatalf("test premise broken: diff = %s, want empty", diff)
	}
	srv.applyReload(next)
	waitReachable(t, newAddr)
}

// withListen clones a config with different endpoint addresses, leaving nodes
// and ports untouched.
func withListen(t *testing.T, base *config.Config, listen, statusListen string) *config.Config {
	t.Helper()
	next := *base
	next.Listen = listen
	next.StatusListen = statusListen
	return &next
}

func waitReachable(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + addr + protocol.WSPath)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never became reachable", addr)
}

func waitUnreachable(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return
		}
		_ = c.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s is still accepting connections", addr)
}
