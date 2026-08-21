// Command tunnel-client connects out to a tunnel-server with nothing but a
// URL and a key; everything else is pushed down by the server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"ws-tunnel/internal/client"
)

var version = "dev"

func main() {
	var (
		url      = flag.String("url", os.Getenv("TUNNEL_URL"), "server WS url, e.g. ws://tunnel.example.com:8443/ws (env TUNNEL_URL)")
		key      = flag.String("key", os.Getenv("TUNNEL_KEY"), "node key (env TUNNEL_KEY)")
		node     = flag.String("node", os.Getenv("TUNNEL_NODE"), "node name; optional, the server can resolve it from the key (env TUNNEL_NODE)")
		statusAt = flag.String("status-listen", os.Getenv("TUNNEL_STATUS_LISTEN"), "serve local /status on this address, e.g. 127.0.0.1:9101; off when empty. Unauthenticated — keep it on loopback (env TUNNEL_STATUS_LISTEN)")
		logLevel = flag.String("log-level", "info", "debug | info | warn | error")
		showVer  = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("tunnel-client", version)
		return
	}

	log := newLogger(*logLevel)

	if strings.TrimSpace(*url) == "" || strings.TrimSpace(*key) == "" {
		log.Error("both --url and --key are required (or TUNNEL_URL / TUNNEL_KEY)")
		flag.Usage()
		os.Exit(2)
	}

	c := &client.Client{URL: *url, Key: *key, Node: *node, Log: log}

	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Info("signal received, shutting down")
		c.Shutdown() // stop new streams, wait for in-flight ones, say bye
		cancel()
	}()

	// Opt-in local telemetry. A bind failure is fatal: an operator who asked
	// for /status should not be left believing it is up.
	if addr := strings.TrimSpace(*statusAt); addr != "" {
		errCh := make(chan error, 1)
		go func() { errCh <- c.ServeStatus(ctx, addr) }()
		select {
		case err := <-errCh:
			if err != nil {
				log.Error("client status endpoint failed", "addr", addr, "err", err)
				os.Exit(1)
			}
		case <-time.After(200 * time.Millisecond):
		}
	}

	if err := c.Run(ctx); err != nil {
		log.Error("client stopped with an error", "err", err)
		os.Exit(1)
	}
}

func newLogger(level string) *slog.Logger {
	var lv slog.Level
	if err := lv.UnmarshalText([]byte(level)); err != nil {
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lv}))
}
