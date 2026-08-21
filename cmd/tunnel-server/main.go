// Command tunnel-server terminates client WS connections and exposes one
// reverse TCP port per configured mapping.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"ws-tunnel/internal/server"
)

var version = "dev"

func main() {
	var (
		cfgPath  = flag.String("config", "config.yaml", "path to config.yaml")
		logLevel = flag.String("log-level", "info", "debug | info | warn | error")
		showVer  = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("tunnel-server", version)
		return
	}

	log := newLogger(*logLevel)

	srv, err := server.New(*cfgPath, log)
	if err != nil {
		log.Error("cannot start", "config", *cfgPath, "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil {
		log.Error("server stopped with an error", "err", err)
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
