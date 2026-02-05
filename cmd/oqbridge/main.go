package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
	"github.com/leonunix/oqbridge/internal/proxy"
	"github.com/leonunix/oqbridge/internal/util"
)

func main() {
	configPath := flag.String("config", "oqbridge.yaml", "path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	util.SetupLogger(cfg.Logging.Level)

	slog.Info("oqbridge proxy starting",
		"listen", cfg.Server.Listen,
		"opensearch", cfg.OpenSearch.URL,
		"quickwit", cfg.Quickwit.URL,
		"retention_days", cfg.Retention.Days,
	)

	hotBackend := backend.NewOpenSearch(cfg.OpenSearch.URL, cfg.OpenSearch.Username, cfg.OpenSearch.Password)
	coldBackend := backend.NewQuickwit(cfg.Quickwit.URL, cfg.Quickwit.Username, cfg.Quickwit.Password, false)

	p, err := proxy.New(cfg, hotBackend, coldBackend)
	if err != nil {
		slog.Error("failed to initialize proxy", "error", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:    cfg.Server.Listen,
		Handler: p,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("proxy listening", "addr", cfg.Server.Listen)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-stop
	slog.Info("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}

	slog.Info("oqbridge stopped")
}
