package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/leonunix/oqbridge/internal/backend"
	"github.com/leonunix/oqbridge/internal/config"
	"github.com/leonunix/oqbridge/internal/migration"
	"github.com/leonunix/oqbridge/internal/util"

	"github.com/robfig/cron/v3"
)

func main() {
	configPath := flag.String("config", "oqbridge.yaml", "path to configuration file")
	once := flag.Bool("once", false, "run migration once and exit (ignore schedule)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load configuration", "error", err)
		os.Exit(1)
	}

	util.SetupLogger(cfg.Logging.Level)

	slog.Info("oqbridge-migrate starting",
		"opensearch", cfg.OpenSearch.URL,
		"quickwit", cfg.Quickwit.URL,
		"retention_days", cfg.Retention.Days,
		"workers", cfg.Migration.Workers,
		"batch_size", cfg.Migration.BatchSize,
		"compress", cfg.Migration.Compress,
		"indices", cfg.Migration.Indices,
	)

	osClient, err := util.NewHTTPClient(cfg.OpenSearch.TLSConfig)
	if err != nil {
		slog.Error("failed to create OpenSearch HTTP client", "error", err)
		os.Exit(1)
	}
	qwClient, err := util.NewHTTPClient(cfg.Quickwit.TLSConfig)
	if err != nil {
		slog.Error("failed to create Quickwit HTTP client", "error", err)
		os.Exit(1)
	}

	hot := backend.NewOpenSearch(cfg.OpenSearch.URL, cfg.OpenSearch.Username, cfg.OpenSearch.Password, osClient)
	cold := backend.NewQuickwit(cfg.Quickwit.URL, cfg.Quickwit.Username, cfg.Quickwit.Password, cfg.Migration.Compress, qwClient)
	if cfg.Migration.TempDir != "" {
		cold.SetTempDir(cfg.Migration.TempDir)
		slog.Info("migration staging via disk", "temp_dir", cfg.Migration.TempDir)
	}

	lock := backend.NewOpenSearchLock(cfg.OpenSearch.URL, cfg.OpenSearch.Username, cfg.OpenSearch.Password, osClient)
	cpStore := migration.NewOpenSearchCheckpointStore(cfg.OpenSearch.URL, cfg.OpenSearch.Username, cfg.OpenSearch.Password, osClient)
	metricsStore := migration.NewOpenSearchMetricsStore(cfg.OpenSearch.URL, cfg.OpenSearch.Username, cfg.OpenSearch.Password, osClient)

	migrator, err := migration.NewMigrator(cfg, hot, cold, cpStore,
		migration.WithDistLock(lock),
		migration.WithMetricsRecorder(metricsStore),
	)
	if err != nil {
		slog.Error("failed to initialize migrator", "error", err)
		os.Exit(1)
	}

	if *once {
		// Run once and exit.
		if err := migrator.MigrateAll(context.Background()); err != nil {
			slog.Error("migration failed", "error", err)
			os.Exit(1)
		}
		slog.Info("migration completed, exiting")
		return
	}

	// Run on a cron schedule.
	c := cron.New()
	_, err = c.AddFunc(cfg.Migration.Schedule, func() {
		slog.Info("scheduled migration starting")
		if err := migrator.MigrateAll(context.Background()); err != nil {
			slog.Error("scheduled migration failed", "error", err)
			return
		}
		slog.Info("scheduled migration completed")
	})
	if err != nil {
		slog.Error("invalid cron schedule", "schedule", cfg.Migration.Schedule, "error", err)
		os.Exit(1)
	}

	c.Start()
	slog.Info("migration scheduler started", "schedule", cfg.Migration.Schedule)

	// Wait for shutdown signal.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	slog.Info("shutting down...")
	ctx := c.Stop()
	<-ctx.Done()
	slog.Info("oqbridge-migrate stopped")
}
