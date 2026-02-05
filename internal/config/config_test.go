package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_ValidConfig(t *testing.T) {
	content := `
server:
  listen: ":8080"
opensearch:
  url: "http://os:9200"
  username: "admin"
  password: "secret"
quickwit:
  url: "http://qw:7280"
retention:
  days: 14
  timestamp_field: "event_time"
  index_fields:
    logs: "@timestamp"
    events: "created_at"
migration:
  enabled: true
  schedule: "0 3 * * *"
  batch_size: 500
  delete_after_migration: true
  indices:
    - "logs-*"
logging:
  level: "debug"
`
	path := writeTempFile(t, content)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Listen != ":8080" {
		t.Errorf("Server.Listen = %q, want %q", cfg.Server.Listen, ":8080")
	}
	if cfg.OpenSearch.URL != "http://os:9200" {
		t.Errorf("OpenSearch.URL = %q", cfg.OpenSearch.URL)
	}
	if cfg.OpenSearch.Username != "admin" {
		t.Errorf("OpenSearch.Username = %q", cfg.OpenSearch.Username)
	}
	if cfg.Quickwit.URL != "http://qw:7280" {
		t.Errorf("Quickwit.URL = %q", cfg.Quickwit.URL)
	}
	if cfg.Retention.Days != 14 {
		t.Errorf("Retention.Days = %d, want 14", cfg.Retention.Days)
	}
	if cfg.Retention.TimestampField != "event_time" {
		t.Errorf("Retention.TimestampField = %q", cfg.Retention.TimestampField)
	}
	if cfg.Retention.IndexFields["logs"] != "@timestamp" {
		t.Errorf("IndexFields[logs] = %q", cfg.Retention.IndexFields["logs"])
	}
	if cfg.Migration.BatchSize != 500 {
		t.Errorf("Migration.BatchSize = %d", cfg.Migration.BatchSize)
	}
	if !cfg.Migration.DeleteAfterMigration {
		t.Error("Migration.DeleteAfterMigration = false, want true")
	}
}

func TestLoad_Defaults(t *testing.T) {
	content := `
opensearch:
  url: "http://os:9200"
quickwit:
  url: "http://qw:7280"
`
	path := writeTempFile(t, content)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Server.Listen != ":9200" {
		t.Errorf("default Server.Listen = %q, want %q", cfg.Server.Listen, ":9200")
	}
	if cfg.Retention.Days != 30 {
		t.Errorf("default Retention.Days = %d, want 30", cfg.Retention.Days)
	}
	if cfg.Retention.TimestampField != "@timestamp" {
		t.Errorf("default TimestampField = %q", cfg.Retention.TimestampField)
	}
	if cfg.Logging.Level != "info" {
		t.Errorf("default Logging.Level = %q", cfg.Logging.Level)
	}
}

func TestLoad_MissingOpenSearchURL(t *testing.T) {
	content := `
quickwit:
  url: "http://qw:7280"
`
	path := writeTempFile(t, content)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing opensearch.url")
	}
}

func TestLoad_MissingQuickwitURL(t *testing.T) {
	content := `
opensearch:
  url: "http://os:9200"
`
	path := writeTempFile(t, content)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for missing quickwit.url")
	}
}

func TestTimestampFieldForIndex(t *testing.T) {
	cfg := &Config{
		Retention: RetentionConfig{
			TimestampField: "@timestamp",
			IndexFields: map[string]string{
				"events": "created_at",
			},
		},
	}

	if got := cfg.TimestampFieldForIndex("events"); got != "created_at" {
		t.Errorf("TimestampFieldForIndex(events) = %q, want %q", got, "created_at")
	}
	if got := cfg.TimestampFieldForIndex("logs"); got != "@timestamp" {
		t.Errorf("TimestampFieldForIndex(logs) = %q, want %q", got, "@timestamp")
	}
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
