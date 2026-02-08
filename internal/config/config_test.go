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

func TestLoad_TempDir(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "staging")
	content := `
opensearch:
  url: "http://os:9200"
quickwit:
  url: "http://qw:7280"
migration:
  temp_dir: "` + tmpDir + `"
`
	path := writeTempFile(t, content)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Migration.TempDir != tmpDir {
		t.Errorf("Migration.TempDir = %q, want %q", cfg.Migration.TempDir, tmpDir)
	}
	// Validation should have created the directory.
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Errorf("temp_dir %q was not created during validation", tmpDir)
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

func TestColdDaysForIndex(t *testing.T) {
	cfg := &Config{
		Retention: RetentionConfig{
			ColdDays: 365,
			IndexColdDays: map[string]int{
				"security-audit":  1095,
				"compliance-*":    2555,
			},
		},
	}

	tests := []struct {
		index string
		want  int
	}{
		{"security-audit", 1095},       // exact match
		{"compliance-2026", 2555},      // glob match
		{"compliance-pci", 2555},       // glob match
		{"logs-2026.01.01", 365},       // fallback to global
		{"other", 365},                 // fallback to global
	}

	for _, tt := range tests {
		if got := cfg.ColdDaysForIndex(tt.index); got != tt.want {
			t.Errorf("ColdDaysForIndex(%q) = %d, want %d", tt.index, got, tt.want)
		}
	}
}

func TestColdDaysForIndex_ZeroGlobal(t *testing.T) {
	cfg := &Config{
		Retention: RetentionConfig{
			ColdDays:      0, // forever
			IndexColdDays: map[string]int{
				"logs-*": 730,
			},
		},
	}

	if got := cfg.ColdDaysForIndex("logs-2026"); got != 730 {
		t.Errorf("ColdDaysForIndex(logs-2026) = %d, want 730", got)
	}
	if got := cfg.ColdDaysForIndex("events"); got != 0 {
		t.Errorf("ColdDaysForIndex(events) = %d, want 0 (forever)", got)
	}
}

func TestLoad_IndexColdDays(t *testing.T) {
	content := `
opensearch:
  url: "http://os:9200"
quickwit:
  url: "http://qw:7280"
retention:
  cold_days: 365
  index_cold_days:
    security-audit-*: 1095
    compliance-*: 2555
`
	path := writeTempFile(t, content)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.ColdDaysForIndex("security-audit-2026"); got != 1095 {
		t.Errorf("ColdDaysForIndex(security-audit-2026) = %d, want 1095", got)
	}
	if got := cfg.ColdDaysForIndex("compliance-pci"); got != 2555 {
		t.Errorf("ColdDaysForIndex(compliance-pci) = %d, want 2555", got)
	}
	if got := cfg.ColdDaysForIndex("logs-2026"); got != 365 {
		t.Errorf("ColdDaysForIndex(logs-2026) = %d, want 365", got)
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
