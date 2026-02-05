package config

import (
	"fmt"
	"net/url"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config holds the complete application configuration.
type Config struct {
	Server    ServerConfig    `koanf:"server"`
	OpenSearch OpenSearchConfig `koanf:"opensearch"`
	Quickwit  QuickwitConfig  `koanf:"quickwit"`
	Retention RetentionConfig `koanf:"retention"`
	Migration MigrationConfig `koanf:"migration"`
	Logging   LoggingConfig   `koanf:"logging"`
}

type ServerConfig struct {
	Listen string `koanf:"listen"`
}

type OpenSearchConfig struct {
	URL      string `koanf:"url"`
	Username string `koanf:"username"`
	Password string `koanf:"password"`
}

type QuickwitConfig struct {
	URL      string `koanf:"url"`
	Username string `koanf:"username"`
	Password string `koanf:"password"`
}

type RetentionConfig struct {
	Days           int               `koanf:"days"`
	TimestampField string            `koanf:"timestamp_field"`
	IndexFields    map[string]string `koanf:"index_fields"`
}

type MigrationConfig struct {
	Enabled              bool     `koanf:"enabled"`
	Schedule             string   `koanf:"schedule"`
	BatchSize            int      `koanf:"batch_size"`
	Workers              int      `koanf:"workers"`              // Number of parallel sliced scroll workers.
	Compress             bool     `koanf:"compress"`             // Gzip compress data sent to Quickwit.
	CheckpointDir        string   `koanf:"checkpoint_dir"`       // Directory to store migration checkpoints.
	DeleteAfterMigration bool     `koanf:"delete_after_migration"`
	Indices              []string `koanf:"indices"`
}

type LoggingConfig struct {
	Level string `koanf:"level"`
}

// Load reads configuration from the given YAML file path.
func Load(path string) (*Config, error) {
	k := koanf.New(".")

	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("loading config from %s: %w", path, err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	setDefaults(&cfg)

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// TimestampFieldForIndex returns the timestamp field name for the given index.
// Falls back to the global default if no per-index override is configured.
func (c *Config) TimestampFieldForIndex(index string) string {
	if field, ok := c.Retention.IndexFields[index]; ok {
		return field
	}
	return c.Retention.TimestampField
}

func setDefaults(cfg *Config) {
	if cfg.Server.Listen == "" {
		cfg.Server.Listen = ":9200"
	}
	if cfg.Retention.Days <= 0 {
		cfg.Retention.Days = 30
	}
	if cfg.Retention.TimestampField == "" {
		cfg.Retention.TimestampField = "@timestamp"
	}
	if cfg.Migration.BatchSize <= 0 {
		cfg.Migration.BatchSize = 5000
	}
	if cfg.Migration.Workers <= 0 {
		cfg.Migration.Workers = 4
	}
	if cfg.Migration.Schedule == "" {
		cfg.Migration.Schedule = "0 2 * * *"
	}
	if cfg.Migration.CheckpointDir == "" {
		cfg.Migration.CheckpointDir = "/var/lib/oqbridge"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
}

func validate(cfg *Config) error {
	if cfg.OpenSearch.URL == "" {
		return fmt.Errorf("opensearch.url is required")
	}
	if _, err := url.Parse(cfg.OpenSearch.URL); err != nil {
		return fmt.Errorf("invalid opensearch.url: %w", err)
	}

	if cfg.Quickwit.URL == "" {
		return fmt.Errorf("quickwit.url is required")
	}
	if _, err := url.Parse(cfg.Quickwit.URL); err != nil {
		return fmt.Errorf("invalid quickwit.url: %w", err)
	}

	return nil
}
