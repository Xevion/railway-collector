package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	Railway  RailwayConfig `koanf:"railway"`
	Collect  CollectConfig `koanf:"collect"`
	Filters  FiltersConfig `koanf:"filters"`
	Sinks    SinksConfig   `koanf:"sinks"`
	LogLevel string        `koanf:"log_level"`
}

type RailwayConfig struct {
	Token string `koanf:"token"`
	// Optional: restrict to a single workspace
	WorkspaceID string `koanf:"workspace_id"`
}

type CollectConfig struct {
	Metrics   MetricsCollectConfig   `koanf:"metrics"`
	Logs      LogsCollectConfig      `koanf:"logs"`
	Resources ResourcesCollectConfig `koanf:"resources"`
}

type MetricsCollectConfig struct {
	Enabled                bool          `koanf:"enabled"`
	Interval               time.Duration `koanf:"interval"`
	Measurements           []string      `koanf:"measurements"`
	SampleRateSeconds      int           `koanf:"sample_rate_seconds"`
	AveragingWindowSeconds int           `koanf:"averaging_window_seconds"`
	// How far back to look on each scrape
	Lookback time.Duration `koanf:"lookback"`
}

type LogsCollectConfig struct {
	Enabled  bool          `koanf:"enabled"`
	Interval time.Duration `koanf:"interval"`
	Types    []string      `koanf:"types"`
	// Max logs per query
	Limit int `koanf:"limit"`
}

type ResourcesCollectConfig struct {
	Enabled  bool          `koanf:"enabled"`
	Interval time.Duration `koanf:"interval"`
}

type FiltersConfig struct {
	Projects     []string `koanf:"projects"`
	Services     []string `koanf:"services"`
	Environments []string `koanf:"environments"`
}

type SinksConfig struct {
	Prometheus PrometheusSinkConfig `koanf:"prometheus"`
	File       FileSinkConfig       `koanf:"file"`
}

type PrometheusSinkConfig struct {
	Enabled bool   `koanf:"enabled"`
	Listen  string `koanf:"listen"`
}

type FileSinkConfig struct {
	Enabled bool   `koanf:"enabled"`
	Path    string `koanf:"path"`
}

func DefaultConfig() *Config {
	return &Config{
		LogLevel: "info",
		Collect: CollectConfig{
			Metrics: MetricsCollectConfig{
				Enabled:                true,
				Interval:               30 * time.Second,
				Measurements:           []string{"cpu", "memory", "network_rx", "network_tx", "disk"},
				SampleRateSeconds:      60,
				AveragingWindowSeconds: 60,
				Lookback:               5 * time.Minute,
			},
			Logs: LogsCollectConfig{
				Enabled:  true,
				Interval: 15 * time.Second,
				Types:    []string{"deployment", "build", "http"},
				Limit:    500,
			},
			Resources: ResourcesCollectConfig{
				Enabled:  true,
				Interval: 5 * time.Minute,
			},
		},
		Sinks: SinksConfig{
			Prometheus: PrometheusSinkConfig{
				Enabled: true,
				Listen:  ":9106",
			},
			File: FileSinkConfig{
				Enabled: false,
				Path:    "./railway-collector.jsonl",
			},
		},
	}
}

func Load(path string) (*Config, error) {
	k := koanf.New(".")

	// Load defaults by marshaling the default config
	cfg := DefaultConfig()

	// Load YAML file if provided
	if path != "" {
		if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("loading config file %s: %w", path, err)
		}
	}

	// Load env vars with RAILWAY_COLLECTOR_ prefix
	if err := k.Load(env.Provider("RAILWAY_COLLECTOR_", ".", func(s string) string {
		return strings.Replace(
			strings.ToLower(strings.TrimPrefix(s, "RAILWAY_COLLECTOR_")),
			"__", ".", -1,
		)
	}), nil); err != nil {
		return nil, fmt.Errorf("loading env vars: %w", err)
	}

	// Support bare RAILWAY_TOKEN and LOG_LEVEL env vars for convenience
	if err := k.Load(env.Provider("RAILWAY_TOKEN", ".", func(s string) string {
		if s == "RAILWAY_TOKEN" {
			return "railway.token"
		}
		return ""
	}), nil); err != nil {
		return nil, fmt.Errorf("loading RAILWAY_TOKEN: %w", err)
	}

	if err := k.Load(env.Provider("LOG_LEVEL", ".", func(s string) string {
		if s == "LOG_LEVEL" {
			return "log_level"
		}
		return ""
	}), nil); err != nil {
		return nil, fmt.Errorf("loading LOG_LEVEL: %w", err)
	}

	if err := k.Unmarshal("", cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if cfg.Railway.Token == "" {
		return nil, fmt.Errorf("railway token is required (set railway.token in config or RAILWAY_TOKEN env var)")
	}

	return cfg, nil
}
