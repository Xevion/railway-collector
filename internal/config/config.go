package config

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/structs"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	Railway  RailwayConfig `koanf:"railway"`
	Collect  CollectConfig `koanf:"collect"`
	Filters  FiltersConfig `koanf:"filters"`
	Sinks    SinksConfig   `koanf:"sinks"`
	State    StateConfig   `koanf:"state"`
	LogLevel string        `koanf:"log_level"`
}

type StateConfig struct {
	// Path to bbolt database for persisting cursors across restarts
	Path string `koanf:"path"`
}

type RailwayConfig struct {
	Token string `koanf:"token"`
	// Optional: restrict to a single workspace
	WorkspaceID string `koanf:"workspace_id"`
}

type CollectConfig struct {
	Metrics   MetricsCollectConfig   `koanf:"metrics"`
	Logs      LogsCollectConfig      `koanf:"logs"`
	Discovery DiscoveryCollectConfig `koanf:"discovery"`
	Backfill  BackfillCollectConfig  `koanf:"backfill"`
	Scheduler SchedulerConfig        `koanf:"scheduler"`
	Credits   CreditsConfig          `koanf:"credits"`
}

type SchedulerConfig struct {
	// TickInterval controls how often the scheduler polls generators (default 1s)
	TickInterval time.Duration `koanf:"tick_interval"`
	// MaxRPS caps requests per second (default ~0.267, i.e. 16/min)
	MaxRPS float64 `koanf:"max_rps"`
	// DrainTimeout is how long to wait for in-flight work during shutdown (default 5s)
	DrainTimeout time.Duration `koanf:"drain_timeout"`
}

type CreditsConfig struct {
	// Credits per minute for each task type
	MetricsRate   float64 `koanf:"metrics_rate"`
	LogsRate      float64 `koanf:"logs_rate"`
	DiscoveryRate float64 `koanf:"discovery_rate"`
	BackfillRate  float64 `koanf:"backfill_rate"`
	// Maximum accumulated credits per type (prevents burst after idle)
	MaxCredits float64 `koanf:"max_credits"`
}

type BackfillCollectConfig struct {
	Enabled         bool          `koanf:"enabled"`
	Interval        time.Duration `koanf:"interval"`
	MetricChunkSize time.Duration `koanf:"metric_chunk_size"`
	MetricRetention time.Duration `koanf:"metric_retention"`
	LogRetention    time.Duration `koanf:"log_retention"`
	MaxItemsPerPoll int           `koanf:"max_items_per_poll"`
}

type DiscoveryCollectConfig struct {
	// Whether periodic discovery refresh is enabled (default true)
	RefreshEnabled bool `koanf:"refresh_enabled"`
	// How often to run periodic discovery refresh (default 2h)
	RefreshInterval time.Duration `koanf:"refresh_interval"`
	// Base TTL for workspace list cache (default 1h)
	WorkspaceTTL time.Duration `koanf:"workspace_ttl"`
	// Base TTL for per-project discovery cache (default 1h)
	ProjectTTL time.Duration `koanf:"project_ttl"`
	// Base TTL for project list (GetProjects) cache (default 4h)
	ProjectListTTL time.Duration `koanf:"project_list_ttl"`
	// Random jitter applied to TTLs: ±jitter (default 15m)
	Jitter time.Duration `koanf:"jitter"`
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

type FiltersConfig struct {
	Projects     []string `koanf:"projects"`
	Services     []string `koanf:"services"`
	Environments []string `koanf:"environments"`
}

type SinksConfig struct {
	Prometheus PrometheusSinkConfig `koanf:"prometheus"`
	File       FileSinkConfig       `koanf:"file"`
	OTLP       OTLPSinkConfig       `koanf:"otlp"`
}

type OTLPSinkConfig struct {
	Enabled bool `koanf:"enabled"`
	// Endpoint for OTLP HTTP metrics (e.g. http://victoriametrics:8428/opentelemetry/v1/metrics)
	MetricsEndpoint string `koanf:"metrics_endpoint"`
	// Endpoint for OTLP HTTP logs (e.g. http://victorialogs:9428/insert/opentelemetry/v1/logs)
	LogsEndpoint string `koanf:"logs_endpoint"`
	// Extra headers for requests (e.g. VL-Stream-Fields for VictoriaLogs)
	Headers map[string]string `koanf:"headers"`
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
		State: StateConfig{
			Path: "./railway-collector.db",
		},
		Collect: CollectConfig{
			Metrics: MetricsCollectConfig{
				Enabled:                true,
				Interval:               5 * time.Minute,
				Measurements:           []string{"cpu", "memory", "network_rx", "network_tx", "disk"},
				SampleRateSeconds:      60,
				AveragingWindowSeconds: 60,
				Lookback:               10 * time.Minute,
			},
			Logs: LogsCollectConfig{
				Enabled:  true,
				Interval: 2 * time.Minute,
				Types:    []string{"deployment", "build", "http"},
				Limit:    500,
			},
			Discovery: DiscoveryCollectConfig{
				RefreshEnabled:  true,
				RefreshInterval: 2 * time.Hour,
				WorkspaceTTL:    time.Hour,
				ProjectTTL:      time.Hour,
				ProjectListTTL:  4 * time.Hour,
				Jitter:          15 * time.Minute,
			},
			Backfill: BackfillCollectConfig{
				Enabled:         true,
				Interval:        30 * time.Minute,
				MetricChunkSize: 10 * 24 * time.Hour,
				MetricRetention: 90 * 24 * time.Hour,
				LogRetention:    5 * 24 * time.Hour,
				MaxItemsPerPoll: 10,
			},
			Scheduler: SchedulerConfig{
				// Zero values use code defaults (1s tick, ~16/min RPS, 5s drain)
			},
			Credits: CreditsConfig{
				MetricsRate:   2.0,
				LogsRate:      2.0,
				DiscoveryRate: 1.0,
				BackfillRate:  11.0,
				MaxCredits:    4.0,
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

	// Load defaults into koanf first so YAML/env layers merge on top
	if err := k.Load(structs.Provider(*DefaultConfig(), "koanf"), nil); err != nil {
		return nil, fmt.Errorf("loading defaults: %w", err)
	}

	// Load YAML config: use the provided path, or fall back to "config.yaml" in the
	// working directory. A missing default file is silently ignored; a missing
	// explicitly-provided file is always an error.
	usingDefault := path == ""
	if usingDefault {
		path = "config.yaml"
	}
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		if !usingDefault || !errors.Is(err, fs.ErrNotExist) {
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

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("unmarshaling config: %w", err)
	}

	if cfg.Railway.Token == "" {
		return nil, fmt.Errorf("railway token is required (set railway.token in config or RAILWAY_TOKEN env var)")
	}

	return &cfg, nil
}
