package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
	"github.com/xevion/railway-collector/internal/state"
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file")
	flag.Parse()

	// Load .env from cwd if present (errors ignored -- file is optional)
	_ = godotenv.Load()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	// Rate limit: ~2 RPS keeps us well within Hobby plan (1000 RPH ~ 16.6 RPM)
	client := railway.NewClient(cfg.Railway.Token, 2.0, logger)

	// Verify auth
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	me, err := client.Me(ctx)
	if err != nil {
		logger.Error("failed to authenticate with Railway API", "error", err)
		os.Exit(1)
	}
	logger.Info("authenticated", "user", me.Me.Name, "email", me.Me.Email, "workspaces", len(me.Me.Workspaces))

	// Build workspace list: if a specific workspace ID is configured, use only that;
	// otherwise discover all workspaces from the authenticated user.
	var workspaces []collector.Workspace
	if cfg.Railway.WorkspaceID != "" {
		workspaces = []collector.Workspace{{ID: cfg.Railway.WorkspaceID}}
		logger.Info("using configured workspace", "id", cfg.Railway.WorkspaceID)
	} else {
		for _, ws := range me.Me.Workspaces {
			workspaces = append(workspaces, collector.Workspace{ID: ws.Id, Name: ws.Name})
			logger.Info("discovered workspace", "name", ws.Name, "id", ws.Id)
		}
	}

	// Initialize sinks
	var sinks []sink.Sink

	if cfg.Sinks.Prometheus.Enabled {
		sinks = append(sinks, sink.NewPrometheusSink(cfg.Sinks.Prometheus.Listen, logger))
	}
	if cfg.Sinks.File.Enabled {
		fs, err := sink.NewFileSink(cfg.Sinks.File.Path, logger)
		if err != nil {
			logger.Error("failed to create file sink", "error", err)
			os.Exit(1)
		}
		sinks = append(sinks, fs)
	}
	if cfg.Sinks.OTLP.Enabled {
		otlpSink, err := sink.NewOTLPSink(ctx, sink.OTLPConfig{
			MetricsEndpoint: cfg.Sinks.OTLP.MetricsEndpoint,
			LogsEndpoint:    cfg.Sinks.OTLP.LogsEndpoint,
			Headers:         cfg.Sinks.OTLP.Headers,
		}, logger)
		if err != nil {
			logger.Error("failed to create OTLP sink", "error", err)
			os.Exit(1)
		}
		sinks = append(sinks, otlpSink)
	}

	if len(sinks) == 0 {
		logger.Error("no sinks enabled, nothing to do")
		os.Exit(1)
	}

	// Initialize state store
	store, err := state.Open(cfg.State.Path)
	if err != nil {
		logger.Error("failed to open state store", "error", err)
		os.Exit(1)
	}
	logger.Info("state store opened", "path", cfg.State.Path)

	// Initialize discovery with caching
	discovery := collector.NewDiscovery(collector.DiscoveryConfig{
		Client:       client,
		Filters:      cfg.Filters,
		Workspaces:   workspaces,
		WorkspaceTTL: cfg.Collect.Discovery.WorkspaceTTL,
		ProjectTTL:   cfg.Collect.Discovery.ProjectTTL,
		Jitter:       cfg.Collect.Discovery.Jitter,
		Logger:       logger,
	})
	if err := discovery.Refresh(ctx); err != nil {
		logger.Error("initial discovery failed", "error", err)
		os.Exit(1)
	}

	// Log estimated API call budget
	targetCount := len(discovery.Targets())
	envSet := make(map[string]bool)
	for _, t := range discovery.Targets() {
		envSet[t.EnvironmentID] = true
	}
	uniqueEnvs := len(envSet)
	projSet := make(map[string]bool)
	for _, t := range discovery.Targets() {
		projSet[t.ProjectID] = true
	}
	uniqueProjects := len(projSet)

	metricsCallsPerHour := 0
	if cfg.Collect.Metrics.Enabled {
		metricsCallsPerHour = uniqueProjects * int(time.Hour/cfg.Collect.Metrics.Interval)
	}
	logCallsPerCycle := 0
	if cfg.Collect.Logs.Enabled {
		logCallsPerCycle += uniqueEnvs      // environment logs
		logCallsPerCycle += targetCount * 2 // build + http per deployment
	}
	logCallsPerHour := 0
	if cfg.Collect.Logs.Enabled && cfg.Collect.Logs.Interval > 0 {
		logCallsPerHour = logCallsPerCycle * int(time.Hour/cfg.Collect.Logs.Interval)
	}
	discoveryCallsPerHour := 0
	if cfg.Collect.Resources.Enabled && cfg.Collect.Resources.Interval > 0 {
		// With caching, most discovery refreshes are no-ops (cache hits).
		// Worst case: 1 workspace query + 1 project query per workspace per hour
		// + 2 calls per target per hour (deployment + service instance)
		discoveryCallsPerCycle := len(workspaces) + targetCount*2
		discoveryCallsPerHour = discoveryCallsPerCycle * int(time.Hour/cfg.Collect.Resources.Interval)
	}
	totalPerHour := metricsCallsPerHour + logCallsPerHour + discoveryCallsPerHour
	logger.Info("estimated API call budget",
		"metrics_per_hour", metricsCallsPerHour,
		"logs_per_hour", logCallsPerHour,
		"discovery_per_hour", discoveryCallsPerHour,
		"total_per_hour", totalPerHour,
		"projects", uniqueProjects,
		"environments", uniqueEnvs,
		"targets", targetCount,
	)
	if totalPerHour > 900 {
		logger.Warn("estimated API calls exceed safe budget for Hobby plan (1000 RPH); consider increasing intervals or adding filters",
			"estimated", totalPerHour, "limit", 1000)
	}

	// Initialize collectors
	var metricsCollector *collector.MetricsCollector
	if cfg.Collect.Metrics.Enabled {
		metricsCollector = collector.NewMetricsCollector(
			client, discovery, sinks, store,
			cfg.Collect.Metrics.Measurements,
			cfg.Collect.Metrics.SampleRateSeconds,
			cfg.Collect.Metrics.AveragingWindowSeconds,
			cfg.Collect.Metrics.Lookback,
			logger,
		)
	}

	var logsCollector *collector.LogsCollector
	if cfg.Collect.Logs.Enabled {
		logsCollector = collector.NewLogsCollector(
			client, discovery, sinks,
			cfg.Collect.Logs.Types,
			cfg.Collect.Logs.Limit,
			store,
			logger,
		)
	}

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("railway collector started",
		"metrics_enabled", cfg.Collect.Metrics.Enabled,
		"logs_enabled", cfg.Collect.Logs.Enabled,
		"targets", len(discovery.Targets()),
	)

	// Collection loop -- only create tickers for enabled collectors;
	// nil channels block forever in select, effectively disabling that case.
	var metricsCh <-chan time.Time
	if metricsCollector != nil {
		t := time.NewTicker(cfg.Collect.Metrics.Interval)
		defer t.Stop()
		metricsCh = t.C
	}
	var logsCh <-chan time.Time
	if logsCollector != nil {
		t := time.NewTicker(cfg.Collect.Logs.Interval)
		defer t.Stop()
		logsCh = t.C
	}
	var discoveryCh <-chan time.Time
	if cfg.Collect.Resources.Enabled {
		t := time.NewTicker(cfg.Collect.Resources.Interval)
		defer t.Stop()
		discoveryCh = t.C
	}

	var wg sync.WaitGroup

	// Collect immediately on start
	if metricsCollector != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := metricsCollector.Collect(ctx); err != nil {
				logger.Error("initial metrics collection failed", "error", err)
			}
		}()
	}
	if logsCollector != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := logsCollector.Collect(ctx); err != nil {
				logger.Error("initial logs collection failed", "error", err)
			}
		}()
	}

	for {
		select {
		case <-sigCh:
			logger.Info("shutting down, waiting for in-flight collections...")
			cancel()

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
				logger.Debug("all collections drained")
			case <-time.After(10 * time.Second):
				logger.Warn("timed out waiting for in-flight collections")
			}

			for _, s := range sinks {
				if err := s.Close(); err != nil {
					logger.Error("failed to close sink", "sink", s.Name(), "error", err)
				}
			}
			if err := store.Close(); err != nil {
				logger.Error("failed to close state store", "error", err)
			}
			return

		case <-metricsCh:
			// Circuit breaker: skip if rate limited
			if limited, wait := client.IsRateLimited(); limited {
				logger.Warn("skipping metrics collection, API rate limited", "wait", wait.Round(time.Second))
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := metricsCollector.Collect(ctx); err != nil {
					logger.Error("metrics collection failed", "error", err)
				}
			}()

		case <-logsCh:
			if limited, wait := client.IsRateLimited(); limited {
				logger.Warn("skipping logs collection, API rate limited", "wait", wait.Round(time.Second))
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := logsCollector.Collect(ctx); err != nil {
					logger.Error("logs collection failed", "error", err)
				}
			}()

		case <-discoveryCh:
			if limited, wait := client.IsRateLimited(); limited {
				logger.Warn("skipping discovery refresh, API rate limited", "wait", wait.Round(time.Second))
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := discovery.Refresh(ctx); err != nil {
					logger.Error("discovery refresh failed", "error", err)
				}
			}()
		}
	}
}
