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
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file")
	flag.Parse()

	// Load .env from cwd if present (errors ignored — file is optional)
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

	// Rate limit: ~5 RPS is safe for Pro plan (10k RPH = ~2.7 RPS, leave headroom)
	client := railway.NewClient(cfg.Railway.Token, 5.0, logger)

	// Verify auth
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	me, err := client.Me(ctx)
	if err != nil {
		logger.Error("failed to authenticate with Railway API", "error", err)
		os.Exit(1)
	}
	logger.Info("authenticated", "user", me.Me.Name, "email", me.Me.Email, "workspaces", len(me.Me.Workspaces))

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

	if len(sinks) == 0 {
		logger.Error("no sinks enabled, nothing to do")
		os.Exit(1)
	}

	// Initialize discovery
	discovery := collector.NewDiscovery(client, cfg.Filters, cfg.Railway.WorkspaceID, logger)
	if err := discovery.Refresh(ctx); err != nil {
		logger.Error("initial discovery failed", "error", err)
		os.Exit(1)
	}

	// Initialize collectors
	var metricsCollector *collector.MetricsCollector
	if cfg.Collect.Metrics.Enabled {
		metricsCollector = collector.NewMetricsCollector(
			client, discovery, sinks,
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

	// Collection loop — only create tickers for enabled collectors;
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

			// Wait for in-flight goroutines with a timeout
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
			return

		case <-metricsCh:
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := metricsCollector.Collect(ctx); err != nil {
					logger.Error("metrics collection failed", "error", err)
				}
			}()

		case <-logsCh:
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := logsCollector.Collect(ctx); err != nil {
					logger.Error("logs collection failed", "error", err)
				}
			}()

		case <-discoveryCh:
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
