package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/jonboulle/clockwork"
	"github.com/lmittmann/tint"
	slogformatter "github.com/samber/slog-formatter"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/logging"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
	"github.com/xevion/railway-collector/internal/state"
)

// RunCmd starts the collector.
type RunCmd struct{}

func (cmd *RunCmd) Run(c *CLI) error {
	// Load .env from cwd if present (errors ignored -- file is optional)
	_ = godotenv.Load()

	cfg, err := config.Load(c.Config)
	if err != nil {
		return err
	}

	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "trace":
		level = logging.LevelTrace
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	// Build handler chain: base -> slog-formatter -> filtering
	var baseHandler slog.Handler
	if os.Getenv("LOG_JSON") == "true" {
		baseHandler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	} else {
		baseHandler = tint.NewHandler(os.Stderr, &tint.Options{
			Level:       level,
			TimeFormat:  time.TimeOnly,
			ReplaceAttr: logging.ReplaceAttrFunc,
		})
	}

	formatted := slogformatter.NewFormatterHandler(logging.Formatters()...)(baseHandler)
	handler := logging.NewFilteringHandler(formatted,
		"Unsolicited response received on idle HTTP channel",
	)

	logger := slog.New(handler)
	slog.SetDefault(logger)

	// Route stdlib log.Print (from net/http, etc.) through slog at WARN
	slog.SetLogLoggerLevel(slog.LevelWarn)
	_ = log.Default()

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
			logger.Error("failed to create file sink", "path", cfg.Sinks.File.Path, "error", err)
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
			logger.Error("failed to create OTLP sink", "metrics_endpoint", cfg.Sinks.OTLP.MetricsEndpoint, "logs_endpoint", cfg.Sinks.OTLP.LogsEndpoint, "error", err)
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
		logger.Error("failed to open state store", "path", cfg.State.Path, "error", err)
		os.Exit(1)
	}
	logger.Info("state store opened", "path", cfg.State.Path)

	// Initialize discovery with caching
	discovery := collector.NewDiscovery(collector.DiscoveryConfig{
		Client:         client,
		Store:          store,
		Clock:          clockwork.NewRealClock(),
		Filters:        cfg.Filters,
		Workspaces:     workspaces,
		WorkspaceTTL:   cfg.Collect.Discovery.WorkspaceTTL,
		ProjectTTL:     cfg.Collect.Discovery.ProjectTTL,
		ProjectListTTL: cfg.Collect.Discovery.ProjectListTTL,
		Jitter:         cfg.Collect.Discovery.Jitter,
		Logger:         logger,
	})
	if err := discovery.Refresh(ctx); err != nil {
		logger.Error("initial discovery failed", "workspaces", len(workspaces), "error", err)
		os.Exit(1)
	}

	// Convert measurement strings to enum values
	measurements := collector.ResolveMeasurements(cfg.Collect.Metrics.Measurements)

	realClock := clockwork.NewRealClock()
	now := realClock.Now()

	// Build credit allocator from config
	creditCfg := collector.CreditConfig{
		MetricsRate:   cfg.Collect.Credits.MetricsRate,
		LogsRate:      cfg.Collect.Credits.LogsRate,
		DiscoveryRate: cfg.Collect.Credits.DiscoveryRate,
		BackfillRate:  cfg.Collect.Credits.BackfillRate,
		MaxCredits:    cfg.Collect.Credits.MaxCredits,
	}
	credits := collector.NewCreditAllocator(creditCfg, now)

	// Build generators
	var generators []collector.TaskGenerator

	if cfg.Collect.Metrics.Enabled {
		generators = append(generators, collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
			Discovery:    discovery,
			Store:        store,
			Sinks:        sinks,
			Clock:        realClock,
			Measurements: measurements,
			SampleRate:   cfg.Collect.Metrics.SampleRateSeconds,
			AvgWindow:    cfg.Collect.Metrics.AveragingWindowSeconds,
			Lookback:     cfg.Collect.Metrics.Lookback,
			Interval:     cfg.Collect.Metrics.Interval,
			Logger:       logger,
		}))
	}

	if cfg.Collect.Logs.Enabled {
		generators = append(generators, collector.NewLogsGenerator(collector.LogsGeneratorConfig{
			Discovery: discovery,
			Store:     store,
			Sinks:     sinks,
			Clock:     realClock,
			Types:     cfg.Collect.Logs.Types,
			Limit:     cfg.Collect.Logs.Limit,
			Interval:  cfg.Collect.Logs.Interval,
			Logger:    logger,
		}))
	}

	if cfg.Collect.Resources.Enabled {
		generators = append(generators, collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
			Discovery: discovery,
			Interval:  cfg.Collect.Resources.Interval,
			Logger:    logger,
		}))
	}

	if cfg.Collect.Backfill.Enabled {
		generators = append(generators, collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
			Discovery:       discovery,
			Store:           store,
			Sinks:           sinks,
			Clock:           realClock,
			Measurements:    measurements,
			SampleRate:      cfg.Collect.Metrics.SampleRateSeconds,
			AvgWindow:       cfg.Collect.Metrics.AveragingWindowSeconds,
			MetricRetention: cfg.Collect.Backfill.MetricRetention,
			LogRetention:    cfg.Collect.Backfill.LogRetention,
			ChunkSize:       cfg.Collect.Backfill.MetricChunkSize,
			Interval:        cfg.Collect.Backfill.Interval,
			MaxItemsPerPoll: cfg.Collect.Backfill.MaxItemsPerPoll,
			LogLimit:        cfg.Collect.Logs.Limit,
			Logger:          logger,
		}))
	}

	sinkNames := make([]string, len(sinks))
	for i, s := range sinks {
		sinkNames[i] = s.Name()
	}
	logger.Info("railway collector started",
		"metrics_enabled", cfg.Collect.Metrics.Enabled,
		"logs_enabled", cfg.Collect.Logs.Enabled,
		"backfill_enabled", cfg.Collect.Backfill.Enabled,
		"targets", len(discovery.Targets()),
		"generators", len(generators),
		"sinks", strings.Join(sinkNames, ","),
	)

	// Build unified scheduler
	scheduler := collector.NewUnifiedScheduler(collector.UnifiedSchedulerConfig{
		Clock:        realClock,
		API:          client,
		Credits:      credits,
		Generators:   generators,
		Logger:       logger,
		TickInterval: cfg.Collect.Scheduler.TickInterval,
		MaxRPS:       cfg.Collect.Scheduler.MaxRPS,
		DrainTimeout: cfg.Collect.Scheduler.DrainTimeout,
	})

	// Two-phase shutdown driven by signal handler:
	//   Phase 1: Stop() -- tickers stop, Run drains in-flight work (5s timeout)
	//   Phase 2: cancel() -- context cancelled, aborts in-flight HTTP requests,
	//            Run drains again (5s timeout) then returns
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down, waiting for in-flight collections...")
		scheduler.Stop()
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_ = scheduler.Run(ctx)
	cancel()

	for _, s := range sinks {
		if err := s.Close(); err != nil {
			logger.Error("failed to close sink", "sink", s.Name(), "error", err)
		}
	}
	if err := store.Close(); err != nil {
		logger.Error("failed to close state store", "error", err)
	}
	return nil
}
