package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/lmittmann/tint"
	slogformatter "github.com/samber/slog-formatter"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/logging"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
	"github.com/xevion/railway-collector/internal/state"
)

// safeBudget is the maximum API calls per hour before auto-adjustment kicks in.
const safeBudget = 900

type budgetResult struct {
	metricsInterval  time.Duration
	logsInterval     time.Duration
	metricsPerHour   int
	logsPerHour      int
	discoveryPerHour int // effective (post-cache) rate used for budgeting
	discoveryWorst   int // worst-case rate before cache discount
	totalPerHour     int
}

// calculateBudget estimates API calls/hour from the current target set and config,
// then returns recommended collection intervals (auto-adjusted if the estimate
// exceeds safeBudget). It does NOT modify cfg.
func calculateBudget(
	targets []collector.ServiceTarget,
	workspaceCount int,
	cfg *config.Config,
	logger *slog.Logger,
) budgetResult {
	targetCount := len(targets)
	envSet := make(map[string]bool)
	for _, t := range targets {
		envSet[t.EnvironmentID] = true
	}
	uniqueEnvs := len(envSet)
	projSet := make(map[string]bool)
	for _, t := range targets {
		projSet[t.ProjectID] = true
	}
	uniqueProjects := len(projSet)

	metricsInterval := cfg.Collect.Metrics.Interval
	logsInterval := cfg.Collect.Logs.Interval

	metricsCallsPerHour := 0
	if cfg.Collect.Metrics.Enabled {
		metricsCallsPerHour = uniqueProjects * int(time.Hour/metricsInterval)
	}
	logCallsPerCycle := 0
	if cfg.Collect.Logs.Enabled {
		logCallsPerCycle += uniqueEnvs      // environment logs
		logCallsPerCycle += targetCount * 2 // build + http per deployment
	}
	logCallsPerHour := 0
	if cfg.Collect.Logs.Enabled && logsInterval > 0 {
		logCallsPerHour = logCallsPerCycle * int(time.Hour/logsInterval)
	}

	discoveryCallsPerHour := 0
	if cfg.Collect.Resources.Enabled && cfg.Collect.Resources.Interval > 0 {
		discoveryCallsPerCycle := workspaceCount + targetCount*2
		discoveryCallsPerHour = discoveryCallsPerCycle * int(time.Hour/cfg.Collect.Resources.Interval)
	}

	// After the first cycle, ~80% of discovery calls will be cache hits.
	// Use the effective (post-cache) rate for budget allocation.
	effectiveDiscoveryPerHour := int(float64(discoveryCallsPerHour) * 0.2)

	totalPerHour := metricsCallsPerHour + logCallsPerHour + effectiveDiscoveryPerHour
	logger.Info("estimated API call budget",
		"metrics_per_hour", metricsCallsPerHour,
		"logs_per_hour", logCallsPerHour,
		"discovery_per_hour_worst", discoveryCallsPerHour,
		"discovery_per_hour_effective", effectiveDiscoveryPerHour,
		"total_per_hour", totalPerHour,
		"projects", uniqueProjects,
		"environments", uniqueEnvs,
		"targets", targetCount,
	)

	// Auto-adjust metrics and logs intervals if estimated budget exceeds safeBudget.
	// Discovery is left alone since it's mostly cache hits.
	if totalPerHour > safeBudget && (metricsCallsPerHour+logCallsPerHour) > 0 {
		availableBudget := safeBudget - effectiveDiscoveryPerHour
		if availableBudget <= 0 {
			logger.Error("discovery alone exceeds API budget; cannot auto-adjust collection intervals",
				"discovery_per_hour", effectiveDiscoveryPerHour, "budget", safeBudget)
		} else {
			scaleFactor := float64(metricsCallsPerHour+logCallsPerHour) / float64(availableBudget)
			if scaleFactor < 1 {
				scaleFactor = 1
			}

			if cfg.Collect.Metrics.Enabled {
				orig := metricsInterval
				metricsInterval = time.Duration(float64(orig) * scaleFactor)
				logger.Warn("auto-adjusted metrics interval to fit API budget",
					"original", orig, "adjusted", metricsInterval)
			}
			if cfg.Collect.Logs.Enabled {
				orig := logsInterval
				logsInterval = time.Duration(float64(orig) * scaleFactor)
				logger.Warn("auto-adjusted logs interval to fit API budget",
					"original", orig, "adjusted", logsInterval)
			}

			// Recalculate with adjusted intervals
			if cfg.Collect.Metrics.Enabled {
				metricsCallsPerHour = uniqueProjects * int(time.Hour/metricsInterval)
			}
			if cfg.Collect.Logs.Enabled && logsInterval > 0 {
				logCallsPerHour = logCallsPerCycle * int(time.Hour/logsInterval)
			}
			totalPerHour = metricsCallsPerHour + logCallsPerHour + effectiveDiscoveryPerHour
			logger.Info("adjusted API call budget",
				"metrics_per_hour", metricsCallsPerHour,
				"logs_per_hour", logCallsPerHour,
				"discovery_per_hour", effectiveDiscoveryPerHour,
				"total_per_hour", totalPerHour,
			)
		}
	}

	return budgetResult{
		metricsInterval:  metricsInterval,
		logsInterval:     logsInterval,
		metricsPerHour:   metricsCallsPerHour,
		logsPerHour:      logCallsPerHour,
		discoveryPerHour: effectiveDiscoveryPerHour,
		discoveryWorst:   discoveryCallsPerHour,
		totalPerHour:     totalPerHour,
	}
}

// significantChange reports whether two durations differ by more than 10%.
func significantChange(a, b time.Duration) bool {
	if a == 0 || b == 0 {
		return a != b
	}
	ratio := float64(a) / float64(b)
	return math.Abs(ratio-1) > 0.10
}

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
	case "trace":
		level = logging.LevelTrace
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	// Build handler chain: base → slog-formatter → filtering
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
		Client:         client,
		Filters:        cfg.Filters,
		Workspaces:     workspaces,
		WorkspaceTTL:   cfg.Collect.Discovery.WorkspaceTTL,
		ProjectTTL:     cfg.Collect.Discovery.ProjectTTL,
		ProjectListTTL: cfg.Collect.Discovery.ProjectListTTL,
		Jitter:         cfg.Collect.Discovery.Jitter,
		Store:          store,
		Logger:         logger,
	})
	if err := discovery.Refresh(ctx); err != nil {
		logger.Error("initial discovery failed", "error", err)
		os.Exit(1)
	}

	// Calculate initial API budget and recommended intervals
	budget := calculateBudget(discovery.Targets(), len(workspaces), cfg, logger)
	currentMetricsInterval := budget.metricsInterval
	currentLogsInterval := budget.logsInterval

	// Initialize collectors
	var metricsCollector *collector.MetricsCollector
	if cfg.Collect.Metrics.Enabled {
		metricsCollector = collector.NewMetricsCollector(
			client, discovery, sinks, store,
			cfg.Collect.Metrics.Measurements,
			cfg.Collect.Metrics.SampleRateSeconds,
			cfg.Collect.Metrics.AveragingWindowSeconds,
			cfg.Collect.Metrics.Lookback,
			currentMetricsInterval,
			logger,
		)
	}

	var logsCollector *collector.LogsCollector
	if cfg.Collect.Logs.Enabled {
		logsCollector = collector.NewLogsCollector(
			client, discovery, sinks,
			cfg.Collect.Logs.Types,
			cfg.Collect.Logs.Limit,
			currentLogsInterval,
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
	// Ticker references are stored at this scope so discovery can Reset() them.
	var tickers []*time.Ticker
	var metricsTicker, logsTicker *time.Ticker

	var metricsCh <-chan time.Time
	if metricsCollector != nil {
		metricsTicker = time.NewTicker(currentMetricsInterval)
		defer metricsTicker.Stop()
		tickers = append(tickers, metricsTicker)
		metricsCh = metricsTicker.C
	}
	var logsCh <-chan time.Time
	if logsCollector != nil {
		logsTicker = time.NewTicker(currentLogsInterval)
		defer logsTicker.Stop()
		tickers = append(tickers, logsTicker)
		logsCh = logsTicker.C
	}
	var discoveryCh <-chan time.Time
	if cfg.Collect.Resources.Enabled {
		t := time.NewTicker(cfg.Collect.Resources.Interval)
		defer t.Stop()
		tickers = append(tickers, t)
		discoveryCh = t.C
	}

	var wg sync.WaitGroup
	discoveryDone := make(chan struct{}, 1)

	// Overlap protection: skip a cycle if the previous one is still running
	var metricsRunning, logsRunning, discoveryRunning sync.Mutex

	// Collect immediately on start
	if metricsCollector != nil {
		metricsRunning.Lock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer metricsRunning.Unlock()
			if err := metricsCollector.Collect(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Debug("initial metrics collection cancelled")
				} else {
					logger.Error("initial metrics collection failed", "error", err)
				}
			}
		}()
	}
	if logsCollector != nil {
		logsRunning.Lock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer logsRunning.Unlock()
			if err := logsCollector.Collect(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Debug("initial logs collection cancelled")
				} else {
					logger.Error("initial logs collection failed", "error", err)
				}
			}
		}()
	}

	for {
		select {
		case <-sigCh:
			logger.Info("shutting down, waiting for in-flight collections...")

			// Phase 1 (soft): stop tickers so no new work is spawned,
			// then wait up to 5s for in-flight goroutines to finish.
			for _, t := range tickers {
				t.Stop()
			}

			drained := make(chan struct{})
			go func() {
				wg.Wait()
				close(drained)
			}()

			select {
			case <-drained:
				logger.Debug("all collections drained")
			case <-time.After(5 * time.Second):
				// Phase 2 (hard): cancel the context to abort in-flight HTTP
				// requests, then wait another 5s for goroutines to return.
				logger.Warn("in-flight collections still running, cancelling context...")
				cancel()
				select {
				case <-drained:
					logger.Debug("all collections drained after cancel")
				case <-time.After(5 * time.Second):
					logger.Warn("timed out waiting for in-flight collections")
				}
			}

			// Ensure context is cancelled before closing sinks, even on clean drain
			cancel()

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
			if limited, wait := client.IsRateLimited(); limited {
				logger.Warn("skipping metrics collection, API rate limited", "wait", wait.Round(time.Second))
				continue
			}
			if !metricsRunning.TryLock() {
				logger.Warn("skipping metrics collection, previous cycle still running")
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer metricsRunning.Unlock()
				if err := metricsCollector.Collect(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						logger.Debug("metrics collection cancelled")
					} else {
						logger.Error("metrics collection failed", "error", err)
					}
				}
			}()

		case <-logsCh:
			if limited, wait := client.IsRateLimited(); limited {
				logger.Warn("skipping logs collection, API rate limited", "wait", wait.Round(time.Second))
				continue
			}
			if !logsRunning.TryLock() {
				logger.Warn("skipping logs collection, previous cycle still running")
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer logsRunning.Unlock()
				if err := logsCollector.Collect(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						logger.Debug("logs collection cancelled")
					} else {
						logger.Error("logs collection failed", "error", err)
					}
				}
			}()

		case <-discoveryCh:
			if limited, wait := client.IsRateLimited(); limited {
				logger.Warn("skipping discovery refresh, API rate limited", "wait", wait.Round(time.Second))
				continue
			}
			if !discoveryRunning.TryLock() {
				logger.Warn("skipping discovery refresh, previous cycle still running")
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer discoveryRunning.Unlock()
				if err := discovery.Refresh(ctx); err != nil {
					if errors.Is(err, context.Canceled) {
						logger.Debug("discovery refresh cancelled")
					} else {
						logger.Error("discovery refresh failed", "error", err)
					}
					return
				}
				// Signal the main loop to recalculate budget
				select {
				case discoveryDone <- struct{}{}:
				default:
				}
			}()

		case <-discoveryDone:
			newBudget := calculateBudget(discovery.Targets(), len(workspaces), cfg, logger)
			if metricsTicker != nil && significantChange(currentMetricsInterval, newBudget.metricsInterval) {
				logger.Info("adjusted metrics interval after discovery",
					"old", currentMetricsInterval, "new", newBudget.metricsInterval)
				currentMetricsInterval = newBudget.metricsInterval
				metricsTicker.Reset(currentMetricsInterval)
			}
			if logsTicker != nil && significantChange(currentLogsInterval, newBudget.logsInterval) {
				logger.Info("adjusted logs interval after discovery",
					"old", currentLogsInterval, "new", newBudget.logsInterval)
				currentLogsInterval = newBudget.logsInterval
				logsTicker.Reset(currentLogsInterval)
			}
		}
	}
}
