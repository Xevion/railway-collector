package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"

	"github.com/xevion/railway-collector/internal/sink"
)

type LogsCollector struct {
	client    RailwayAPI
	discovery TargetProvider
	sinks     []sink.Sink
	types     map[string]bool
	limit     int
	interval  time.Duration
	clock     clockwork.Clock
	firstRun  bool
	logger    *slog.Logger
	store     StateStore
}

func NewLogsCollector(
	client RailwayAPI,
	discovery TargetProvider,
	sinks []sink.Sink,
	types []string,
	limit int,
	interval time.Duration,
	store StateStore,
	clock clockwork.Clock,
	logger *slog.Logger,
) *LogsCollector {
	typeSet := make(map[string]bool, len(types))
	for _, t := range types {
		typeSet[t] = true
	}

	return &LogsCollector{
		client:    client,
		discovery: discovery,
		sinks:     sinks,
		types:     typeSet,
		limit:     limit,
		interval:  interval,
		clock:     clock,
		firstRun:  true,
		store:     store,
		logger:    logger,
	}
}

func (lc *LogsCollector) Collect(ctx context.Context) error {
	targets := lc.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	start := time.Now()

	// Skip jitter on the initial collection after startup
	applyJitter := !lc.firstRun
	if lc.firstRun {
		lc.firstRun = false
	}

	var (
		mu      sync.Mutex
		allLogs []sink.LogEntry
	)
	limit := lc.limit

	// Phase 1: Environment logs — one API call per unique environment instead
	// of one per deployment. This is the main source of API call savings.
	if lc.types["deployment"] {
		// Group targets by environment ID so we can look up labels by service ID.
		type envGroup struct {
			environmentID   string
			environmentName string
			// serviceID -> target for label enrichment from log tags
			services map[string]ServiceTarget
		}
		envGroups := make(map[string]*envGroup)

		for _, target := range targets {
			eg, ok := envGroups[target.EnvironmentID]
			if !ok {
				eg = &envGroup{
					environmentID:   target.EnvironmentID,
					environmentName: target.EnvironmentName,
					services:        make(map[string]ServiceTarget),
				}
				envGroups[target.EnvironmentID] = eg
			}
			eg.services[target.ServiceID] = target
		}

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(4)

		for _, eg := range envGroups {
			g.Go(func() error {
				// Spread API calls across 80% of the interval to avoid thundering herd
				if applyJitter {
					maxJitter := time.Duration(float64(lc.interval) * 0.8)
					if maxJitter > 0 {
						jitter := time.Duration(rand.Int64N(int64(maxJitter)))
						select {
						case <-gCtx.Done():
							return gCtx.Err()
						case <-lc.clock.After(jitter):
						}
					}
				}

				localLimit := limit
				logs, err := lc.collectEnvironmentLogs(gCtx, eg.environmentID, &localLimit, eg.services)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						lc.logger.Debug("environment log collection cancelled",
							"environment", eg.environmentName, "environment_id", eg.environmentID)
					} else {
						lc.logger.Error("failed to collect environment logs",
							"environment", eg.environmentName, "environment_id", eg.environmentID, "error", err)
					}
					return nil
				}
				mu.Lock()
				allLogs = append(allLogs, logs...)
				mu.Unlock()
				return nil
			})
		}

		_ = g.Wait()
	}

	// Phase 2: Build and HTTP logs — still per-deployment (no batch alternative).
	{
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(4)

		for _, target := range targets {
			if target.DeploymentID == "" {
				continue
			}

			g.Go(func() error {
				// Spread API calls across 80% of the interval to avoid thundering herd
				if applyJitter {
					maxJitter := time.Duration(float64(lc.interval) * 0.8)
					if maxJitter > 0 {
						jitter := time.Duration(rand.Int64N(int64(maxJitter)))
						select {
						case <-gCtx.Done():
							return gCtx.Err()
						case <-lc.clock.After(jitter):
						}
					}
				}

				baseLabels := map[string]string{
					"project_id":       target.ProjectID,
					"project_name":     target.ProjectName,
					"service_id":       target.ServiceID,
					"service_name":     target.ServiceName,
					"environment_id":   target.EnvironmentID,
					"environment_name": target.EnvironmentName,
					"deployment_id":    target.DeploymentID,
				}

				localLimit := limit

				if lc.types["build"] {
					startDate := lc.getLastSeen(target.DeploymentID, "build")
					var startDateStr *string
					if !startDate.IsZero() {
						s := startDate.Format(time.RFC3339Nano)
						startDateStr = &s
					}
					logs, err := lc.collectBuildLogs(gCtx, target.DeploymentID, &localLimit, startDateStr, baseLabels)
					if err != nil {
						lc.logger.Warn("failed to collect build logs", "deployment", target.DeploymentID, "error", err)
					} else {
						mu.Lock()
						allLogs = append(allLogs, logs...)
						mu.Unlock()
					}
				}

				if lc.types["http"] {
					startDate := lc.getLastSeen(target.DeploymentID, "http")
					var startDateStr *string
					if !startDate.IsZero() {
						s := startDate.Format(time.RFC3339Nano)
						startDateStr = &s
					}
					logs, err := lc.collectHttpLogs(gCtx, target.DeploymentID, &localLimit, startDateStr, baseLabels)
					if err != nil {
						lc.logger.Warn("failed to collect HTTP logs", "deployment", target.DeploymentID, "error", err)
					} else {
						mu.Lock()
						allLogs = append(allLogs, logs...)
						mu.Unlock()
					}
				}

				return nil
			})
		}

		_ = g.Wait()
	}

	if len(allLogs) == 0 {
		return nil
	}

	var deploymentCount, buildCount, httpCount int
	for _, entry := range allLogs {
		switch entry.Labels["log_type"] {
		case "deployment":
			deploymentCount++
		case "build":
			buildCount++
		case "http":
			httpCount++
		}
	}
	lc.logger.Info("collected log entries",
		"count", len(allLogs),
		"deployment", deploymentCount,
		"build", buildCount,
		"http", httpCount,
		"duration", time.Since(start),
	)

	// Use a fresh bounded context for sink writes if the original was cancelled,
	// so we don't lose data that was already collected.
	sinkCtx := ctx
	if ctx.Err() != nil {
		lc.logger.Info("original context cancelled, flushing collected data with 10s deadline", "entries", len(allLogs))
		var sinkCancel context.CancelFunc
		sinkCtx, sinkCancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer sinkCancel()
	}

	for _, s := range lc.sinks {
		if err := s.WriteLogs(sinkCtx, allLogs); err != nil {
			lc.logger.Error("failed to write logs to sink", "sink", s.Name(), "error", err)
		} else {
			lc.logger.Debug("wrote logs to sink", "sink", s.Name(), "entries", len(allLogs))
		}
	}

	if ctx.Err() != nil {
		if sinkCtx.Err() != nil {
			lc.logger.Warn("sink flush may have been truncated by timeout", "entries", len(allLogs))
		} else {
			lc.logger.Info("sink flush completed successfully during shutdown", "entries", len(allLogs))
		}
	}

	return nil
}

// collectEnvironmentLogs fetches runtime logs for an entire environment in a
// single API call, then enriches each log entry with service/project labels
// by matching the log's ServiceId tag against the known targets.
func (lc *LogsCollector) collectEnvironmentLogs(
	ctx context.Context,
	environmentID string,
	limit *int,
	services map[string]ServiceTarget,
) ([]sink.LogEntry, error) {
	environmentName := ""
	for _, t := range services {
		environmentName = t.EnvironmentName
		break
	}

	startDate := lc.getLastSeen(environmentID, "environment")
	var afterDateStr *string
	if !startDate.IsZero() {
		s := startDate.Format(time.RFC3339Nano)
		afterDateStr = &s
	}

	resp, err := lc.client.GetEnvironmentLogs(ctx, environmentID, nil, afterDateStr, nil, limit, nil, nil)
	if err != nil {
		return nil, err
	}

	var entries []sink.LogEntry
	var maxTS time.Time

	for _, log := range resp.EnvironmentLogs {
		ts, err := time.Parse(time.RFC3339Nano, log.Timestamp)
		if err != nil {
			lc.logger.Warn("skipping environment log with unparseable timestamp",
				"environment", environmentName, "environment_id", environmentID,
				"timestamp", log.Timestamp, "error", err)
			continue
		}

		labels := map[string]string{
			"log_type":       "deployment",
			"environment_id": environmentID,
		}

		// Enrich labels from tags if the log entry has them.
		if log.Tags != nil {
			if log.Tags.ServiceId != nil {
				if target, ok := services[*log.Tags.ServiceId]; ok {
					labels["project_id"] = target.ProjectID
					labels["project_name"] = target.ProjectName
					labels["service_id"] = target.ServiceID
					labels["service_name"] = target.ServiceName
					labels["environment_name"] = target.EnvironmentName
					labels["deployment_id"] = target.DeploymentID
				} else {
					labels["service_id"] = *log.Tags.ServiceId
				}
			}
			if log.Tags.DeploymentId != nil {
				// Prefer the tag's deployment ID over the target's — the log
				// may be from a previous deployment still running.
				labels["deployment_id"] = *log.Tags.DeploymentId
			}
			if log.Tags.DeploymentInstanceId != nil {
				labels["deployment_instance_id"] = *log.Tags.DeploymentInstanceId
			}
			if log.Tags.ProjectId != nil {
				if _, ok := labels["project_id"]; !ok {
					labels["project_id"] = *log.Tags.ProjectId
				}
			}
			if log.Tags.EnvironmentId != nil {
				labels["environment_id"] = *log.Tags.EnvironmentId
			}
		}

		attrs := make(map[string]string, len(log.Attributes))
		for _, a := range log.Attributes {
			attrs[a.Key] = a.Value
		}

		sev := ""
		if log.Severity != nil {
			sev = *log.Severity
		}

		entries = append(entries, sink.LogEntry{
			Timestamp:  ts,
			Message:    log.Message,
			Severity:   sev,
			Labels:     labels,
			Attributes: attrs,
		})

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	if !maxTS.IsZero() {
		lc.setLastSeen(environmentID, "environment", maxTS)

		// Record coverage for this environment's logs
		covStart := startDate
		if covStart.IsZero() {
			covStart = maxTS.Add(-time.Hour)
		}
		coverageKey := CoverageKey(environmentID, "log", "environment")
		existing, covErr := LoadCoverage(lc.store, coverageKey)
		if covErr != nil {
			lc.logger.Warn("failed to load log coverage", "environment", environmentName, "environment_id", environmentID, "error", covErr)
		} else {
			kind := CoverageCollected
			if len(entries) == 0 {
				kind = CoverageEmpty
			}
			updated := InsertInterval(existing, CoverageInterval{
				Start: covStart,
				End:   maxTS,
				Kind:  kind,
			})
			if covErr := SaveCoverage(lc.store, coverageKey, updated); covErr != nil {
				lc.logger.Warn("failed to save log coverage", "environment", environmentName, "environment_id", environmentID, "error", covErr)
			}
		}
	}

	return entries, nil
}

func (lc *LogsCollector) collectBuildLogs(ctx context.Context, deploymentID string, limit *int, startDate *string, baseLabels map[string]string) ([]sink.LogEntry, error) {
	resp, err := lc.client.GetBuildLogs(ctx, deploymentID, limit, startDate, nil, nil)
	if err != nil {
		return nil, err
	}

	var entries []sink.LogEntry
	var maxTS time.Time

	for _, log := range resp.BuildLogs {
		ts, err := time.Parse(time.RFC3339Nano, log.Timestamp)
		if err != nil {
			lc.logger.Warn("skipping build log with unparseable timestamp", "timestamp", log.Timestamp, "error", err)
			continue
		}

		labels := copyLabels(baseLabels)
		labels["log_type"] = "build"

		attrs := make(map[string]string)
		for _, a := range log.Attributes {
			attrs[a.Key] = a.Value
		}

		sev := ""
		if log.Severity != nil {
			sev = *log.Severity
		}

		entries = append(entries, sink.LogEntry{
			Timestamp:  ts,
			Message:    log.Message,
			Severity:   sev,
			Labels:     labels,
			Attributes: attrs,
		})

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	if !maxTS.IsZero() {
		lc.setLastSeen(deploymentID, "build", maxTS)

		// Record coverage for this deployment's build logs
		var covStart time.Time
		if startDate != nil {
			if parsed, err := time.Parse(time.RFC3339Nano, *startDate); err == nil {
				covStart = parsed
			}
		}
		if covStart.IsZero() {
			covStart = maxTS.Add(-time.Hour)
		}
		coverageKey := CoverageKey(deploymentID, "log", "build")
		existing, covErr := LoadCoverage(lc.store, coverageKey)
		if covErr != nil {
			lc.logger.Warn("failed to load log coverage", "deployment", deploymentID, "error", covErr)
		} else {
			kind := CoverageCollected
			if len(entries) == 0 {
				kind = CoverageEmpty
			}
			updated := InsertInterval(existing, CoverageInterval{
				Start: covStart,
				End:   maxTS,
				Kind:  kind,
			})
			if covErr := SaveCoverage(lc.store, coverageKey, updated); covErr != nil {
				lc.logger.Warn("failed to save log coverage", "deployment", deploymentID, "error", covErr)
			}
		}
	}

	return entries, nil
}

func (lc *LogsCollector) collectHttpLogs(ctx context.Context, deploymentID string, limit *int, startDate *string, baseLabels map[string]string) ([]sink.LogEntry, error) {
	resp, err := lc.client.GetHttpLogs(ctx, deploymentID, limit, startDate, nil, nil)
	if err != nil {
		return nil, err
	}

	var entries []sink.LogEntry
	var maxTS time.Time

	for _, log := range resp.HttpLogs {
		ts, err := time.Parse(time.RFC3339Nano, log.Timestamp)
		if err != nil {
			lc.logger.Warn("skipping http log with unparseable timestamp", "timestamp", log.Timestamp, "error", err)
			continue
		}

		labels := copyLabels(baseLabels)
		labels["log_type"] = "http"
		labels["method"] = log.Method
		labels["status"] = fmt.Sprintf("%d", log.HttpStatus)
		labels["edge_region"] = log.EdgeRegion

		attrs := map[string]string{
			"path":                 log.Path,
			"host":                 log.Host,
			"client_ua":            log.ClientUa,
			"src_ip":               log.SrcIp,
			"total_duration_ms":    fmt.Sprintf("%d", log.TotalDuration),
			"upstream_duration_ms": fmt.Sprintf("%d", log.UpstreamRqDuration),
			"rx_bytes":             fmt.Sprintf("%d", log.RxBytes),
			"tx_bytes":             fmt.Sprintf("%d", log.TxBytes),
		}

		msg := fmt.Sprintf("%s %s %d", log.Method, log.Path, log.HttpStatus)
		sev := "info"
		if log.HttpStatus >= 500 {
			sev = "error"
		} else if log.HttpStatus >= 400 {
			sev = "warn"
		}

		entries = append(entries, sink.LogEntry{
			Timestamp:  ts,
			Message:    msg,
			Severity:   sev,
			Labels:     labels,
			Attributes: attrs,
		})

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	if !maxTS.IsZero() {
		lc.setLastSeen(deploymentID, "http", maxTS)

		// Record coverage for this deployment's HTTP logs
		var covStart time.Time
		if startDate != nil {
			if parsed, err := time.Parse(time.RFC3339Nano, *startDate); err == nil {
				covStart = parsed
			}
		}
		if covStart.IsZero() {
			covStart = maxTS.Add(-time.Hour)
		}
		coverageKey := CoverageKey(deploymentID, "log", "http")
		existing, covErr := LoadCoverage(lc.store, coverageKey)
		if covErr != nil {
			lc.logger.Warn("failed to load log coverage", "deployment", deploymentID, "error", covErr)
		} else {
			kind := CoverageCollected
			if len(entries) == 0 {
				kind = CoverageEmpty
			}
			updated := InsertInterval(existing, CoverageInterval{
				Start: covStart,
				End:   maxTS,
				Kind:  kind,
			})
			if covErr := SaveCoverage(lc.store, coverageKey, updated); covErr != nil {
				lc.logger.Warn("failed to save log coverage", "deployment", deploymentID, "error", covErr)
			}
		}
	}

	return entries, nil
}

func (lc *LogsCollector) getLastSeen(key, logType string) time.Time {
	return lc.store.GetLogCursor(key, logType)
}

func (lc *LogsCollector) setLastSeen(key, logType string, ts time.Time) {
	if err := lc.store.SetLogCursor(key, logType, ts); err != nil {
		lc.logger.Error("failed to persist log cursor", "key", key, "type", logType, "error", err)
	}
}

func copyLabels(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
