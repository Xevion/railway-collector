package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
	"github.com/xevion/railway-collector/internal/state"
)

type LogsCollector struct {
	client    *railway.Client
	discovery *Discovery
	sinks     []sink.Sink
	types     map[string]bool
	limit     int
	logger    *slog.Logger
	store     *state.Store
}

func NewLogsCollector(
	client *railway.Client,
	discovery *Discovery,
	sinks []sink.Sink,
	types []string,
	limit int,
	store *state.Store,
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
		store:     store,
		logger:    logger,
	}
}

func (lc *LogsCollector) Collect(ctx context.Context) error {
	targets := lc.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	var allLogs []sink.LogEntry
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
			g, ok := envGroups[target.EnvironmentID]
			if !ok {
				g = &envGroup{
					environmentID:   target.EnvironmentID,
					environmentName: target.EnvironmentName,
					services:        make(map[string]ServiceTarget),
				}
				envGroups[target.EnvironmentID] = g
			}
			g.services[target.ServiceID] = target
		}

		for _, g := range envGroups {
			logs, err := lc.collectEnvironmentLogs(ctx, g.environmentID, &limit, g.services)
			if err != nil {
				lc.logger.Error("failed to collect environment logs",
					"environment", g.environmentID, "error", err)
			} else {
				allLogs = append(allLogs, logs...)
			}
		}
	}

	// Phase 2: Build and HTTP logs — still per-deployment (no batch alternative).
	for _, target := range targets {
		if target.DeploymentID == "" {
			continue
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

		if lc.types["build"] {
			startDate := lc.getLastSeen(target.DeploymentID, "build")
			var startDateStr *string
			if !startDate.IsZero() {
				s := startDate.Format(time.RFC3339Nano)
				startDateStr = &s
			}
			logs, err := lc.collectBuildLogs(ctx, target.DeploymentID, &limit, startDateStr, baseLabels)
			if err != nil {
				lc.logger.Debug("failed to collect build logs", "deployment", target.DeploymentID, "error", err)
			} else {
				allLogs = append(allLogs, logs...)
			}
		}

		if lc.types["http"] {
			startDate := lc.getLastSeen(target.DeploymentID, "http")
			var startDateStr *string
			if !startDate.IsZero() {
				s := startDate.Format(time.RFC3339Nano)
				startDateStr = &s
			}
			logs, err := lc.collectHttpLogs(ctx, target.DeploymentID, &limit, startDateStr, baseLabels)
			if err != nil {
				lc.logger.Debug("failed to collect HTTP logs", "deployment", target.DeploymentID, "error", err)
			} else {
				allLogs = append(allLogs, logs...)
			}
		}
	}

	if len(allLogs) == 0 {
		return nil
	}

	lc.logger.Debug("collected log entries", "count", len(allLogs))

	for _, s := range lc.sinks {
		if err := s.WriteLogs(ctx, allLogs); err != nil {
			lc.logger.Error("failed to write logs to sink", "sink", s.Name(), "error", err)
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
