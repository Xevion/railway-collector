package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
)

type LogsCollector struct {
	client    *railway.Client
	discovery *Discovery
	sinks     []sink.Sink
	types     map[string]bool
	limit     int
	logger    *slog.Logger

	mu          sync.Mutex
	lastSeenLog map[string]time.Time
}

func NewLogsCollector(
	client *railway.Client,
	discovery *Discovery,
	sinks []sink.Sink,
	types []string,
	limit int,
	logger *slog.Logger,
) *LogsCollector {
	typeSet := make(map[string]bool, len(types))
	for _, t := range types {
		typeSet[t] = true
	}

	return &LogsCollector{
		client:      client,
		discovery:   discovery,
		sinks:       sinks,
		types:       typeSet,
		limit:       limit,
		logger:      logger,
		lastSeenLog: make(map[string]time.Time),
	}
}

func (lc *LogsCollector) Collect(ctx context.Context) error {
	targets := lc.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	var allLogs []sink.LogEntry
	limit := lc.limit

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

		if lc.types["deployment"] {
			startDate := lc.getLastSeen(target.DeploymentID, "deployment")
			var startDateStr *string
			if !startDate.IsZero() {
				s := startDate.Format(time.RFC3339Nano)
				startDateStr = &s
			}
			logs, err := lc.collectDeploymentLogs(ctx, target.DeploymentID, &limit, startDateStr, baseLabels)
			if err != nil {
				lc.logger.Error("failed to collect deployment logs", "deployment", target.DeploymentID, "error", err)
			} else {
				allLogs = append(allLogs, logs...)
			}
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

func (lc *LogsCollector) collectDeploymentLogs(ctx context.Context, deploymentID string, limit *int, startDate *string, baseLabels map[string]string) ([]sink.LogEntry, error) {
	resp, err := lc.client.GetDeploymentLogs(ctx, deploymentID, limit, startDate, nil, nil)
	if err != nil {
		return nil, err
	}

	var entries []sink.LogEntry
	var maxTS time.Time

	for _, log := range resp.DeploymentLogs {
		ts, err := time.Parse(time.RFC3339Nano, log.Timestamp)
		if err != nil {
			lc.logger.Warn("skipping deployment log with unparseable timestamp", "timestamp", log.Timestamp, "error", err)
			continue
		}

		labels := copyLabels(baseLabels)
		labels["log_type"] = "deployment"

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
		lc.setLastSeen(deploymentID, "deployment", maxTS)
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
	// HttpLogs uses string dates, not DateTime
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

func (lc *LogsCollector) getLastSeen(deploymentID, logType string) time.Time {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.lastSeenLog[deploymentID+":"+logType]
}

func (lc *LogsCollector) setLastSeen(deploymentID, logType string, ts time.Time) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	key := deploymentID + ":" + logType
	if existing, ok := lc.lastSeenLog[key]; !ok || ts.After(existing) {
		lc.lastSeenLog[key] = ts
	}
}

func copyLabels(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
