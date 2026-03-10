package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/sink"
)

// Raw JSON response types for log queries.

type rawLogEntry struct {
	Timestamp  string            `json:"timestamp"`
	Message    string            `json:"message"`
	Severity   *string           `json:"severity"`
	Attributes []rawLogAttribute `json:"attributes"`
	Tags       *rawLogTags       `json:"tags"`
}

type rawLogAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type rawLogTags struct {
	ServiceID            *string `json:"serviceId"`
	ProjectID            *string `json:"projectId"`
	EnvironmentID        *string `json:"environmentId"`
	DeploymentID         *string `json:"deploymentId"`
	DeploymentInstanceID *string `json:"deploymentInstanceId"`
}

type rawHttpLogEntry struct {
	Timestamp          string  `json:"timestamp"`
	RequestID          *string `json:"requestId"`
	Method             string  `json:"method"`
	Path               string  `json:"path"`
	Host               string  `json:"host"`
	HttpStatus         int     `json:"httpStatus"`
	TotalDuration      int     `json:"totalDuration"`
	UpstreamRqDuration int     `json:"upstreamRqDuration"`
	SrcIp              string  `json:"srcIp"`
	ClientUa           string  `json:"clientUa"`
	RxBytes            int     `json:"rxBytes"`
	TxBytes            int     `json:"txBytes"`
	EdgeRegion         string  `json:"edgeRegion"`
	DeploymentID       *string `json:"deploymentId"`
}

// LogsGeneratorConfig configures a LogsGenerator.
type LogsGeneratorConfig struct {
	Discovery TargetProvider
	Store     StateStore
	Sinks     []sink.Sink
	Clock     clockwork.Clock
	Types     []string      // enabled log types: "deployment", "build", "http"
	Limit     int           // max logs per request
	Interval  time.Duration // minimum time between polls
	Logger    *slog.Logger
}

// LogsGenerator implements TaskGenerator for log collection.
// It emits WorkItems for environment logs, build logs, and HTTP logs.
type LogsGenerator struct {
	discovery TargetProvider
	store     StateStore
	sinks     []sink.Sink
	clock     clockwork.Clock
	types     map[string]bool
	limit     int
	interval  time.Duration
	logger    *slog.Logger

	nextPoll time.Time
}

// NewLogsGenerator creates a LogsGenerator.
func NewLogsGenerator(cfg LogsGeneratorConfig) *LogsGenerator {
	typeSet := make(map[string]bool, len(cfg.Types))
	for _, t := range cfg.Types {
		typeSet[t] = true
	}

	return &LogsGenerator{
		discovery: cfg.Discovery,
		store:     cfg.Store,
		sinks:     cfg.Sinks,
		clock:     cfg.Clock,
		types:     typeSet,
		limit:     cfg.Limit,
		interval:  cfg.Interval,
		logger:    cfg.Logger,
	}
}

// Type returns TaskTypeLogs.
func (g *LogsGenerator) Type() TaskType {
	return TaskTypeLogs
}

// NextPoll returns the earliest time this generator will produce work.
func (g *LogsGenerator) NextPoll() time.Time { return g.nextPoll }

// Poll returns WorkItems for each log type that needs fetching.
// Environment logs: one item per unique environment.
// Build/HTTP logs: one item per deployment with that type enabled.
func (g *LogsGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	var items []WorkItem

	// Environment logs: one per unique environment
	if g.types["deployment"] {
		seen := make(map[string]bool)
		for _, t := range targets {
			if seen[t.EnvironmentID] {
				continue
			}
			seen[t.EnvironmentID] = true

			cursor := g.store.GetLogCursor(t.EnvironmentID, "environment")
			params := map[string]any{
				"afterLimit": g.limit,
			}
			if !cursor.IsZero() {
				params["afterDate"] = cursor.Format(time.RFC3339Nano)
			}

			items = append(items, WorkItem{
				ID:       "envlogs:" + t.EnvironmentID,
				Kind:     QueryEnvironmentLogs,
				TaskType: TaskTypeLogs,
				AliasKey: t.EnvironmentID,
				BatchKey: fmt.Sprintf("limit=%d", g.limit),
				Params:   params,
			})
		}
	}

	// Build logs: one per deployment
	if g.types["build"] {
		for _, t := range targets {
			if t.DeploymentID == "" {
				continue
			}

			cursor := g.store.GetLogCursor(t.DeploymentID, "build")
			params := map[string]any{
				"limit": g.limit,
			}
			if !cursor.IsZero() {
				params["startDate"] = cursor.Format(time.RFC3339Nano)
			}

			items = append(items, WorkItem{
				ID:       "buildlogs:" + t.DeploymentID,
				Kind:     QueryBuildLogs,
				TaskType: TaskTypeLogs,
				AliasKey: t.DeploymentID,
				BatchKey: fmt.Sprintf("limit=%d", g.limit),
				Params:   params,
			})
		}
	}

	// HTTP logs: one per deployment
	if g.types["http"] {
		for _, t := range targets {
			if t.DeploymentID == "" {
				continue
			}

			cursor := g.store.GetLogCursor(t.DeploymentID, "http")
			params := map[string]any{
				"limit": g.limit,
			}
			if !cursor.IsZero() {
				params["startDate"] = cursor.Format(time.RFC3339Nano)
			}

			items = append(items, WorkItem{
				ID:       "httplogs:" + t.DeploymentID,
				Kind:     QueryHttpLogs,
				TaskType: TaskTypeLogs,
				AliasKey: t.DeploymentID,
				BatchKey: fmt.Sprintf("limit=%d", g.limit),
				Params:   params,
			})
		}
	}

	if len(items) > 0 {
		g.nextPoll = now.Add(g.interval)
	}

	return items
}

// Deliver processes raw log JSON for a single alias.
// Routes to the appropriate handler based on the WorkItem's Kind.
func (g *LogsGenerator) Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error) {
	if err != nil {
		g.logger.Error("log delivery failed",
			"kind", item.Kind, "alias_key", item.AliasKey, "error", err)
		return
	}

	switch item.Kind {
	case QueryEnvironmentLogs:
		g.deliverEnvironmentLogs(ctx, item, data)
	case QueryBuildLogs:
		g.deliverBuildLogs(ctx, item, data)
	case QueryHttpLogs:
		g.deliverHttpLogs(ctx, item, data)
	default:
		g.logger.Error("unknown log work item kind", "kind", item.Kind)
	}
}

func (g *LogsGenerator) deliverEnvironmentLogs(ctx context.Context, item WorkItem, data json.RawMessage) {
	envID := item.AliasKey
	targets := g.discovery.Targets()

	services, envName := environmentServiceLookup(envID, targets)

	var rawLogs []rawLogEntry
	if unmarshalErr := json.Unmarshal(data, &rawLogs); unmarshalErr != nil {
		g.logger.Error("failed to parse environment logs",
			"environment", envName, "environment_id", envID, "error", unmarshalErr)
		return
	}

	var entries []sink.LogEntry
	var maxTS time.Time

	for _, log := range rawLogs {
		ts, parseErr := time.Parse(time.RFC3339Nano, log.Timestamp)
		if parseErr != nil {
			g.logger.Warn("skipping environment log with unparseable timestamp",
				"environment", envName, "environment_id", envID,
				"timestamp", log.Timestamp, "error", parseErr)
			continue
		}

		labels := map[string]string{
			"log_type":       "deployment",
			"environment_id": envID,
		}

		if log.Tags != nil {
			if log.Tags.ServiceID != nil {
				if target, ok := services[*log.Tags.ServiceID]; ok {
					labels["project_id"] = target.ProjectID
					labels["project_name"] = target.ProjectName
					labels["service_id"] = target.ServiceID
					labels["service_name"] = target.ServiceName
					labels["environment_name"] = target.EnvironmentName
					labels["deployment_id"] = target.DeploymentID
				} else {
					labels["service_id"] = *log.Tags.ServiceID
				}
			}
			if log.Tags.DeploymentID != nil {
				labels["deployment_id"] = *log.Tags.DeploymentID
			}
			if log.Tags.DeploymentInstanceID != nil {
				labels["deployment_instance_id"] = *log.Tags.DeploymentInstanceID
			}
			if log.Tags.ProjectID != nil {
				if _, ok := labels["project_id"]; !ok {
					labels["project_id"] = *log.Tags.ProjectID
				}
			}
			if log.Tags.EnvironmentID != nil {
				labels["environment_id"] = *log.Tags.EnvironmentID
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
		if cursorErr := g.store.SetLogCursor(envID, "environment", maxTS); cursorErr != nil {
			g.logger.Error("failed to persist log cursor",
				"environment", envName, "environment_id", envID, "error", cursorErr)
		}

		// Record coverage
		covStart := maxTS.Add(-time.Hour) // fallback
		if afterDate, ok := item.Params["afterDate"].(string); ok {
			if parsed, parseErr := time.Parse(time.RFC3339Nano, afterDate); parseErr == nil {
				covStart = parsed
			}
		}
		coverageKey := CoverageKey(envID, "log", "environment")
		existing, covErr := LoadCoverage(g.store, coverageKey)
		if covErr == nil {
			kind := CoverageCollected
			if len(entries) == 0 {
				kind = CoverageEmpty
			}
			updated := InsertInterval(existing, CoverageInterval{
				Start: covStart, End: maxTS, Kind: kind,
			})
			if saveErr := SaveCoverage(g.store, coverageKey, updated); saveErr != nil {
				g.logger.Warn("failed to save log coverage",
					"environment", envName, "environment_id", envID, "error", saveErr)
			}
		}
	}

	g.logger.Debug("environment logs delivered",
		"environment", envName, "environment_id", envID, "entries", len(entries))

	for _, s := range g.sinks {
		if sinkErr := s.WriteLogs(ctx, entries); sinkErr != nil {
			g.logger.Error("failed to write logs to sink",
				"sink", s.Name(), "environment", envName, "error", sinkErr)
		}
	}
}

func (g *LogsGenerator) deliverBuildLogs(ctx context.Context, item WorkItem, data json.RawMessage) {
	deploymentID := item.AliasKey
	baseLabels := deploymentBaseLabels(deploymentID, g.discovery.Targets())

	var rawLogs []rawLogEntry
	if unmarshalErr := json.Unmarshal(data, &rawLogs); unmarshalErr != nil {
		g.logger.Error("failed to parse build logs",
			"deployment_id", deploymentID, "error", unmarshalErr)
		return
	}

	var entries []sink.LogEntry
	var maxTS time.Time

	for _, log := range rawLogs {
		ts, parseErr := time.Parse(time.RFC3339Nano, log.Timestamp)
		if parseErr != nil {
			g.logger.Warn("skipping build log with unparseable timestamp",
				"deployment_id", deploymentID, "timestamp", log.Timestamp, "error", parseErr)
			continue
		}

		labels := copyLabels(baseLabels)
		labels["log_type"] = "build"

		attrs := make(map[string]string, len(log.Attributes))
		for _, a := range log.Attributes {
			attrs[a.Key] = a.Value
		}

		sev := ""
		if log.Severity != nil {
			sev = *log.Severity
		}

		entries = append(entries, sink.LogEntry{
			Timestamp: ts, Message: log.Message,
			Severity: sev, Labels: labels, Attributes: attrs,
		})

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	if !maxTS.IsZero() {
		if cursorErr := g.store.SetLogCursor(deploymentID, "build", maxTS); cursorErr != nil {
			g.logger.Error("failed to persist build log cursor",
				"deployment_id", deploymentID, "error", cursorErr)
		}

		g.recordDeploymentLogCoverage(deploymentID, "build", item, maxTS)
	}

	g.logger.Debug("build logs delivered", "deployment_id", deploymentID, "entries", len(entries))

	for _, s := range g.sinks {
		if sinkErr := s.WriteLogs(ctx, entries); sinkErr != nil {
			g.logger.Error("failed to write build logs to sink",
				"sink", s.Name(), "deployment_id", deploymentID, "error", sinkErr)
		}
	}
}

func (g *LogsGenerator) deliverHttpLogs(ctx context.Context, item WorkItem, data json.RawMessage) {
	deploymentID := item.AliasKey
	baseLabels := deploymentBaseLabels(deploymentID, g.discovery.Targets())

	var rawLogs []rawHttpLogEntry
	if unmarshalErr := json.Unmarshal(data, &rawLogs); unmarshalErr != nil {
		g.logger.Error("failed to parse HTTP logs",
			"deployment_id", deploymentID, "error", unmarshalErr)
		return
	}

	var entries []sink.LogEntry
	var maxTS time.Time

	for _, log := range rawLogs {
		ts, parseErr := time.Parse(time.RFC3339Nano, log.Timestamp)
		if parseErr != nil {
			g.logger.Warn("skipping HTTP log with unparseable timestamp",
				"deployment_id", deploymentID, "timestamp", log.Timestamp, "error", parseErr)
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
			Timestamp: ts, Message: msg,
			Severity: sev, Labels: labels, Attributes: attrs,
		})

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	if !maxTS.IsZero() {
		if cursorErr := g.store.SetLogCursor(deploymentID, "http", maxTS); cursorErr != nil {
			g.logger.Error("failed to persist HTTP log cursor",
				"deployment_id", deploymentID, "error", cursorErr)
		}

		g.recordDeploymentLogCoverage(deploymentID, "http", item, maxTS)
	}

	g.logger.Debug("HTTP logs delivered", "deployment_id", deploymentID, "entries", len(entries))

	for _, s := range g.sinks {
		if sinkErr := s.WriteLogs(ctx, entries); sinkErr != nil {
			g.logger.Error("failed to write HTTP logs to sink",
				"sink", s.Name(), "deployment_id", deploymentID, "error", sinkErr)
		}
	}
}

// recordDeploymentLogCoverage records coverage for build/HTTP logs.
func (g *LogsGenerator) recordDeploymentLogCoverage(deploymentID, logType string, item WorkItem, maxTS time.Time) {
	var covStart time.Time
	if startDate, ok := item.Params["startDate"].(string); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, startDate); err == nil {
			covStart = parsed
		}
	}
	if covStart.IsZero() {
		covStart = maxTS.Add(-time.Hour)
	}

	coverageKey := CoverageKey(deploymentID, "log", logType)
	existing, covErr := LoadCoverage(g.store, coverageKey)
	if covErr != nil {
		g.logger.Warn("failed to load log coverage",
			"deployment_id", deploymentID, "log_type", logType, "error", covErr)
		return
	}

	updated := InsertInterval(existing, CoverageInterval{
		Start: covStart, End: maxTS, Kind: CoverageCollected,
	})
	if saveErr := SaveCoverage(g.store, coverageKey, updated); saveErr != nil {
		g.logger.Warn("failed to save log coverage",
			"deployment_id", deploymentID, "log_type", logType, "error", saveErr)
	}
}
