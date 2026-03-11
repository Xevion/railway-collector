package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/logging"
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
	Discovery       TargetProvider
	Store           StateStore
	Sinks           []sink.Sink
	Clock           clockwork.Clock
	Types           []string      // enabled log types: "deployment", "build", "http"
	Limit           int           // max logs per request
	Interval        time.Duration // minimum time between polls
	LogRetention    time.Duration // how far back to scan for env log gaps (e.g. 5 days)
	ChunkSize       time.Duration // chunk size for older gaps (e.g. 6 hours)
	MaxItemsPerPoll int           // max work items to emit per poll
	Logger          *slog.Logger
}

// LogsGenerator implements TaskGenerator for log collection.
// All log types (environment, build, HTTP) use coverage-driven gap filling.
type LogsGenerator struct {
	discovery       TargetProvider
	store           StateStore
	sinks           []sink.Sink
	clock           clockwork.Clock
	types           map[string]bool
	limit           int
	interval        time.Duration
	logRetention    time.Duration
	chunkSize       time.Duration
	maxItemsPerPoll int
	logger          *slog.Logger

	nextPoll time.Time
}

// NewLogsGenerator creates a LogsGenerator.
func NewLogsGenerator(cfg LogsGeneratorConfig) *LogsGenerator {
	typeSet := make(map[string]bool, len(cfg.Types))
	for _, t := range cfg.Types {
		typeSet[t] = true
	}
	if cfg.LogRetention == 0 {
		cfg.LogRetention = 5 * 24 * time.Hour
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 6 * time.Hour
	}
	if cfg.MaxItemsPerPoll == 0 {
		cfg.MaxItemsPerPoll = 10
	}

	return &LogsGenerator{
		discovery:       cfg.Discovery,
		store:           cfg.Store,
		sinks:           cfg.Sinks,
		clock:           cfg.Clock,
		types:           typeSet,
		limit:           cfg.Limit,
		interval:        cfg.Interval,
		logRetention:    cfg.LogRetention,
		chunkSize:       cfg.ChunkSize,
		maxItemsPerPoll: cfg.MaxItemsPerPoll,
		logger:          cfg.Logger,
	}
}

// Type returns TaskTypeLogs.
func (g *LogsGenerator) Type() TaskType {
	return TaskTypeLogs
}

// NextPoll returns the earliest time this generator will produce work.
func (g *LogsGenerator) NextPoll() time.Time { return g.nextPoll }

// Poll returns WorkItems for each log type that needs fetching.
// All log types use coverage-driven gap filling.
func (g *LogsGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	var items []WorkItem
	itemCount := 0

	// Environment logs: coverage-driven gap filling per unique environment
	if g.types["deployment"] {
		retentionStart := now.Add(-g.logRetention)
		seen := make(map[string]bool)
		for _, t := range targets {
			if seen[t.EnvironmentID] || itemCount >= g.maxItemsPerPoll {
				continue
			}
			seen[t.EnvironmentID] = true

			coverageKey := CoverageKey(t.EnvironmentID, CoverageTypeLogEnv)
			existing, err := LoadCoverage(g.store, coverageKey)
			if err != nil {
				g.logger.Warn("failed to load env log coverage",
					"environment_id", t.EnvironmentID, "error", err)
				continue
			}

			gaps := FindGaps(existing, retentionStart, now)
			if len(gaps) == 0 {
				continue
			}

			var totalGapDuration time.Duration
			for _, gap := range gaps {
				totalGapDuration += gap.End.Sub(gap.Start)
			}
			g.logger.Debug("env log coverage gaps found",
				"environment_id", t.EnvironmentID,
				"gaps", len(gaps),
				"total_gap_duration", totalGapDuration,
				"oldest_gap", gaps[0].Start.Format(time.RFC3339),
			)

			prioritized := PrioritizeGaps(gaps, now)

			for _, gap := range prioritized {
				if itemCount >= g.maxItemsPerPoll {
					break
				}

				isLiveEdge := now.Sub(gap.End) < time.Minute

				if isLiveEdge {
					// If the gap is larger than one chunk, split the older
					// portion into fixed chunks and only keep the tail as live edge.
					gapDuration := gap.End.Sub(gap.Start)
					if gapDuration > g.chunkSize {
						olderGap := TimeRange{Start: gap.Start, End: gap.End.Add(-g.chunkSize)}
						chunks := alignedChunks(olderGap, g.chunkSize)
						for _, chunk := range chunks {
							if itemCount >= g.maxItemsPerPoll {
								break
							}
							items = append(items, WorkItem{
								ID:       fmt.Sprintf("envlogs:%s:%s", t.EnvironmentID, chunk.Start.Format(time.RFC3339)),
								Kind:     QueryEnvironmentLogs,
								TaskType: TaskTypeLogs,
								AliasKey: t.EnvironmentID,
								BatchKey: fmt.Sprintf("limit=%d,s=%s,e=%s",
									g.limit, chunk.Start.Format(time.RFC3339), chunk.End.Format(time.RFC3339)),
								Params: map[string]any{
									"afterDate":  chunk.Start.Format(time.RFC3339Nano),
									"beforeDate": chunk.End.Format(time.RFC3339Nano),
									"afterLimit": g.limit,
								},
							})
							itemCount++
						}
						gap = TimeRange{Start: gap.End.Add(-g.chunkSize), End: gap.End}
					}

					if itemCount >= g.maxItemsPerPoll {
						break
					}

					// Open-ended query for live edge
					params := map[string]any{
						"afterLimit": g.limit,
						"afterDate":  gap.Start.Format(time.RFC3339Nano),
					}
					items = append(items, WorkItem{
						ID:       fmt.Sprintf("envlogs:%s:%s", t.EnvironmentID, gap.Start.Format(time.RFC3339)),
						Kind:     QueryEnvironmentLogs,
						TaskType: TaskTypeLogs,
						AliasKey: t.EnvironmentID,
						BatchKey: fmt.Sprintf("limit=%d", g.limit),
						Params:   params,
					})
					itemCount++
				} else {
					// Older gap: chunk for batching
					chunks := alignedChunks(gap, g.chunkSize)
					for _, chunk := range chunks {
						if itemCount >= g.maxItemsPerPoll {
							break
						}
						items = append(items, WorkItem{
							ID:       fmt.Sprintf("envlogs:%s:%s", t.EnvironmentID, chunk.Start.Format(time.RFC3339)),
							Kind:     QueryEnvironmentLogs,
							TaskType: TaskTypeLogs,
							AliasKey: t.EnvironmentID,
							BatchKey: fmt.Sprintf("limit=%d,s=%s,e=%s",
								g.limit, chunk.Start.Format(time.RFC3339), chunk.End.Format(time.RFC3339)),
							Params: map[string]any{
								"afterDate":  chunk.Start.Format(time.RFC3339Nano),
								"beforeDate": chunk.End.Format(time.RFC3339Nano),
								"afterLimit": g.limit,
							},
						})
						itemCount++
					}
				}
			}
		}
	}

	// Build logs: coverage-driven gap filling (per deployment)
	if g.types["build"] {
		retentionStart := now.Add(-g.logRetention)
		for _, t := range targets {
			if t.DeploymentID == "" || itemCount >= g.maxItemsPerPoll {
				continue
			}

			coverageKey := CoverageKey(t.DeploymentID, CoverageTypeLogBuild)
			existing, err := LoadCoverage(g.store, coverageKey)
			if err != nil {
				g.logger.Warn("failed to load build log coverage",
					"deployment_id", t.DeploymentID, "error", err)
				continue
			}

			gaps := FindGaps(existing, retentionStart, now)
			if len(gaps) == 0 {
				continue
			}

			var totalGapDuration time.Duration
			for _, gap := range gaps {
				totalGapDuration += gap.End.Sub(gap.Start)
			}
			g.logger.Debug("build log coverage gaps found",
				"deployment_id", t.DeploymentID,
				"gaps", len(gaps),
				"total_gap_duration", totalGapDuration,
				"oldest_gap", gaps[0].Start.Format(time.RFC3339),
			)

			prioritized := PrioritizeGaps(gaps, now)
			for _, gap := range prioritized {
				if itemCount >= g.maxItemsPerPoll {
					break
				}
				items = append(items, WorkItem{
					ID:       fmt.Sprintf("buildlogs:%s:%s", t.DeploymentID, gap.Start.Format(time.RFC3339)),
					Kind:     QueryBuildLogs,
					TaskType: TaskTypeLogs,
					AliasKey: t.DeploymentID,
					BatchKey: fmt.Sprintf("limit=%d", g.limit),
					Params: map[string]any{
						"limit":     g.limit,
						"startDate": gap.Start.Format(time.RFC3339Nano),
					},
				})
				itemCount++
			}
		}
	}

	// HTTP logs: coverage-driven gap filling (per deployment)
	if g.types["http"] {
		retentionStart := now.Add(-g.logRetention)
		for _, t := range targets {
			if t.DeploymentID == "" || itemCount >= g.maxItemsPerPoll {
				continue
			}

			coverageKey := CoverageKey(t.DeploymentID, CoverageTypeLogHTTP)
			existing, err := LoadCoverage(g.store, coverageKey)
			if err != nil {
				g.logger.Warn("failed to load HTTP log coverage",
					"deployment_id", t.DeploymentID, "error", err)
				continue
			}

			gaps := FindGaps(existing, retentionStart, now)
			if len(gaps) == 0 {
				continue
			}

			var totalGapDuration time.Duration
			for _, gap := range gaps {
				totalGapDuration += gap.End.Sub(gap.Start)
			}
			g.logger.Debug("HTTP log coverage gaps found",
				"deployment_id", t.DeploymentID,
				"gaps", len(gaps),
				"total_gap_duration", totalGapDuration,
				"oldest_gap", gaps[0].Start.Format(time.RFC3339),
			)

			prioritized := PrioritizeGaps(gaps, now)
			for _, gap := range prioritized {
				if itemCount >= g.maxItemsPerPoll {
					break
				}
				items = append(items, WorkItem{
					ID:       fmt.Sprintf("httplogs:%s:%s", t.DeploymentID, gap.Start.Format(time.RFC3339)),
					Kind:     QueryHttpLogs,
					TaskType: TaskTypeLogs,
					AliasKey: t.DeploymentID,
					BatchKey: fmt.Sprintf("limit=%d", g.limit),
					Params: map[string]any{
						"limit":     g.limit,
						"startDate": gap.Start.Format(time.RFC3339Nano),
					},
				})
				itemCount++
			}
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
	now := g.clock.Now().UTC()
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

		g.logger.Log(ctx, logging.LevelTrace, "log entry",
			"log_type", "deployment",
			"environment_id", envID,
			"timestamp", ts.Format(time.RFC3339Nano),
		)

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	// Record coverage using params (afterDate/beforeDate) as boundaries
	afterDateStr, _ := item.Params["afterDate"].(string)
	covStart, _ := time.Parse(time.RFC3339Nano, afterDateStr)

	// End time: use beforeDate if present (chunked gap), otherwise use now or maxTS
	covEnd := now
	if beforeDateStr, ok := item.Params["beforeDate"].(string); ok {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, beforeDateStr); parseErr == nil {
			covEnd = parsed
		}
	} else if !maxTS.IsZero() {
		covEnd = maxTS
	}

	if !covStart.IsZero() {
		covKey := CoverageKey(envID, CoverageTypeLogEnv)
		if covErr := updateCoverage(g.store, covKey, covStart, covEnd, len(entries) == 0, 0); covErr != nil {
			g.logger.Warn("failed to update log coverage",
				"environment", envName, "environment_id", envID, "error", covErr)
		}
	}

	level := deliveryLogLevel(len(entries))
	g.logger.Log(ctx, level, "environment logs delivered",
		"environment", envName, "environment_id", envID, "entries", len(entries),
		"start", afterDateStr, "end", covEnd.Format(time.RFC3339Nano))

	writeLogsToSinks(ctx, g.sinks, entries, g.logger)
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

		g.logger.Log(ctx, logging.LevelTrace, "log entry",
			"log_type", "build",
			"deployment_id", deploymentID,
			"timestamp", ts.Format(time.RFC3339Nano),
		)

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	// Record coverage using params (startDate) as boundaries
	startDateStr, _ := item.Params["startDate"].(string)
	covStart, _ := time.Parse(time.RFC3339Nano, startDateStr)

	now := g.clock.Now().UTC()
	covEnd := now
	if !maxTS.IsZero() {
		covEnd = maxTS
	}

	if !covStart.IsZero() {
		covKey := CoverageKey(deploymentID, CoverageTypeLogBuild)
		if covErr := updateCoverage(g.store, covKey, covStart, covEnd, len(entries) == 0, 0); covErr != nil {
			g.logger.Warn("failed to update build log coverage",
				"deployment_id", deploymentID, "error", covErr)
		}
	}

	level := deliveryLogLevel(len(entries))
	g.logger.Log(ctx, level, "build logs delivered", "deployment_id", deploymentID, "entries", len(entries),
		"start", startDateStr, "end", covEnd.Format(time.RFC3339Nano))

	writeLogsToSinks(ctx, g.sinks, entries, g.logger)
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

		g.logger.Log(ctx, logging.LevelTrace, "log entry",
			"log_type", "http",
			"deployment_id", deploymentID,
			"method", log.Method,
			"status", log.HttpStatus,
			"timestamp", ts.Format(time.RFC3339Nano),
		)

		if ts.After(maxTS) {
			maxTS = ts
		}
	}

	// Record coverage using params (startDate) as boundaries
	startDateStr, _ := item.Params["startDate"].(string)
	covStart, _ := time.Parse(time.RFC3339Nano, startDateStr)

	now := g.clock.Now().UTC()
	covEnd := now
	if !maxTS.IsZero() {
		covEnd = maxTS
	}

	if !covStart.IsZero() {
		covKey := CoverageKey(deploymentID, CoverageTypeLogHTTP)
		if covErr := updateCoverage(g.store, covKey, covStart, covEnd, len(entries) == 0, 0); covErr != nil {
			g.logger.Warn("failed to update HTTP log coverage",
				"deployment_id", deploymentID, "error", covErr)
		}
	}

	level := deliveryLogLevel(len(entries))
	g.logger.Log(ctx, level, "HTTP logs delivered", "deployment_id", deploymentID, "entries", len(entries),
		"start", startDateStr, "end", covEnd.Format(time.RFC3339Nano))

	writeLogsToSinks(ctx, g.sinks, entries, g.logger)
}
