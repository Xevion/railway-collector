package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/collector/coverage"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/sink"
)

// Raw response types for httpDurationMetrics endpoint.

type rawHttpDurationResponse struct {
	Samples []rawHttpDurationSample `json:"samples"`
}

type rawHttpDurationSample struct {
	Ts  int     `json:"ts"`
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

// Raw response types for httpMetricsGroupedByStatus endpoint.

type rawHttpStatusGroup struct {
	StatusCode int                   `json:"statusCode"`
	Samples    []rawHttpStatusSample `json:"samples"`
}

type rawHttpStatusSample struct {
	Ts    int     `json:"ts"`
	Value float64 `json:"value"`
}

// HttpMetricsGeneratorConfig configures an HttpMetricsGenerator.
type HttpMetricsGeneratorConfig struct {
	Discovery       types.TargetProvider
	Store           types.StateStore
	Sinks           []sink.Sink
	Clock           clockwork.Clock
	Interval        time.Duration // minimum time between polls
	MetricRetention time.Duration // how far back to scan for gaps
	ChunkSize       time.Duration // chunk size for older gaps
	MaxItemsPerPoll int           // max work items to emit per poll
	StepSeconds     int           // step size for HTTP metric queries (default 60)
	Logger          *slog.Logger
}

// HttpMetricsGenerator implements TaskGenerator for HTTP request metrics.
// It emits two WorkItems per service+environment pair: one for duration
// percentiles (httpDurationMetrics) and one for status code counts
// (httpMetricsGroupedByStatus).
type HttpMetricsGenerator struct {
	discovery       types.TargetProvider
	store           types.StateStore
	sinks           []sink.Sink
	clock           clockwork.Clock
	interval        time.Duration
	metricRetention time.Duration
	chunkSize       time.Duration
	maxItemsPerPoll int
	stepSeconds     int
	logger          *slog.Logger

	nextPoll time.Time
}

// NewHttpMetricsGenerator creates an HttpMetricsGenerator.
func NewHttpMetricsGenerator(cfg HttpMetricsGeneratorConfig) *HttpMetricsGenerator {
	if cfg.MetricRetention == 0 {
		cfg.MetricRetention = 90 * 24 * time.Hour
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 6 * time.Hour
	}
	if cfg.MaxItemsPerPoll == 0 {
		cfg.MaxItemsPerPoll = 10
	}
	if cfg.StepSeconds == 0 {
		cfg.StepSeconds = 60
	}

	return &HttpMetricsGenerator{
		discovery:       cfg.Discovery,
		store:           cfg.Store,
		sinks:           cfg.Sinks,
		clock:           cfg.Clock,
		interval:        cfg.Interval,
		metricRetention: cfg.MetricRetention,
		chunkSize:       cfg.ChunkSize,
		maxItemsPerPoll: cfg.MaxItemsPerPoll,
		stepSeconds:     cfg.StepSeconds,
		logger:          cfg.Logger,
	}
}

// Type returns types.TaskTypeMetrics.
func (g *HttpMetricsGenerator) Type() types.TaskType {
	return types.TaskTypeMetrics
}

// NextPoll returns the earliest time this generator will produce work.
func (g *HttpMetricsGenerator) NextPoll() time.Time { return g.nextPoll }

// durationBatchKey returns the batch key for httpDurationMetrics items.
func (g *HttpMetricsGenerator) durationBatchKey(start, end time.Time) string {
	return fmt.Sprintf("httpdur:step=%d,s=%s,e=%s",
		g.stepSeconds, start.Format(time.RFC3339), end.Format(time.RFC3339))
}

// statusBatchKey returns the batch key for httpMetricsGroupedByStatus items.
func (g *HttpMetricsGenerator) statusBatchKey(start, end time.Time) string {
	return fmt.Sprintf("httpstatus:step=%d,s=%s,e=%s",
		g.stepSeconds, start.Format(time.RFC3339), end.Format(time.RFC3339))
}

// durationBatchKeyLive returns the batch key for live-edge duration items (no endDate).
func (g *HttpMetricsGenerator) durationBatchKeyLive() string {
	return fmt.Sprintf("httpdur:step=%d", g.stepSeconds)
}

// statusBatchKeyLive returns the batch key for live-edge status items (no endDate).
func (g *HttpMetricsGenerator) statusBatchKeyLive() string {
	return fmt.Sprintf("httpstatus:step=%d", g.stepSeconds)
}

// Poll scans coverage gaps for all unique service+environment pairs and emits
// two WorkItems per target: one for duration percentiles, one for status counts.
func (g *HttpMetricsGenerator) Poll(now time.Time) []types.WorkItem {
	items := pollCoverageGaps(now, gapPollParams{
		store:           g.store,
		discovery:       g.discovery,
		logger:          g.logger,
		metricRetention: g.metricRetention,
		chunkSize:       g.chunkSize,
		maxItemsPerPoll: g.maxItemsPerPoll,
		nextPoll:        g.nextPoll,
		itemsPerEmit:    2,
		logPrefix:       "http metric",
		entities: func(targets []types.ServiceTarget) []pollEntity {
			svcEnvs := uniqueServiceEnvironments(targets)
			entities := make([]pollEntity, len(svcEnvs))
			for i, t := range svcEnvs {
				entities[i] = pollEntity{
					Key:          t.CompositeKey(),
					CoverageType: coverage.CoverageTypeHTTPMetric,
					LogAttrs:     []any{"service_id", t.ServiceID, "environment_id", t.EnvironmentID},
					Data:         t,
				}
			}
			return entities
		},
		buildItems: func(entity pollEntity, chunk coverage.TimeRange, isLiveEdge bool) []types.WorkItem {
			t := entity.Data.(types.ServiceTarget)

			// HTTP endpoints always require endDate; for live edge use now
			endDate := chunk.End.Format(time.RFC3339)
			durBatchKey := g.durationBatchKey(chunk.Start, chunk.End)
			statusBatchKey := g.statusBatchKey(chunk.Start, chunk.End)
			if isLiveEdge {
				endDate = now.Format(time.RFC3339)
				durBatchKey = g.durationBatchKeyLive()
				statusBatchKey = g.statusBatchKeyLive()
			}

			startDate := chunk.Start.Format(time.RFC3339)
			return []types.WorkItem{
				{
					ID:       fmt.Sprintf("http-duration:%s:%s:%s", t.ServiceID, t.EnvironmentID, startDate),
					Kind:     types.QueryHttpDurationMetrics,
					TaskType: types.TaskTypeMetrics,
					AliasKey: entity.Key,
					BatchKey: durBatchKey,
					Params: map[string]any{
						"serviceId":     t.ServiceID,
						"environmentId": t.EnvironmentID,
						"startDate":     startDate,
						"endDate":       endDate,
						"stepSeconds":   g.stepSeconds,
					},
				},
				{
					ID:       fmt.Sprintf("http-status:%s:%s:%s", t.ServiceID, t.EnvironmentID, startDate),
					Kind:     types.QueryHttpMetricsGroupedByStatus,
					TaskType: types.TaskTypeMetrics,
					AliasKey: entity.Key,
					BatchKey: statusBatchKey,
					Params: map[string]any{
						"serviceId":     t.ServiceID,
						"environmentId": t.EnvironmentID,
						"startDate":     startDate,
						"endDate":       endDate,
						"stepSeconds":   g.stepSeconds,
					},
				},
			}
		},
	})

	if len(items) > 0 {
		g.nextPoll = now.Add(g.interval)
	}

	return items
}

// Deliver processes the raw JSON response for an HTTP metrics work item.
// It routes by item.Kind to handle the two different response shapes.
func (g *HttpMetricsGenerator) Deliver(ctx context.Context, item types.WorkItem, data json.RawMessage, err error) {
	now := g.clock.Now().UTC()
	targets := g.discovery.Targets()
	info := parseCompositeKey(item.AliasKey, targets)
	serviceID := info.serviceID
	environmentID := info.environmentID
	serviceName := info.serviceName
	environmentName := info.environmentName
	projectName := info.projectName
	projectID := info.projectID

	if err != nil {
		g.logger.Error("http metrics delivery failed",
			"kind", string(item.Kind),
			"service", serviceName, "service_id", serviceID,
			"environment", environmentName, "environment_id", environmentID,
			"error", err)
		return
	}

	switch item.Kind {
	case types.QueryHttpDurationMetrics:
		g.deliverDuration(ctx, item, data, serviceID, environmentID, serviceName, environmentName, projectName, projectID, now)
	case types.QueryHttpMetricsGroupedByStatus:
		g.deliverStatus(ctx, item, data, serviceID, environmentID, serviceName, environmentName, projectName, projectID, now)
	default:
		g.logger.Error("unknown http metrics query kind",
			"kind", string(item.Kind),
			"service_id", serviceID, "environment_id", environmentID)
	}
}

// deliverDuration handles types.QueryHttpDurationMetrics responses.
func (g *HttpMetricsGenerator) deliverDuration(
	ctx context.Context, item types.WorkItem, data json.RawMessage,
	serviceID, environmentID, serviceName, environmentName, projectName, projectID string,
	now time.Time,
) {
	var resp rawHttpDurationResponse
	if unmarshalErr := json.Unmarshal(data, &resp); unmarshalErr != nil {
		g.logger.Error("failed to parse http duration metrics response",
			"service", serviceName, "service_id", serviceID,
			"environment", environmentName, "environment_id", environmentID,
			"error", unmarshalErr)
		return
	}

	// Determine coverage interval
	startDateStr, _ := item.Params["startDate"].(string)
	startTime, _ := time.Parse(time.RFC3339, startDateStr)

	endTime := now
	if endDateStr, ok := item.Params["endDate"].(string); ok {
		if parsed, parseErr := time.Parse(time.RFC3339, endDateStr); parseErr == nil {
			endTime = parsed
		}
	}

	// Update coverage (duration is the primary coverage updater for both kinds)
	compositeKey := serviceID + ":" + environmentID
	if !startTime.IsZero() {
		covKey := coverage.CoverageKey(compositeKey, coverage.CoverageTypeHTTPMetric)
		if covErr := updateCoverage(g.store, covKey, startTime, endTime, len(resp.Samples) == 0, g.stepSeconds); covErr != nil {
			g.logger.Warn("failed to update http metric coverage",
				"service", serviceName, "service_id", serviceID,
				"environment", environmentName, "environment_id", environmentID,
				"error", covErr)
		}
	}

	// Build base labels using already-resolved names from Deliver
	baseLabels := map[string]string{
		"project_id":       projectID,
		"project_name":     projectName,
		"service_id":       serviceID,
		"service_name":     serviceName,
		"environment_id":   environmentID,
		"environment_name": environmentName,
	}

	// Transform to MetricPoints: 4 per sample (p50, p90, p95, p99)
	var points []sink.MetricPoint
	for _, s := range resp.Samples {
		ts := time.Unix(int64(s.Ts), 0)

		for _, m := range []struct {
			name  string
			value float64
		}{
			{"railway_http_duration_p50", s.P50},
			{"railway_http_duration_p90", s.P90},
			{"railway_http_duration_p95", s.P95},
			{"railway_http_duration_p99", s.P99},
		} {
			labels := copyLabels(baseLabels)
			points = append(points, sink.MetricPoint{
				Name:      m.name,
				Value:     m.value,
				Timestamp: ts,
				Labels:    labels,
			})
		}
	}

	level := deliveryLogLevel(len(points))
	g.logger.Log(ctx, level, "http duration metrics delivered",
		"project", projectName,
		"service", serviceName, "service_id", serviceID,
		"environment", environmentName, "environment_id", environmentID,
		"samples", len(resp.Samples), "points", len(points),
		"start", startDateStr, "end", endTime.Format(time.RFC3339),
	)

	writeMetricsToSinks(ctx, g.sinks, points, g.logger)
}

// deliverStatus handles types.QueryHttpMetricsGroupedByStatus responses.
func (g *HttpMetricsGenerator) deliverStatus(
	ctx context.Context, item types.WorkItem, data json.RawMessage,
	serviceID, environmentID, serviceName, environmentName, projectName, projectID string,
	now time.Time,
) {
	var groups []rawHttpStatusGroup
	if unmarshalErr := json.Unmarshal(data, &groups); unmarshalErr != nil {
		g.logger.Error("failed to parse http status metrics response",
			"service", serviceName, "service_id", serviceID,
			"environment", environmentName, "environment_id", environmentID,
			"error", unmarshalErr)
		return
	}

	startDateStr, _ := item.Params["startDate"].(string)
	endTime := now
	if endDateStr, ok := item.Params["endDate"].(string); ok {
		if parsed, parseErr := time.Parse(time.RFC3339, endDateStr); parseErr == nil {
			endTime = parsed
		}
	}

	// Build base labels using already-resolved names from Deliver
	baseLabels := map[string]string{
		"project_id":       projectID,
		"project_name":     projectName,
		"service_id":       serviceID,
		"service_name":     serviceName,
		"environment_id":   environmentID,
		"environment_name": environmentName,
	}

	// Transform to MetricPoints: 1 per sample per status group
	var points []sink.MetricPoint
	for _, group := range groups {
		statusCodeStr := fmt.Sprintf("%d", group.StatusCode)
		for _, s := range group.Samples {
			labels := copyLabels(baseLabels)
			labels["status_code"] = statusCodeStr
			points = append(points, sink.MetricPoint{
				Name:      "railway_http_requests",
				Value:     s.Value,
				Timestamp: time.Unix(int64(s.Ts), 0),
				Labels:    labels,
			})
		}
	}

	level := deliveryLogLevel(len(points))
	g.logger.Log(ctx, level, "http status metrics delivered",
		"project", projectName,
		"service", serviceName, "service_id", serviceID,
		"environment", environmentName, "environment_id", environmentID,
		"status_groups", len(groups), "points", len(points),
		"start", startDateStr, "end", endTime.Format(time.RFC3339),
	)

	writeMetricsToSinks(ctx, g.sinks, points, g.logger)
}
