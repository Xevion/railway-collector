package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
)

// rawMetricsResult mirrors the JSON shape of a single metrics result
// returned by the Railway API. Used for unmarshalling raw query responses.
type rawMetricsResult struct {
	Measurement string           `json:"measurement"`
	Tags        rawMetricsTags   `json:"tags"`
	Values      []rawMetricValue `json:"values"`
}

type rawMetricsTags struct {
	ProjectID            *string `json:"projectId"`
	ServiceID            *string `json:"serviceId"`
	EnvironmentID        *string `json:"environmentId"`
	DeploymentID         *string `json:"deploymentId"`
	DeploymentInstanceID *string `json:"deploymentInstanceId"`
	Region               *string `json:"region"`
}

// buildMetricLabels builds metric labels from raw JSON tags, enriching with
// target names where possible.
func buildMetricLabels(tags rawMetricsTags, targets []ServiceTarget) map[string]string {
	labels := make(map[string]string)

	if tags.ProjectID != nil {
		labels["project_id"] = *tags.ProjectID
	}
	if tags.ServiceID != nil {
		labels["service_id"] = *tags.ServiceID
	}
	if tags.EnvironmentID != nil {
		labels["environment_id"] = *tags.EnvironmentID
	}
	if tags.DeploymentID != nil {
		labels["deployment_id"] = *tags.DeploymentID
	}
	if tags.Region != nil {
		labels["region"] = *tags.Region
	}

	for _, t := range targets {
		if tags.ServiceID != nil && t.ServiceID == *tags.ServiceID &&
			tags.EnvironmentID != nil && t.EnvironmentID == *tags.EnvironmentID {
			labels["project_name"] = t.ProjectName
			labels["service_name"] = t.ServiceName
			labels["environment_name"] = t.EnvironmentName
			break
		}
	}

	return labels
}

type rawMetricValue struct {
	Ts    int     `json:"ts"`
	Value float64 `json:"value"`
}

// MetricsGeneratorConfig configures a MetricsGenerator.
type MetricsGeneratorConfig struct {
	Discovery    TargetProvider
	Store        StateStore
	Sinks        []sink.Sink
	Clock        clockwork.Clock
	Measurements []railway.MetricMeasurement
	SampleRate   int
	AvgWindow    int
	Lookback     time.Duration
	Interval     time.Duration // minimum time between polls (e.g. 30s)
	Logger       *slog.Logger
}

// MetricsGenerator implements TaskGenerator for metrics collection.
// It emits one WorkItem per project on each poll cycle.
type MetricsGenerator struct {
	discovery    TargetProvider
	store        StateStore
	sinks        []sink.Sink
	clock        clockwork.Clock
	measurements []railway.MetricMeasurement
	sampleRate   int
	avgWindow    int
	lookback     time.Duration
	interval     time.Duration
	logger       *slog.Logger

	nextPoll time.Time // earliest time to emit items again
}

// NewMetricsGenerator creates a MetricsGenerator.
func NewMetricsGenerator(cfg MetricsGeneratorConfig) *MetricsGenerator {
	measurements := cfg.Measurements
	if len(measurements) == 0 {
		measurements = []railway.MetricMeasurement{
			railway.MetricMeasurementCpuUsage,
			railway.MetricMeasurementMemoryUsageGb,
			railway.MetricMeasurementNetworkRxGb,
			railway.MetricMeasurementNetworkTxGb,
			railway.MetricMeasurementDiskUsageGb,
		}
	}

	return &MetricsGenerator{
		discovery:    cfg.Discovery,
		store:        cfg.Store,
		sinks:        cfg.Sinks,
		clock:        cfg.Clock,
		measurements: measurements,
		sampleRate:   cfg.SampleRate,
		avgWindow:    cfg.AvgWindow,
		lookback:     cfg.Lookback,
		interval:     cfg.Interval,
		logger:       cfg.Logger,
	}
}

// Type returns TaskTypeMetrics.
func (g *MetricsGenerator) Type() TaskType {
	return TaskTypeMetrics
}

// NextPoll returns the earliest time this generator will produce work.
func (g *MetricsGenerator) NextPoll() time.Time { return g.nextPoll }

// metricsBatchKey computes the batch key from shared query parameters.
// Items with the same batch key can be merged into one aliased request.
func (g *MetricsGenerator) metricsBatchKey() string {
	var parts []string
	for _, m := range g.measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("sr=%d,aw=%d,m=%s", g.sampleRate, g.avgWindow, strings.Join(parts, "+"))
}

// Poll returns one WorkItem per project that needs metrics fetched.
// Returns nil if the poll interval hasn't elapsed or there are no targets.
func (g *MetricsGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	projectIDs := uniqueProjectIDs(targets)
	fallbackStart := now.Add(-g.lookback)
	batchKey := g.metricsBatchKey()

	groupBy := []railway.MetricTag{
		railway.MetricTagServiceId,
		railway.MetricTagEnvironmentId,
		railway.MetricTagDeploymentId,
	}

	var items []WorkItem
	for _, pid := range projectIDs {
		startTime := g.store.GetMetricCursor(pid)
		if startTime.IsZero() || startTime.Before(fallbackStart) {
			startTime = fallbackStart
		}

		items = append(items, WorkItem{
			ID:       "metrics:" + pid,
			Kind:     QueryMetrics,
			TaskType: TaskTypeMetrics,
			AliasKey: pid,
			BatchKey: batchKey,
			Params: map[string]any{
				"startDate":              startTime.Format(time.RFC3339),
				"measurements":           g.measurements,
				"groupBy":                groupBy,
				"sampleRateSeconds":      g.sampleRate,
				"averagingWindowSeconds": g.avgWindow,
			},
		})
	}

	g.nextPoll = now.Add(g.interval)
	return items
}

// Deliver processes the raw metrics JSON response for a single project.
// It transforms the data into MetricPoints, writes to sinks, and updates
// cursors and coverage.
func (g *MetricsGenerator) Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error) {
	projectID := item.AliasKey
	now := g.clock.Now().UTC()
	targets := g.discovery.Targets()

	projectName := ""
	for _, t := range targets {
		if t.ProjectID == projectID {
			projectName = t.ProjectName
			break
		}
	}

	if err != nil {
		g.logger.Error("metrics delivery failed",
			"project", projectName, "project_id", projectID, "error", err)
		return
	}

	var results []rawMetricsResult
	if unmarshalErr := json.Unmarshal(data, &results); unmarshalErr != nil {
		g.logger.Error("failed to parse metrics response",
			"project", projectName, "project_id", projectID, "error", unmarshalErr)
		return
	}

	// Update cursor to current time
	if cursorErr := g.store.SetMetricCursor(projectID, now); cursorErr != nil {
		g.logger.Error("failed to persist metric cursor",
			"project", projectName, "project_id", projectID, "error", cursorErr)
	}

	// Update coverage
	startDateStr, _ := item.Params["startDate"].(string)
	startTime, _ := time.Parse(time.RFC3339, startDateStr)
	if startTime.IsZero() {
		startTime = now.Add(-g.lookback)
	}

	coverageKey := CoverageKey(projectID, "metric")
	existing, covErr := LoadCoverage(g.store, coverageKey)
	if covErr != nil {
		g.logger.Warn("failed to load metric coverage",
			"project", projectName, "project_id", projectID, "error", covErr)
	} else {
		kind := CoverageCollected
		if len(results) == 0 {
			kind = CoverageEmpty
		}
		updated := InsertInterval(existing, CoverageInterval{
			Start:      startTime,
			End:        now,
			Kind:       kind,
			Resolution: g.sampleRate,
		})
		if saveErr := SaveCoverage(g.store, coverageKey, updated); saveErr != nil {
			g.logger.Warn("failed to save metric coverage",
				"project", projectName, "project_id", projectID, "error", saveErr)
		}
	}

	// Transform to MetricPoints
	var points []sink.MetricPoint
	for _, result := range results {
		metricName := measurementToMetricName(result.Measurement)
		labels := g.buildLabelsFromRaw(result.Tags, targets)

		for _, v := range result.Values {
			points = append(points, sink.MetricPoint{
				Name:      metricName,
				Value:     v.Value,
				Timestamp: time.Unix(int64(v.Ts), 0),
				Labels:    labels,
			})
		}
	}

	g.logger.Debug("metrics delivered",
		"project", projectName, "project_id", projectID,
		"series", len(results), "points", len(points),
		"start", startTime.Format(time.RFC3339),
		"end", now.Format(time.RFC3339),
	)

	// Write to sinks
	for _, s := range g.sinks {
		if sinkErr := s.WriteMetrics(ctx, points); sinkErr != nil {
			g.logger.Error("failed to write metrics to sink",
				"sink", s.Name(), "project", projectName, "project_id", projectID, "error", sinkErr)
		}
	}
}

// buildLabelsFromRaw builds metric labels from raw JSON tags,
// enriching with target names where possible.
func (g *MetricsGenerator) buildLabelsFromRaw(tags rawMetricsTags, targets []ServiceTarget) map[string]string {
	return buildMetricLabels(tags, targets)
}

// measurementToMetricName converts a raw measurement string
// (e.g. "CPU_USAGE") to the canonical metric name.
func measurementToMetricName(measurement string) string {
	m := railway.MetricMeasurement(measurement)
	if name, ok := metricNameMap[m]; ok {
		return name
	}
	return fmt.Sprintf("railway_%s", strings.ToLower(measurement))
}
