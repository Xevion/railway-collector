package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/logging"
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
	Discovery       TargetProvider
	Store           StateStore
	Sinks           []sink.Sink
	Clock           clockwork.Clock
	Measurements    []railway.MetricMeasurement
	SampleRate      int
	AvgWindow       int
	Interval        time.Duration // minimum time between polls (e.g. 30s)
	MetricRetention time.Duration // how far back to scan for gaps (e.g. 90 days)
	ChunkSize       time.Duration // chunk size for older gaps (e.g. 6 hours)
	MaxItemsPerPoll int           // max work items to emit per poll
	Logger          *slog.Logger
}

// MetricsGenerator implements TaskGenerator for metrics collection.
// It scans coverage gaps each poll, prioritizes by recency (live edge first),
// and emits chunked WorkItems for older gaps and open-ended items for the live edge.
type MetricsGenerator struct {
	discovery       TargetProvider
	store           StateStore
	sinks           []sink.Sink
	clock           clockwork.Clock
	measurements    []railway.MetricMeasurement
	sampleRate      int
	avgWindow       int
	interval        time.Duration
	metricRetention time.Duration
	chunkSize       time.Duration
	maxItemsPerPoll int
	logger          *slog.Logger

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
	if cfg.MetricRetention == 0 {
		cfg.MetricRetention = 90 * 24 * time.Hour
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 6 * time.Hour
	}
	if cfg.MaxItemsPerPoll == 0 {
		cfg.MaxItemsPerPoll = 10
	}

	return &MetricsGenerator{
		discovery:       cfg.Discovery,
		store:           cfg.Store,
		sinks:           cfg.Sinks,
		clock:           cfg.Clock,
		measurements:    measurements,
		sampleRate:      cfg.SampleRate,
		avgWindow:       cfg.AvgWindow,
		interval:        cfg.Interval,
		metricRetention: cfg.MetricRetention,
		chunkSize:       cfg.ChunkSize,
		maxItemsPerPoll: cfg.MaxItemsPerPoll,
		logger:          cfg.Logger,
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

// metricsBatchKeyChunk computes a batch key for a chunked (older gap) query.
// Includes chunk boundaries so that items for the same time window can be merged.
func (g *MetricsGenerator) metricsBatchKeyChunk(chunkStart, chunkEnd time.Time) string {
	var parts []string
	for _, m := range g.measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("sr=%d,aw=%d,m=%s,s=%s,e=%s",
		g.sampleRate, g.avgWindow, strings.Join(parts, "+"),
		chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))
}

// alignToChunkBoundary truncates t down to the nearest chunk boundary.
func alignToChunkBoundary(t time.Time, chunkSize time.Duration) time.Time {
	unix := t.Unix()
	chunkSec := int64(chunkSize.Seconds())
	return time.Unix(unix-unix%chunkSec, 0).UTC()
}

// alignedChunks splits a gap into chunks aligned to fixed boundaries.
func alignedChunks(gap TimeRange, chunkSize time.Duration) []TimeRange {
	alignedStart := alignToChunkBoundary(gap.Start, chunkSize)

	var chunks []TimeRange
	cursor := alignedStart
	for cursor.Before(gap.End) {
		end := cursor.Add(chunkSize)
		if end.After(gap.End) {
			end = gap.End
		}
		chunks = append(chunks, TimeRange{Start: cursor, End: end})
		cursor = cursor.Add(chunkSize)
	}
	return chunks
}

// Poll scans coverage gaps for all projects, prioritizes by recency (live edge
// first), and returns WorkItems. The live edge gets an open-ended query (no
// endDate); older gaps are chunked into fixed intervals for batching.
func (g *MetricsGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	projectIDs := uniqueProjectIDs(targets)
	retentionStart := now.Add(-g.metricRetention)

	groupBy := []railway.MetricTag{
		railway.MetricTagServiceId,
		railway.MetricTagEnvironmentId,
		railway.MetricTagDeploymentId,
	}

	var items []WorkItem
	itemCount := 0

	for _, pid := range projectIDs {
		if itemCount >= g.maxItemsPerPoll {
			break
		}

		coverageKey := CoverageKey(pid, "metric")
		existing, err := LoadCoverage(g.store, coverageKey)
		if err != nil {
			g.logger.Warn("failed to load metric coverage",
				"project_id", pid, "error", err)
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
		g.logger.Debug("metric coverage gaps found",
			"project_id", pid,
			"gaps", len(gaps),
			"total_gap_duration", totalGapDuration,
			"oldest_gap", gaps[0].Start.Format(time.RFC3339),
		)

		prioritized := PrioritizeGaps(gaps, now)

		for _, gap := range prioritized {
			if itemCount >= g.maxItemsPerPoll {
				break
			}

			// Live edge: gap ends at or near "now" (within 1 minute)
			isLiveEdge := now.Sub(gap.End) < time.Minute

			if isLiveEdge {
				// Open-ended query: no endDate, captures data up to "now"
				items = append(items, WorkItem{
					ID:       fmt.Sprintf("metrics:%s:%s", pid, gap.Start.Format(time.RFC3339)),
					Kind:     QueryMetrics,
					TaskType: TaskTypeMetrics,
					AliasKey: pid,
					BatchKey: g.metricsBatchKey(),
					Params: map[string]any{
						"startDate":              gap.Start.Format(time.RFC3339),
						"measurements":           g.measurements,
						"groupBy":                groupBy,
						"sampleRateSeconds":      g.sampleRate,
						"averagingWindowSeconds": g.avgWindow,
					},
				})
				itemCount++
			} else {
				// Older gap: chunk into fixed intervals for batching
				chunks := alignedChunks(gap, g.chunkSize)
				for _, chunk := range chunks {
					if itemCount >= g.maxItemsPerPoll {
						break
					}

					items = append(items, WorkItem{
						ID:       fmt.Sprintf("metrics:%s:%s", pid, chunk.Start.Format(time.RFC3339)),
						Kind:     QueryMetrics,
						TaskType: TaskTypeMetrics,
						AliasKey: pid,
						BatchKey: g.metricsBatchKeyChunk(chunk.Start, chunk.End),
						Params: map[string]any{
							"startDate":              chunk.Start.Format(time.RFC3339),
							"endDate":                chunk.End.Format(time.RFC3339),
							"measurements":           g.measurements,
							"groupBy":                groupBy,
							"sampleRateSeconds":      g.sampleRate,
							"averagingWindowSeconds": g.avgWindow,
						},
					})
					itemCount++
				}
			}
		}
	}

	if len(items) > 0 {
		g.nextPoll = now.Add(g.interval)
	}

	return items
}

// Deliver processes the raw metrics JSON response for a single project.
// It transforms the data into MetricPoints, writes to sinks, and updates
// coverage. Coverage is the single source of truth (no cursors).
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

	// Determine coverage interval from params
	startDateStr, _ := item.Params["startDate"].(string)
	startTime, _ := time.Parse(time.RFC3339, startDateStr)

	// End time: use endDate if present (chunked gap), otherwise use now (live edge)
	endTime := now
	if endDateStr, ok := item.Params["endDate"].(string); ok {
		if parsed, parseErr := time.Parse(time.RFC3339, endDateStr); parseErr == nil {
			endTime = parsed
		}
	}

	// Update coverage
	if !startTime.IsZero() {
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
				End:        endTime,
				Kind:       kind,
				Resolution: g.sampleRate,
			})
			if saveErr := SaveCoverage(g.store, coverageKey, updated); saveErr != nil {
				g.logger.Warn("failed to save metric coverage",
					"project", projectName, "project_id", projectID, "error", saveErr)
			}
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

		g.logger.Log(ctx, logging.LevelTrace, "metric series",
			"measurement", result.Measurement,
			"points", len(result.Values),
			"project_id", projectID,
		)
	}

	level := slog.LevelDebug
	if len(points) == 0 {
		level = logging.LevelTrace
	}
	g.logger.Log(ctx, level, "metrics delivered",
		"project", projectName, "project_id", projectID,
		"series", len(results), "points", len(points),
		"start", startDateStr, "end", endTime.Format(time.RFC3339),
	)

	// Write to sinks
	if len(points) > 0 {
		for _, s := range g.sinks {
			if sinkErr := s.WriteMetrics(ctx, points); sinkErr != nil {
				g.logger.Error("failed to write metrics to sink",
					"sink", s.Name(), "project", projectName, "project_id", projectID, "error", sinkErr)
			}
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
