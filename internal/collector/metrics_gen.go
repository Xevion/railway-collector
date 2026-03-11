package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
	VolumeID             *string `json:"volumeId"`
	VolumeInstanceID     *string `json:"volumeInstanceId"`
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
	if tags.DeploymentInstanceID != nil {
		labels["deployment_instance_id"] = *tags.DeploymentInstanceID
	}
	if tags.Region != nil {
		labels["region"] = *tags.Region
	}
	if tags.VolumeID != nil {
		labels["volume_id"] = *tags.VolumeID
	}
	if tags.VolumeInstanceID != nil {
		labels["volume_instance_id"] = *tags.VolumeInstanceID
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

// ProjectMetricsGeneratorConfig configures a ProjectMetricsGenerator.
type ProjectMetricsGeneratorConfig struct {
	BaseMetricsConfig
}

// ProjectMetricsGenerator implements TaskGenerator for metrics collection.
// It scans coverage gaps each poll, prioritizes by recency (live edge first),
// and emits chunked WorkItems for older gaps and open-ended items for the live edge.
type ProjectMetricsGenerator struct {
	baseMetrics
}

// NewProjectMetricsGenerator creates a ProjectMetricsGenerator.
func NewProjectMetricsGenerator(cfg ProjectMetricsGeneratorConfig) *ProjectMetricsGenerator {
	measurements := applyConfigDefaults(&cfg.BaseMetricsConfig, []railway.MetricMeasurement{
		railway.MetricMeasurementCpuUsage,
		railway.MetricMeasurementMemoryUsageGb,
		railway.MetricMeasurementNetworkRxGb,
		railway.MetricMeasurementNetworkTxGb,
		railway.MetricMeasurementDiskUsageGb,
	})

	return &ProjectMetricsGenerator{
		baseMetrics: newBaseMetrics(cfg.BaseMetricsConfig, measurements),
	}
}

// Type returns TaskTypeMetrics.
func (g *ProjectMetricsGenerator) Type() TaskType {
	return TaskTypeMetrics
}

// NextPoll returns the earliest time this generator will produce work.
func (g *ProjectMetricsGenerator) NextPoll() time.Time { return g.nextPoll }

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
func (g *ProjectMetricsGenerator) Poll(now time.Time) []WorkItem {
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

		coverageKey := CoverageKey(pid, CoverageTypeMetric)
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
				// If the gap is larger than one chunk, split the older
				// portion into fixed chunks and only keep the tail as
				// an open-ended live-edge query.
				gapDuration := gap.End.Sub(gap.Start)
				if gapDuration > g.chunkSize {
					olderGap := TimeRange{Start: gap.Start, End: gap.End.Add(-g.chunkSize)}
					chunks := alignedChunks(olderGap, g.chunkSize)
					for _, chunk := range chunks {
						if itemCount >= g.maxItemsPerPoll {
							break
						}
						items = append(items, WorkItem{
							ID:       fmt.Sprintf("metrics:%s:%s", pid, chunk.Start.Format(time.RFC3339)),
							Kind:     QueryMetrics,
							TaskType: TaskTypeMetrics,
							AliasKey: pid,
							BatchKey: metricsBatchKeyChunk(g.measurements, g.sampleRate, g.avgWindow, chunk.Start, chunk.End),
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
					// Adjust the live-edge start to the tail
					gap = TimeRange{Start: gap.End.Add(-g.chunkSize), End: gap.End}
				}

				if itemCount >= g.maxItemsPerPoll {
					break
				}

				// Open-ended query: no endDate, captures data up to "now"
				items = append(items, WorkItem{
					ID:       fmt.Sprintf("metrics:%s:%s", pid, gap.Start.Format(time.RFC3339)),
					Kind:     QueryMetrics,
					TaskType: TaskTypeMetrics,
					AliasKey: pid,
					BatchKey: metricsBatchKey(g.measurements, g.sampleRate, g.avgWindow),
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
						BatchKey: metricsBatchKeyChunk(g.measurements, g.sampleRate, g.avgWindow, chunk.Start, chunk.End),
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
func (g *ProjectMetricsGenerator) Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error) {
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
		covKey := CoverageKey(projectID, CoverageTypeMetric)
		if covErr := updateCoverage(g.store, covKey, startTime, endTime, len(results) == 0, g.sampleRate); covErr != nil {
			g.logger.Warn("failed to update metric coverage",
				"project", projectName, "project_id", projectID, "error", covErr)
		}
	}

	// Transform to MetricPoints
	var points []sink.MetricPoint
	for _, result := range results {
		metricName := measurementToMetricName(result.Measurement)
		baseLabels := g.buildLabelsFromRaw(result.Tags, targets)

		for _, v := range result.Values {
			points = append(points, sink.MetricPoint{
				Name:      metricName,
				Value:     v.Value,
				Timestamp: time.Unix(int64(v.Ts), 0),
				Labels:    copyLabels(baseLabels),
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
	writeMetricsToSinks(ctx, g.sinks, points, g.logger)
}

// buildLabelsFromRaw builds metric labels from raw JSON tags,
// enriching with target names where possible.
func (g *ProjectMetricsGenerator) buildLabelsFromRaw(tags rawMetricsTags, targets []ServiceTarget) map[string]string {
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
