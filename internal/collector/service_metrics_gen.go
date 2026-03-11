package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/xevion/railway-collector/internal/logging"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
)

// ServiceMetricsGeneratorConfig configures a ServiceMetricsGenerator.
type ServiceMetricsGeneratorConfig struct {
	BaseMetricsConfig
}

// ServiceMetricsGenerator implements TaskGenerator for per-service+environment
// metrics collection. It scans coverage gaps for each unique (serviceID, environmentID)
// pair, prioritizes by recency, and emits chunked WorkItems.
type ServiceMetricsGenerator struct {
	baseMetrics
}

// NewServiceMetricsGenerator creates a ServiceMetricsGenerator.
func NewServiceMetricsGenerator(cfg ServiceMetricsGeneratorConfig) *ServiceMetricsGenerator {
	measurements := applyConfigDefaults(&cfg.BaseMetricsConfig, []railway.MetricMeasurement{
		railway.MetricMeasurementCpuUsage,
		railway.MetricMeasurementCpuLimit,
		railway.MetricMeasurementMemoryUsageGb,
		railway.MetricMeasurementMemoryLimitGb,
		railway.MetricMeasurementNetworkRxGb,
		railway.MetricMeasurementNetworkTxGb,
		railway.MetricMeasurementDiskUsageGb,
		railway.MetricMeasurementEphemeralDiskUsageGb,
		railway.MetricMeasurementBackupUsageGb,
	})

	return &ServiceMetricsGenerator{
		baseMetrics: newBaseMetrics(cfg.BaseMetricsConfig, measurements),
	}
}

// Type returns TaskTypeMetrics.
func (g *ServiceMetricsGenerator) Type() TaskType {
	return TaskTypeMetrics
}

// NextPoll returns the earliest time this generator will produce work.
func (g *ServiceMetricsGenerator) NextPoll() time.Time { return g.nextPoll }

// Poll scans coverage gaps for all unique service+environment pairs,
// prioritizes by recency (live edge first), and returns WorkItems.
func (g *ServiceMetricsGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	svcEnvs := uniqueServiceEnvironments(targets)
	retentionStart := now.Add(-g.metricRetention)

	groupBy := []railway.MetricTag{
		railway.MetricTagDeploymentId,
		railway.MetricTagDeploymentInstanceId,
		railway.MetricTagRegion,
	}

	var items []WorkItem
	itemCount := 0

	for _, target := range svcEnvs {
		if itemCount >= g.maxItemsPerPoll {
			break
		}

		compositeKey := target.CompositeKey()
		coverageKey := CoverageKey(compositeKey, CoverageTypeServiceMetric)
		existing, err := LoadCoverage(g.store, coverageKey)
		if err != nil {
			g.logger.Warn("failed to load service metric coverage",
				"service_id", target.ServiceID, "environment_id", target.EnvironmentID, "error", err)
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
		g.logger.Debug("service metric coverage gaps found",
			"service_id", target.ServiceID,
			"environment_id", target.EnvironmentID,
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
							ID:       fmt.Sprintf("svc-metrics:%s:%s:%s", target.ServiceID, target.EnvironmentID, chunk.Start.Format(time.RFC3339)),
							Kind:     QueryServiceMetrics,
							TaskType: TaskTypeMetrics,
							AliasKey: compositeKey,
							BatchKey: metricsBatchKeyChunk(g.measurements, g.sampleRate, g.avgWindow, chunk.Start, chunk.End),
							Params: map[string]any{
								"serviceId":              target.ServiceID,
								"environmentId":          target.EnvironmentID,
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
					gap = TimeRange{Start: gap.End.Add(-g.chunkSize), End: gap.End}
				}

				if itemCount >= g.maxItemsPerPoll {
					break
				}

				// Open-ended query: no endDate, captures data up to "now"
				items = append(items, WorkItem{
					ID:       fmt.Sprintf("svc-metrics:%s:%s:%s", target.ServiceID, target.EnvironmentID, gap.Start.Format(time.RFC3339)),
					Kind:     QueryServiceMetrics,
					TaskType: TaskTypeMetrics,
					AliasKey: compositeKey,
					BatchKey: metricsBatchKey(g.measurements, g.sampleRate, g.avgWindow),
					Params: map[string]any{
						"serviceId":              target.ServiceID,
						"environmentId":          target.EnvironmentID,
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
						ID:       fmt.Sprintf("svc-metrics:%s:%s:%s", target.ServiceID, target.EnvironmentID, chunk.Start.Format(time.RFC3339)),
						Kind:     QueryServiceMetrics,
						TaskType: TaskTypeMetrics,
						AliasKey: compositeKey,
						BatchKey: metricsBatchKeyChunk(g.measurements, g.sampleRate, g.avgWindow, chunk.Start, chunk.End),
						Params: map[string]any{
							"serviceId":              target.ServiceID,
							"environmentId":          target.EnvironmentID,
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

// Deliver processes the raw metrics JSON response for a single service+environment.
// It transforms the data into MetricPoints, writes to sinks, and updates coverage.
func (g *ServiceMetricsGenerator) Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error) {
	compositeKey := item.AliasKey
	now := g.clock.Now().UTC()
	targets := g.discovery.Targets()

	info := parseCompositeKey(compositeKey, targets)
	serviceID := info.serviceID
	environmentID := info.environmentID
	serviceName := info.serviceName
	environmentName := info.environmentName
	projectName := info.projectName

	if err != nil {
		g.logger.Error("service metrics delivery failed",
			"service", serviceName, "service_id", serviceID,
			"environment", environmentName, "environment_id", environmentID,
			"error", err)
		return
	}

	var results []rawMetricsResult
	if unmarshalErr := json.Unmarshal(data, &results); unmarshalErr != nil {
		g.logger.Error("failed to parse service metrics response",
			"service", serviceName, "service_id", serviceID,
			"environment", environmentName, "environment_id", environmentID,
			"error", unmarshalErr)
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
		covKey := CoverageKey(compositeKey, CoverageTypeServiceMetric)
		if covErr := updateCoverage(g.store, covKey, startTime, endTime, len(results) == 0, g.sampleRate); covErr != nil {
			g.logger.Warn("failed to update service metric coverage",
				"service", serviceName, "service_id", serviceID,
				"environment", environmentName, "environment_id", environmentID,
				"error", covErr)
		}
	}

	// Transform to MetricPoints
	var points []sink.MetricPoint
	for _, result := range results {
		metricName := measurementToMetricName(result.Measurement)
		baseLabels := buildMetricLabels(result.Tags, targets)
		// Ensure project_id is always set, even if API tags omit it for service-scoped queries
		if _, ok := baseLabels["project_id"]; !ok {
			for _, t := range targets {
				if t.ServiceID == serviceID && t.EnvironmentID == environmentID {
					baseLabels["project_id"] = t.ProjectID
					break
				}
			}
		}

		for _, v := range result.Values {
			points = append(points, sink.MetricPoint{
				Name:      metricName,
				Value:     v.Value,
				Timestamp: time.Unix(int64(v.Ts), 0),
				Labels:    copyLabels(baseLabels),
			})
		}

		g.logger.Log(ctx, logging.LevelTrace, "service metric series",
			"measurement", result.Measurement,
			"points", len(result.Values),
			"service_id", serviceID,
			"environment_id", environmentID,
		)
	}

	level := slog.LevelDebug
	if len(points) == 0 {
		level = logging.LevelTrace
	}
	g.logger.Log(ctx, level, "service metrics delivered",
		"project", projectName,
		"service", serviceName, "service_id", serviceID,
		"environment", environmentName, "environment_id", environmentID,
		"series", len(results), "points", len(points),
		"start", startDateStr, "end", endTime.Format(time.RFC3339),
	)

	// Write to sinks
	writeMetricsToSinks(ctx, g.sinks, points, g.logger)
}
