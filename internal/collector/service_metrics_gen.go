package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/xevion/railway-collector/internal/collector/coverage"
	"github.com/xevion/railway-collector/internal/collector/types"
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

// Type returns types.TaskTypeMetrics.
func (g *ServiceMetricsGenerator) Type() types.TaskType {
	return types.TaskTypeMetrics
}

// NextPoll returns the earliest time this generator will produce work.
func (g *ServiceMetricsGenerator) NextPoll() time.Time { return g.nextPoll }

// Poll scans coverage gaps for all unique service+environment pairs,
// prioritizes by recency (live edge first), and returns WorkItems.
func (g *ServiceMetricsGenerator) Poll(now time.Time) []types.WorkItem {
	groupBy := []railway.MetricTag{
		railway.MetricTagDeploymentId,
		railway.MetricTagDeploymentInstanceId,
		railway.MetricTagRegion,
	}

	items := pollCoverageGaps(now, gapPollParams{
		store:           g.store,
		discovery:       g.discovery,
		logger:          g.logger,
		metricRetention: g.metricRetention,
		chunkSize:       g.chunkSize,
		maxItemsPerPoll: g.maxItemsPerPoll,
		nextPoll:        g.nextPoll,
		itemsPerEmit:    1,
		logPrefix:       "service metric",
		entities: func(targets []types.ServiceTarget) []pollEntity {
			svcEnvs := uniqueServiceEnvironments(targets)
			entities := make([]pollEntity, len(svcEnvs))
			for i, t := range svcEnvs {
				entities[i] = pollEntity{
					Key:          t.CompositeKey(),
					CoverageType: coverage.CoverageTypeServiceMetric,
					LogAttrs:     []any{"service_id", t.ServiceID, "environment_id", t.EnvironmentID},
					Data:         t,
				}
			}
			return entities
		},
		buildItems: func(entity pollEntity, chunk coverage.TimeRange, isLiveEdge bool) []types.WorkItem {
			t := entity.Data.(types.ServiceTarget)
			params := map[string]any{
				"serviceId":              t.ServiceID,
				"environmentId":          t.EnvironmentID,
				"startDate":              chunk.Start.Format(time.RFC3339),
				"measurements":           g.measurements,
				"groupBy":                groupBy,
				"sampleRateSeconds":      g.sampleRate,
				"averagingWindowSeconds": g.avgWindow,
			}
			batchKey := metricsBatchKey(g.measurements, g.sampleRate, g.avgWindow)
			if !isLiveEdge {
				params["endDate"] = chunk.End.Format(time.RFC3339)
				batchKey = metricsBatchKeyChunk(g.measurements, g.sampleRate, g.avgWindow, chunk.Start, chunk.End)
			}
			return []types.WorkItem{{
				ID:       fmt.Sprintf("svc-metrics:%s:%s:%s", t.ServiceID, t.EnvironmentID, chunk.Start.Format(time.RFC3339)),
				Kind:     types.QueryServiceMetrics,
				TaskType: types.TaskTypeMetrics,
				AliasKey: entity.Key,
				BatchKey: batchKey,
				Params:   params,
			}}
		},
	})

	if len(items) > 0 {
		g.nextPoll = now.Add(g.interval)
	}

	return items
}

// Deliver processes the raw metrics JSON response for a single service+environment.
// It transforms the data into MetricPoints, writes to sinks, and updates coverage.
func (g *ServiceMetricsGenerator) Deliver(ctx context.Context, item types.WorkItem, data json.RawMessage, err error) {
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
		covKey := coverage.CoverageKey(compositeKey, coverage.CoverageTypeServiceMetric)
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

	level := deliveryLogLevel(len(points))
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
