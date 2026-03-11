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

// rawReplicaMetricsResult mirrors the JSON shape returned by the Railway
// replicaMetrics endpoint. Unlike the standard metrics response, it includes
// a replicaName field instead of grouped tags.
type rawReplicaMetricsResult struct {
	Measurement string           `json:"measurement"`
	ReplicaName string           `json:"replicaName"`
	Values      []rawMetricValue `json:"values"`
}

// ReplicaMetricsGeneratorConfig configures a ReplicaMetricsGenerator.
type ReplicaMetricsGeneratorConfig struct {
	BaseMetricsConfig
}

// ReplicaMetricsGenerator implements TaskGenerator for per-replica metrics
// collection. It scans coverage gaps for each unique (serviceID, environmentID)
// pair and emits WorkItems targeting the replicaMetrics endpoint, which returns
// per-replica CPU and memory data.
type ReplicaMetricsGenerator struct {
	baseMetrics
}

// NewReplicaMetricsGenerator creates a ReplicaMetricsGenerator.
func NewReplicaMetricsGenerator(cfg ReplicaMetricsGeneratorConfig) *ReplicaMetricsGenerator {
	measurements := applyConfigDefaults(&cfg.BaseMetricsConfig, []railway.MetricMeasurement{
		railway.MetricMeasurementCpuUsage,
		railway.MetricMeasurementMemoryUsageGb,
	})

	return &ReplicaMetricsGenerator{
		baseMetrics: newBaseMetrics(cfg.BaseMetricsConfig, measurements),
	}
}

// Type returns types.TaskTypeMetrics.
func (g *ReplicaMetricsGenerator) Type() types.TaskType {
	return types.TaskTypeMetrics
}

// NextPoll returns the earliest time this generator will produce work.
func (g *ReplicaMetricsGenerator) NextPoll() time.Time { return g.nextPoll }

// Poll scans coverage gaps for all unique service+environment pairs,
// prioritizes by recency (live edge first), and returns WorkItems targeting
// the replicaMetrics endpoint.
func (g *ReplicaMetricsGenerator) Poll(now time.Time) []types.WorkItem {
	items := pollCoverageGaps(now, gapPollParams{
		store:           g.store,
		discovery:       g.discovery,
		logger:          g.logger,
		metricRetention: g.metricRetention,
		chunkSize:       g.chunkSize,
		maxItemsPerPoll: g.maxItemsPerPoll,
		nextPoll:        g.nextPoll,
		itemsPerEmit:    1,
		logPrefix:       "replica metric",
		entities: func(targets []types.ServiceTarget) []pollEntity {
			svcEnvs := uniqueServiceEnvironments(targets)
			entities := make([]pollEntity, len(svcEnvs))
			for i, t := range svcEnvs {
				entities[i] = pollEntity{
					Key:          t.CompositeKey(),
					CoverageType: coverage.CoverageTypeReplicaMetric,
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
				"sampleRateSeconds":      g.sampleRate,
				"averagingWindowSeconds": g.avgWindow,
			}
			batchKey := metricsBatchKey(g.measurements, g.sampleRate, g.avgWindow)
			if !isLiveEdge {
				params["endDate"] = chunk.End.Format(time.RFC3339)
				batchKey = metricsBatchKeyChunk(g.measurements, g.sampleRate, g.avgWindow, chunk.Start, chunk.End)
			}
			return []types.WorkItem{{
				ID:       fmt.Sprintf("replica-metrics:%s:%s:%s", t.ServiceID, t.EnvironmentID, chunk.Start.Format(time.RFC3339)),
				Kind:     types.QueryReplicaMetrics,
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

// Deliver processes the raw replica metrics JSON response for a single
// service+environment. It transforms the data into MetricPoints with
// replica_name labels, writes to sinks, and updates coverage.
func (g *ReplicaMetricsGenerator) Deliver(ctx context.Context, item types.WorkItem, data json.RawMessage, err error) {
	compositeKey := item.AliasKey
	now := g.clock.Now().UTC()
	targets := g.discovery.Targets()

	info := parseCompositeKey(compositeKey, targets)
	serviceID := info.serviceID
	environmentID := info.environmentID
	serviceName := info.serviceName
	environmentName := info.environmentName
	projectName := info.projectName
	projectID := info.projectID

	if err != nil {
		g.logger.Error("replica metrics delivery failed",
			"service", serviceName, "service_id", serviceID,
			"environment", environmentName, "environment_id", environmentID,
			"error", err)
		return
	}

	var results []rawReplicaMetricsResult
	if unmarshalErr := json.Unmarshal(data, &results); unmarshalErr != nil {
		g.logger.Error("failed to parse replica metrics response",
			"service", serviceName, "service_id", serviceID,
			"environment", environmentName, "environment_id", environmentID,
			"error", unmarshalErr)
		return
	}

	// Determine coverage interval from params
	startDateStr, _ := item.Params["startDate"].(string)
	startTime, _ := time.Parse(time.RFC3339, startDateStr)

	endTime := now
	if endDateStr, ok := item.Params["endDate"].(string); ok {
		if parsed, parseErr := time.Parse(time.RFC3339, endDateStr); parseErr == nil {
			endTime = parsed
		}
	}

	// Update coverage
	if !startTime.IsZero() {
		covKey := coverage.CoverageKey(compositeKey, coverage.CoverageTypeReplicaMetric)
		if covErr := updateCoverage(g.store, covKey, startTime, endTime, len(results) == 0, g.sampleRate); covErr != nil {
			g.logger.Warn("failed to update replica metric coverage",
				"service", serviceName, "service_id", serviceID,
				"environment", environmentName, "environment_id", environmentID,
				"error", covErr)
		}
	}

	// Transform to MetricPoints
	var points []sink.MetricPoint
	for _, result := range results {
		metricName := measurementToMetricName(result.Measurement)

		labels := map[string]string{
			"project_id":     projectID,
			"service_id":     serviceID,
			"environment_id": environmentID,
			"replica_name":   result.ReplicaName,
		}
		// Enrich with names from targets
		if projectName != "" {
			labels["project_name"] = projectName
		}
		if serviceName != "" {
			labels["service_name"] = serviceName
		}
		if environmentName != "" {
			labels["environment_name"] = environmentName
		}

		for _, v := range result.Values {
			points = append(points, sink.MetricPoint{
				Name:      metricName,
				Value:     v.Value,
				Timestamp: time.Unix(int64(v.Ts), 0),
				Labels:    copyLabels(labels),
			})
		}

		g.logger.Log(ctx, logging.LevelTrace, "replica metric series",
			"measurement", result.Measurement,
			"replica_name", result.ReplicaName,
			"points", len(result.Values),
			"service_id", serviceID,
			"environment_id", environmentID,
		)
	}

	level := deliveryLogLevel(len(points))
	g.logger.Log(ctx, level, "replica metrics delivered",
		"project", projectName,
		"service", serviceName, "service_id", serviceID,
		"environment", environmentName, "environment_id", environmentID,
		"series", len(results), "points", len(points),
		"start", startDateStr, "end", endTime.Format(time.RFC3339),
	)

	// Write to sinks
	writeMetricsToSinks(ctx, g.sinks, points, g.logger)
}
