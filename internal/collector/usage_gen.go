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

// Raw JSON response types for usage queries.

type rawUsageResult struct {
	Measurement string       `json:"measurement"`
	Value       float64      `json:"value"`
	Tags        rawUsageTags `json:"tags"`
}

type rawUsageTags struct {
	ProjectID     *string `json:"projectId"`
	ServiceID     *string `json:"serviceId"`
	EnvironmentID *string `json:"environmentId"`
}

type rawEstimatedUsageResult struct {
	EstimatedValue float64 `json:"estimatedValue"`
	Measurement    string  `json:"measurement"`
	ProjectID      string  `json:"projectId"`
}

// UsageGeneratorConfig configures a UsageGenerator.
type UsageGeneratorConfig struct {
	Discovery    TargetProvider
	Sinks        []sink.Sink
	Clock        clockwork.Clock
	Measurements []railway.MetricMeasurement
	Interval     time.Duration // default: 1 hour
	Logger       *slog.Logger
}

// UsageGenerator implements TaskGenerator for billing/cost data collection.
// It collects current-state snapshots at a low cadence (default: hourly),
// emitting usage and estimated-usage WorkItems per project.
type UsageGenerator struct {
	discovery    TargetProvider
	sinks        []sink.Sink
	clock        clockwork.Clock
	measurements []railway.MetricMeasurement
	interval     time.Duration
	logger       *slog.Logger

	nextPoll time.Time
}

// NewUsageGenerator creates a UsageGenerator.
func NewUsageGenerator(cfg UsageGeneratorConfig) *UsageGenerator {
	measurements := cfg.Measurements
	if len(measurements) == 0 {
		measurements = []railway.MetricMeasurement{
			railway.MetricMeasurementCpuUsage,
			railway.MetricMeasurementMemoryUsageGb,
			railway.MetricMeasurementNetworkTxGb,
			railway.MetricMeasurementDiskUsageGb,
			railway.MetricMeasurementBackupUsageGb,
		}
	}

	interval := cfg.Interval
	if interval == 0 {
		interval = time.Hour
	}

	return &UsageGenerator{
		discovery:    cfg.Discovery,
		sinks:        cfg.Sinks,
		clock:        cfg.Clock,
		measurements: measurements,
		interval:     interval,
		logger:       cfg.Logger,
	}
}

// Type returns TaskTypeUsage.
func (g *UsageGenerator) Type() TaskType {
	return TaskTypeUsage
}

// NextPoll returns the earliest time this generator will produce work.
func (g *UsageGenerator) NextPoll() time.Time { return g.nextPoll }

// usageBatchKey computes the batch key for usage queries.
// All projects share the same batch key since they have identical measurements.
func (g *UsageGenerator) usageBatchKey() string {
	var parts []string
	for _, m := range g.measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("usage:m=%s", strings.Join(parts, "+"))
}

// estimatedUsageBatchKey computes the batch key for estimated usage queries.
func (g *UsageGenerator) estimatedUsageBatchKey() string {
	var parts []string
	for _, m := range g.measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("estusage:m=%s", strings.Join(parts, "+"))
}

// Poll checks whether it's time to collect usage data and emits WorkItems.
// For each project, it emits two items: one for usage and one for estimated usage.
func (g *UsageGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	projectIDs := uniqueProjectIDs(targets)

	groupBy := []railway.MetricTag{
		railway.MetricTagProjectId,
		railway.MetricTagServiceId,
	}

	var items []WorkItem
	for _, pid := range projectIDs {
		items = append(items, WorkItem{
			ID:       fmt.Sprintf("usage:%s", pid),
			Kind:     QueryUsage,
			TaskType: TaskTypeUsage,
			AliasKey: pid,
			BatchKey: g.usageBatchKey(),
			Params: map[string]any{
				"measurements": g.measurements,
				"groupBy":      groupBy,
			},
		})

		items = append(items, WorkItem{
			ID:       fmt.Sprintf("estimated-usage:%s", pid),
			Kind:     QueryEstimatedUsage,
			TaskType: TaskTypeUsage,
			AliasKey: pid,
			BatchKey: g.estimatedUsageBatchKey(),
			Params: map[string]any{
				"measurements": g.measurements,
			},
		})
	}

	if len(items) > 0 {
		g.nextPoll = now.Add(g.interval)
	}

	return items
}

// Deliver processes the raw JSON response for a usage work item.
// It routes by item.Kind to handle the two different response shapes.
func (g *UsageGenerator) Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error) {
	projectID := item.AliasKey
	targets := g.discovery.Targets()

	projectName := ""
	for _, t := range targets {
		if t.ProjectID == projectID {
			projectName = t.ProjectName
			break
		}
	}

	if err != nil {
		g.logger.Error("usage delivery failed",
			"kind", string(item.Kind),
			"project", projectName, "project_id", projectID,
			"error", err)
		return
	}

	switch item.Kind {
	case QueryUsage:
		g.deliverUsage(ctx, item, data, projectID, projectName, targets)
	case QueryEstimatedUsage:
		g.deliverEstimatedUsage(ctx, item, data, projectID, projectName, targets)
	default:
		g.logger.Error("unknown usage query kind",
			"kind", string(item.Kind), "project_id", projectID)
	}
}

// deliverUsage handles QueryUsage responses.
func (g *UsageGenerator) deliverUsage(
	ctx context.Context, _ WorkItem, data json.RawMessage,
	projectID, projectName string, targets []ServiceTarget,
) {
	var results []rawUsageResult
	if unmarshalErr := json.Unmarshal(data, &results); unmarshalErr != nil {
		g.logger.Error("failed to parse usage response",
			"project", projectName, "project_id", projectID,
			"error", unmarshalErr)
		return
	}

	now := g.clock.Now().UTC()
	var points []sink.MetricPoint

	for _, result := range results {
		metricName := usageMetricName("railway_usage", result.Measurement)

		labels := make(map[string]string)
		if result.Tags.ProjectID != nil {
			labels["project_id"] = *result.Tags.ProjectID
		}
		if result.Tags.ServiceID != nil {
			labels["service_id"] = *result.Tags.ServiceID
		}
		if result.Tags.EnvironmentID != nil {
			labels["environment_id"] = *result.Tags.EnvironmentID
		}

		// Enrich with names from targets
		for _, t := range targets {
			if result.Tags.ServiceID != nil && t.ServiceID == *result.Tags.ServiceID {
				labels["project_name"] = t.ProjectName
				labels["service_name"] = t.ServiceName
				if result.Tags.EnvironmentID != nil && t.EnvironmentID == *result.Tags.EnvironmentID {
					labels["environment_name"] = t.EnvironmentName
				}
				break
			}
		}
		// If no service-level match, still enrich project name
		if _, ok := labels["project_name"]; !ok {
			if result.Tags.ProjectID != nil {
				for _, t := range targets {
					if t.ProjectID == *result.Tags.ProjectID {
						labels["project_name"] = t.ProjectName
						break
					}
				}
			}
		}

		points = append(points, sink.MetricPoint{
			Name:      metricName,
			Value:     result.Value,
			Timestamp: now,
			Labels:    labels,
		})

		g.logger.Log(ctx, logging.LevelTrace, "usage result",
			"measurement", result.Measurement,
			"value", result.Value,
			"project_id", projectID,
		)
	}

	level := deliveryLogLevel(len(points))
	g.logger.Log(ctx, level, "usage delivered",
		"project", projectName, "project_id", projectID,
		"results", len(results), "points", len(points),
	)

	writeMetricsToSinks(ctx, g.sinks, points, g.logger)
}

// deliverEstimatedUsage handles QueryEstimatedUsage responses.
func (g *UsageGenerator) deliverEstimatedUsage(
	ctx context.Context, _ WorkItem, data json.RawMessage,
	projectID, projectName string, targets []ServiceTarget,
) {
	var results []rawEstimatedUsageResult
	if unmarshalErr := json.Unmarshal(data, &results); unmarshalErr != nil {
		g.logger.Error("failed to parse estimated usage response",
			"project", projectName, "project_id", projectID,
			"error", unmarshalErr)
		return
	}

	now := g.clock.Now().UTC()
	var points []sink.MetricPoint

	for _, result := range results {
		metricName := usageMetricName("railway_estimated_usage", result.Measurement)

		labels := map[string]string{
			"project_id": result.ProjectID,
		}

		// Enrich with project name from targets
		for _, t := range targets {
			if t.ProjectID == result.ProjectID {
				labels["project_name"] = t.ProjectName
				break
			}
		}

		points = append(points, sink.MetricPoint{
			Name:      metricName,
			Value:     result.EstimatedValue,
			Timestamp: now,
			Labels:    labels,
		})

		g.logger.Log(ctx, logging.LevelTrace, "estimated usage result",
			"measurement", result.Measurement,
			"estimated_value", result.EstimatedValue,
			"project_id", result.ProjectID,
		)
	}

	level := deliveryLogLevel(len(points))
	g.logger.Log(ctx, level, "estimated usage delivered",
		"project", projectName, "project_id", projectID,
		"results", len(results), "points", len(points),
	)

	writeMetricsToSinks(ctx, g.sinks, points, g.logger)
}
