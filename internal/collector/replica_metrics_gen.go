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

// ReplicaMetricsGenerator implements TaskGenerator for per-replica metrics
// collection. It scans coverage gaps for each unique (serviceID, environmentID)
// pair and emits WorkItems targeting the replicaMetrics endpoint, which returns
// per-replica CPU and memory data.
type ReplicaMetricsGenerator struct {
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

// NewReplicaMetricsGenerator creates a ReplicaMetricsGenerator.
func NewReplicaMetricsGenerator(cfg ReplicaMetricsGeneratorConfig) *ReplicaMetricsGenerator {
	measurements := cfg.Measurements
	if len(measurements) == 0 {
		measurements = []railway.MetricMeasurement{
			railway.MetricMeasurementCpuUsage,
			railway.MetricMeasurementMemoryUsageGb,
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

	return &ReplicaMetricsGenerator{
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
func (g *ReplicaMetricsGenerator) Type() TaskType {
	return TaskTypeMetrics
}

// NextPoll returns the earliest time this generator will produce work.
func (g *ReplicaMetricsGenerator) NextPoll() time.Time { return g.nextPoll }

// metricsBatchKey computes the batch key from shared query parameters.
// Items with the same batch key can be merged into one aliased request.
func (g *ReplicaMetricsGenerator) metricsBatchKey() string {
	var parts []string
	for _, m := range g.measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("sr=%d,aw=%d,m=%s", g.sampleRate, g.avgWindow, strings.Join(parts, "+"))
}

// metricsBatchKeyChunk computes a batch key for a chunked (older gap) query.
// Includes chunk boundaries so that items for the same time window can be merged.
func (g *ReplicaMetricsGenerator) metricsBatchKeyChunk(chunkStart, chunkEnd time.Time) string {
	var parts []string
	for _, m := range g.measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("sr=%d,aw=%d,m=%s,s=%s,e=%s",
		g.sampleRate, g.avgWindow, strings.Join(parts, "+"),
		chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))
}

// Poll scans coverage gaps for all unique service+environment pairs,
// prioritizes by recency (live edge first), and returns WorkItems targeting
// the replicaMetrics endpoint.
func (g *ReplicaMetricsGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	svcEnvs := uniqueServiceEnvironments(targets)
	retentionStart := now.Add(-g.metricRetention)

	var items []WorkItem
	itemCount := 0

	for _, target := range svcEnvs {
		if itemCount >= g.maxItemsPerPoll {
			break
		}

		compositeKey := target.ServiceID + ":" + target.EnvironmentID
		coverageKey := CoverageKey(compositeKey, "replica-metric")
		existing, err := LoadCoverage(g.store, coverageKey)
		if err != nil {
			g.logger.Warn("failed to load replica metric coverage",
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
		g.logger.Debug("replica metric coverage gaps found",
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

			isLiveEdge := now.Sub(gap.End) < time.Minute

			if isLiveEdge {
				gapDuration := gap.End.Sub(gap.Start)
				if gapDuration > g.chunkSize {
					olderGap := TimeRange{Start: gap.Start, End: gap.End.Add(-g.chunkSize)}
					chunks := alignedChunks(olderGap, g.chunkSize)
					for _, chunk := range chunks {
						if itemCount >= g.maxItemsPerPoll {
							break
						}
						items = append(items, WorkItem{
							ID:       fmt.Sprintf("replica-metrics:%s:%s:%s", target.ServiceID, target.EnvironmentID, chunk.Start.Format(time.RFC3339)),
							Kind:     QueryReplicaMetrics,
							TaskType: TaskTypeMetrics,
							AliasKey: compositeKey,
							BatchKey: g.metricsBatchKeyChunk(chunk.Start, chunk.End),
							Params: map[string]any{
								"serviceId":              target.ServiceID,
								"environmentId":          target.EnvironmentID,
								"startDate":              chunk.Start.Format(time.RFC3339),
								"endDate":                chunk.End.Format(time.RFC3339),
								"measurements":           g.measurements,
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

				items = append(items, WorkItem{
					ID:       fmt.Sprintf("replica-metrics:%s:%s:%s", target.ServiceID, target.EnvironmentID, gap.Start.Format(time.RFC3339)),
					Kind:     QueryReplicaMetrics,
					TaskType: TaskTypeMetrics,
					AliasKey: compositeKey,
					BatchKey: g.metricsBatchKey(),
					Params: map[string]any{
						"serviceId":              target.ServiceID,
						"environmentId":          target.EnvironmentID,
						"startDate":              gap.Start.Format(time.RFC3339),
						"measurements":           g.measurements,
						"sampleRateSeconds":      g.sampleRate,
						"averagingWindowSeconds": g.avgWindow,
					},
				})
				itemCount++
			} else {
				chunks := alignedChunks(gap, g.chunkSize)
				for _, chunk := range chunks {
					if itemCount >= g.maxItemsPerPoll {
						break
					}

					items = append(items, WorkItem{
						ID:       fmt.Sprintf("replica-metrics:%s:%s:%s", target.ServiceID, target.EnvironmentID, chunk.Start.Format(time.RFC3339)),
						Kind:     QueryReplicaMetrics,
						TaskType: TaskTypeMetrics,
						AliasKey: compositeKey,
						BatchKey: g.metricsBatchKeyChunk(chunk.Start, chunk.End),
						Params: map[string]any{
							"serviceId":              target.ServiceID,
							"environmentId":          target.EnvironmentID,
							"startDate":              chunk.Start.Format(time.RFC3339),
							"endDate":                chunk.End.Format(time.RFC3339),
							"measurements":           g.measurements,
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

// Deliver processes the raw replica metrics JSON response for a single
// service+environment. It transforms the data into MetricPoints with
// replica_name labels, writes to sinks, and updates coverage.
func (g *ReplicaMetricsGenerator) Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error) {
	compositeKey := item.AliasKey
	now := g.clock.Now().UTC()
	targets := g.discovery.Targets()

	parts := strings.SplitN(compositeKey, ":", 2)
	serviceID := parts[0]
	environmentID := ""
	if len(parts) > 1 {
		environmentID = parts[1]
	}

	serviceName := ""
	environmentName := ""
	projectName := ""
	projectID := ""
	for _, t := range targets {
		if t.ServiceID == serviceID && t.EnvironmentID == environmentID {
			serviceName = t.ServiceName
			environmentName = t.EnvironmentName
			projectName = t.ProjectName
			projectID = t.ProjectID
			break
		}
	}

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
		coverageKey := CoverageKey(compositeKey, "replica-metric")
		existing, covErr := LoadCoverage(g.store, coverageKey)
		if covErr != nil {
			g.logger.Warn("failed to load replica metric coverage",
				"service", serviceName, "service_id", serviceID,
				"environment", environmentName, "environment_id", environmentID,
				"error", covErr)
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
				g.logger.Warn("failed to save replica metric coverage",
					"service", serviceName, "service_id", serviceID,
					"environment", environmentName, "environment_id", environmentID,
					"error", saveErr)
			}
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

	level := slog.LevelDebug
	if len(points) == 0 {
		level = logging.LevelTrace
	}
	g.logger.Log(ctx, level, "replica metrics delivered",
		"project", projectName,
		"service", serviceName, "service_id", serviceID,
		"environment", environmentName, "environment_id", environmentID,
		"series", len(results), "points", len(points),
		"start", startDateStr, "end", endTime.Format(time.RFC3339),
	)

	// Write to sinks
	if len(points) > 0 {
		for _, s := range g.sinks {
			if sinkErr := s.WriteMetrics(ctx, points); sinkErr != nil {
				g.logger.Error("failed to write replica metrics to sink",
					"sink", s.Name(),
					"service", serviceName, "service_id", serviceID,
					"environment", environmentName, "environment_id", environmentID,
					"error", sinkErr)
			}
		}
	}
}
