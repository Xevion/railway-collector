package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
)

type BackfillConfig struct {
	API       RailwayAPI
	Store     StateStore
	Discovery TargetProvider
	Sinks     []sink.Sink
	Clock     clockwork.Clock

	MetricSampleRate int
	MetricAvgWindow  int
	MetricRetention  time.Duration // ~90 days
	MetricChunkSize  time.Duration // ~10 days per API call

	LogRetention time.Duration // ~5 days
	LogLimit     int           // max logs per request

	MaxChunksPerRun int // max API calls per RunOnce

	Logger *slog.Logger
}

type BackfillManager struct {
	cfg BackfillConfig
}

func NewBackfillManager(cfg BackfillConfig) *BackfillManager {
	if cfg.MaxChunksPerRun == 0 {
		cfg.MaxChunksPerRun = 3
	}
	if cfg.MetricChunkSize == 0 {
		cfg.MetricChunkSize = 10 * 24 * time.Hour
	}
	if cfg.MetricRetention == 0 {
		cfg.MetricRetention = 90 * 24 * time.Hour
	}
	if cfg.LogRetention == 0 {
		cfg.LogRetention = 5 * 24 * time.Hour
	}
	return &BackfillManager{cfg: cfg}
}

// Collect implements the Collector interface so BackfillManager can be scheduled.
func (bm *BackfillManager) Collect(ctx context.Context) error {
	return bm.RunOnce(ctx)
}

// RunOnce scans all targets for coverage gaps and fills up to MaxChunksPerRun chunks.
func (bm *BackfillManager) RunOnce(ctx context.Context) error {
	now := bm.cfg.Clock.Now().UTC()
	targets := bm.cfg.Discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	// Build a scoped logger; backfill is always "backfill" but honour any injected trigger.
	logger := bm.cfg.Logger
	if trigger := triggerFromCtx(ctx); trigger != "" {
		logger = logger.With("trigger", trigger)
	}

	start := time.Now()

	projectNames := make(map[string]string)
	for _, t := range targets {
		projectNames[t.ProjectID] = t.ProjectName
	}
	envNames := make(map[string]string)
	for _, t := range targets {
		envNames[t.EnvironmentID] = t.EnvironmentName
	}

	chunksUsed := 0
	totalMetricPoints := 0
	totalLogEntries := 0

	// Phase 1: Metric backfill
	metricRetentionStart := now.Add(-bm.cfg.MetricRetention)
	projectIDs := uniqueProjectIDs(targets)

	// Phase 2 env set is built later, but we need envSet length for the start log.
	envSet := make(map[string][]ServiceTarget)
	for _, t := range targets {
		envSet[t.EnvironmentID] = append(envSet[t.EnvironmentID], t)
	}

	logger.Info("backfill started", "projects", len(projectIDs), "environments", len(envSet))

	for _, projectID := range projectIDs {
		if chunksUsed >= bm.cfg.MaxChunksPerRun {
			break
		}

		coverageKey := CoverageKey(projectID, "metric")
		existing, err := LoadCoverage(bm.cfg.Store, coverageKey)
		if err != nil {
			logger.Warn("failed to load metric coverage for backfill",
				"project", projectNames[projectID], "project_id", projectID, "error", err)
			continue
		}

		gaps := FindGaps(existing, metricRetentionStart, now)
		if len(gaps) == 0 {
			continue
		}

		prioritized := PrioritizeGaps(gaps, now)

		for _, gap := range prioritized {
			if chunksUsed >= bm.cfg.MaxChunksPerRun {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}

			chunks := ChunkTimeRange(gap, bm.cfg.MetricChunkSize)
			for _, chunk := range chunks {
				if chunksUsed >= bm.cfg.MaxChunksPerRun {
					break
				}

				points, err := bm.backfillMetricChunk(ctx, projectID, chunk, targets)
				if err != nil {
					logger.Warn("metric backfill chunk failed",
						"project", projectNames[projectID], "project_id", projectID, "start", chunk.Start, "end", chunk.End, "error", err)
					continue
				}
				chunksUsed++
				totalMetricPoints += len(points)
				logger.Info("backfill metric chunk complete",
					"project", projectNames[projectID], "project_id", projectID,
					"start", chunk.Start, "end", chunk.End,
					"points", len(points),
				)

				if len(points) > 0 {
					for _, s := range bm.cfg.Sinks {
						if err := s.WriteMetrics(ctx, points); err != nil {
							logger.Error("failed to write backfill metrics", "sink", s.Name(), "project", projectNames[projectID], "project_id", projectID, "points", len(points), "error", err)
						} else {
							logger.Debug("wrote backfill metrics to sink", "sink", s.Name(), "points", len(points))
						}
					}
				}

				kind := CoverageCollected
				if len(points) == 0 {
					kind = CoverageEmpty
				}
				updated := InsertInterval(existing, CoverageInterval{
					Start: chunk.Start, End: chunk.End,
					Kind: kind, Resolution: bm.cfg.MetricSampleRate,
				})
				existing = updated
				if err := SaveCoverage(bm.cfg.Store, coverageKey, updated); err != nil {
					logger.Warn("failed to save backfill coverage", "project", projectNames[projectID], "project_id", projectID, "error", err)
				}
			}
		}
	}

	// Phase 2: Log backfill (environment logs)
	logRetentionStart := now.Add(-bm.cfg.LogRetention)

	for envID, envTargets := range envSet {
		if chunksUsed >= bm.cfg.MaxChunksPerRun {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		coverageKey := CoverageKey(envID, "log", "environment")
		existing, err := LoadCoverage(bm.cfg.Store, coverageKey)
		if err != nil {
			logger.Warn("failed to load log coverage for backfill", "environment", envNames[envID], "environment_id", envID, "error", err)
			continue
		}

		gaps := FindGaps(existing, logRetentionStart, now)
		if len(gaps) == 0 {
			continue
		}

		prioritized := PrioritizeGaps(gaps, now)
		for _, gap := range prioritized {
			if chunksUsed >= bm.cfg.MaxChunksPerRun {
				break
			}

			entries, err := bm.backfillEnvLogChunk(ctx, envID, gap, envTargets)
			if err != nil {
				logger.Warn("env log backfill failed", "environment", envNames[envID], "environment_id", envID, "error", err)
				continue
			}
			chunksUsed++
			totalLogEntries += len(entries)
			logger.Info("backfill log chunk complete",
				"environment", envNames[envID], "environment_id", envID,
				"start", gap.Start, "end", gap.End,
				"entries", len(entries),
			)

			if len(entries) > 0 {
				for _, s := range bm.cfg.Sinks {
					if err := s.WriteLogs(ctx, entries); err != nil {
						logger.Error("failed to write backfill logs", "sink", s.Name(), "environment", envNames[envID], "environment_id", envID, "entries", len(entries), "error", err)
					} else {
						logger.Debug("wrote backfill logs to sink", "sink", s.Name(), "entries", len(entries))
					}
				}
			}

			kind := CoverageCollected
			if len(entries) == 0 {
				kind = CoverageEmpty
			}
			updated := InsertInterval(existing, CoverageInterval{Start: gap.Start, End: gap.End, Kind: kind})
			existing = updated
			if err := SaveCoverage(bm.cfg.Store, coverageKey, updated); err != nil {
				logger.Warn("failed to save log backfill coverage", "environment", envNames[envID], "environment_id", envID, "error", err)
			}
		}
	}

	if chunksUsed == 0 {
		logger.Debug("backfill skipped, no gaps found")
	} else {
		logger.Info("backfill complete",
			"chunks_processed", chunksUsed,
			"metric_points", totalMetricPoints,
			"log_entries", totalLogEntries,
			"duration", time.Since(start),
		)
	}

	return nil
}

func (bm *BackfillManager) backfillMetricChunk(ctx context.Context, projectID string, chunk TimeRange, targets []ServiceTarget) ([]sink.MetricPoint, error) {
	startDate := chunk.Start.Format(time.RFC3339)
	endDate := chunk.End.Format(time.RFC3339)
	sampleRate := bm.cfg.MetricSampleRate
	avgWindow := bm.cfg.MetricAvgWindow

	measurements := []railway.MetricMeasurement{
		railway.MetricMeasurementCpuUsage,
		railway.MetricMeasurementMemoryUsageGb,
		railway.MetricMeasurementNetworkRxGb,
		railway.MetricMeasurementNetworkTxGb,
		railway.MetricMeasurementDiskUsageGb,
	}
	groupBy := []railway.MetricTag{
		railway.MetricTagServiceId,
		railway.MetricTagEnvironmentId,
	}

	resp, err := bm.cfg.API.GetMetrics(ctx, &projectID, nil, nil, startDate, &endDate, measurements, groupBy, &sampleRate, &avgWindow)
	if err != nil {
		return nil, fmt.Errorf("backfill GetMetrics: %w", err)
	}

	var points []sink.MetricPoint
	for _, result := range resp.Metrics {
		metricName, ok := prometheusNameMap[result.Measurement]
		if !ok {
			metricName = fmt.Sprintf("railway_%s", strings.ToLower(string(result.Measurement)))
		}
		labels := buildBackfillLabels(result.Tags, targets)
		for _, v := range result.Values {
			points = append(points, sink.MetricPoint{
				Name: metricName, Value: v.Value,
				Timestamp: time.Unix(int64(v.Ts), 0),
				Labels:    labels,
			})
		}
	}
	return points, nil
}

func (bm *BackfillManager) backfillEnvLogChunk(ctx context.Context, envID string, gap TimeRange, envTargets []ServiceTarget) ([]sink.LogEntry, error) {
	afterDate := gap.Start.Format(time.RFC3339Nano)
	beforeDate := gap.End.Format(time.RFC3339Nano)
	limit := bm.cfg.LogLimit
	if limit == 0 {
		limit = 5000
	}

	resp, err := bm.cfg.API.GetEnvironmentLogs(ctx, envID, nil, &afterDate, &beforeDate, &limit, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("backfill GetEnvironmentLogs: %w", err)
	}

	services := make(map[string]ServiceTarget)
	for _, t := range envTargets {
		services[t.ServiceID] = t
	}

	var entries []sink.LogEntry
	for _, log := range resp.EnvironmentLogs {
		ts, err := time.Parse(time.RFC3339Nano, log.Timestamp)
		if err != nil {
			continue
		}
		labels := map[string]string{
			"log_type":       "deployment",
			"environment_id": envID,
			"backfill":       "true",
		}
		if log.Tags != nil && log.Tags.ServiceId != nil {
			if target, ok := services[*log.Tags.ServiceId]; ok {
				labels["project_id"] = target.ProjectID
				labels["project_name"] = target.ProjectName
				labels["service_id"] = target.ServiceID
				labels["service_name"] = target.ServiceName
				labels["environment_name"] = target.EnvironmentName
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
			Timestamp: ts, Message: log.Message,
			Severity: sev, Labels: labels, Attributes: attrs,
		})
	}
	return entries, nil
}

func buildBackfillLabels(tags railway.MetricsMetricsMetricsResultTagsMetricTags, targets []ServiceTarget) map[string]string {
	labels := map[string]string{"backfill": "true"}
	if tags.ProjectId != nil {
		labels["project_id"] = *tags.ProjectId
	}
	if tags.ServiceId != nil {
		labels["service_id"] = *tags.ServiceId
	}
	if tags.EnvironmentId != nil {
		labels["environment_id"] = *tags.EnvironmentId
	}
	if tags.Region != nil {
		labels["region"] = *tags.Region
	}
	for _, t := range targets {
		if tags.ServiceId != nil && t.ServiceID == *tags.ServiceId &&
			tags.EnvironmentId != nil && t.EnvironmentID == *tags.EnvironmentId {
			labels["project_name"] = t.ProjectName
			labels["service_name"] = t.ServiceName
			labels["environment_name"] = t.EnvironmentName
			break
		}
	}
	return labels
}

// ChunkTimeRange splits a TimeRange into chunks of at most chunkSize.
func ChunkTimeRange(tr TimeRange, chunkSize time.Duration) []TimeRange {
	var chunks []TimeRange
	cursor := tr.Start
	for cursor.Before(tr.End) {
		end := cursor.Add(chunkSize)
		if end.After(tr.End) {
			end = tr.End
		}
		chunks = append(chunks, TimeRange{Start: cursor, End: end})
		cursor = end
	}
	return chunks
}
