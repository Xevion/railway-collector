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

// BackfillGeneratorConfig configures a BackfillGenerator.
type BackfillGeneratorConfig struct {
	Discovery       TargetProvider
	Store           StateStore
	Sinks           []sink.Sink
	Clock           clockwork.Clock
	Measurements    []railway.MetricMeasurement
	SampleRate      int
	AvgWindow       int
	MetricRetention time.Duration // how far back to backfill metrics (e.g. 90 days)
	LogRetention    time.Duration // how far back to backfill env logs (e.g. 5 days)
	ChunkSize       time.Duration // time-aligned chunk size (e.g. 6 hours)
	Interval        time.Duration // minimum time between polls
	MaxItemsPerPoll int           // max work items to emit per poll
	LogLimit        int           // max log entries per request
	Logger          *slog.Logger
}

// BackfillGenerator implements TaskGenerator for backfill collection.
// It scans coverage gaps, aligns them to fixed chunk boundaries, and emits
// WorkItems that can be batched with other projects sharing the same chunk.
type BackfillGenerator struct {
	discovery       TargetProvider
	store           StateStore
	sinks           []sink.Sink
	clock           clockwork.Clock
	measurements    []railway.MetricMeasurement
	sampleRate      int
	avgWindow       int
	metricRetention time.Duration
	logRetention    time.Duration
	chunkSize       time.Duration
	interval        time.Duration
	maxItemsPerPoll int
	logLimit        int
	logger          *slog.Logger

	nextPoll     time.Time
	pendingItems []WorkItem      // cached items re-emitted until consumed via Deliver
	delivered    map[string]bool // tracks which pending items have been delivered
}

// NewBackfillGenerator creates a BackfillGenerator.
func NewBackfillGenerator(cfg BackfillGeneratorConfig) *BackfillGenerator {
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
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 6 * time.Hour
	}
	if cfg.MetricRetention == 0 {
		cfg.MetricRetention = 90 * 24 * time.Hour
	}
	if cfg.LogRetention == 0 {
		cfg.LogRetention = 5 * 24 * time.Hour
	}
	if cfg.MaxItemsPerPoll == 0 {
		cfg.MaxItemsPerPoll = 10
	}
	if cfg.LogLimit == 0 {
		cfg.LogLimit = 5000
	}

	return &BackfillGenerator{
		discovery:       cfg.Discovery,
		store:           cfg.Store,
		sinks:           cfg.Sinks,
		clock:           cfg.Clock,
		measurements:    measurements,
		sampleRate:      cfg.SampleRate,
		avgWindow:       cfg.AvgWindow,
		metricRetention: cfg.MetricRetention,
		logRetention:    cfg.LogRetention,
		chunkSize:       cfg.ChunkSize,
		interval:        cfg.Interval,
		maxItemsPerPoll: cfg.MaxItemsPerPoll,
		logLimit:        cfg.LogLimit,
		logger:          cfg.Logger,
	}
}

// Type returns TaskTypeBackfill.
func (g *BackfillGenerator) Type() TaskType {
	return TaskTypeBackfill
}

// AlignToChunkBoundary snaps a time down to the nearest chunk boundary
// (a multiple of chunkSize from the Unix epoch).
func AlignToChunkBoundary(t time.Time, chunkSize time.Duration) time.Time {
	chunkSecs := int64(chunkSize.Seconds())
	tSecs := t.Unix()
	aligned := tSecs - (tSecs % chunkSecs)
	return time.Unix(aligned, 0).UTC()
}

// backfillBatchKey computes a BatchKey from chunk boundaries and shared params,
// so that items for the same time window can be merged into one aliased request.
func (g *BackfillGenerator) backfillMetricBatchKey(chunkStart, chunkEnd time.Time) string {
	var parts []string
	for _, m := range g.measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("bf:sr=%d,aw=%d,m=%s,s=%s,e=%s",
		g.sampleRate, g.avgWindow, strings.Join(parts, "+"),
		chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))
}

// Poll scans all targets for coverage gaps, aligns them to chunk boundaries,
// and emits WorkItems. Projects with gaps in the same chunk share a BatchKey
// so they can be merged into one aliased request.
//
// Items are cached and re-emitted on subsequent ticks until consumed via
// Deliver. The poll interval only advances once all pending items are delivered,
// preventing work from being silently dropped when higher-priority batches win.
// NextPoll returns the earliest time this generator will produce work.
func (g *BackfillGenerator) NextPoll() time.Time { return g.nextPoll }

func (g *BackfillGenerator) Poll(now time.Time) []WorkItem {
	// Re-emit undelivered items from a previous poll.
	if len(g.pendingItems) > 0 {
		var remaining []WorkItem
		for _, item := range g.pendingItems {
			if !g.delivered[item.ID] {
				remaining = append(remaining, item)
			}
		}
		if len(remaining) > 0 {
			return remaining
		}
		// All pending items delivered; clear state and advance the poll gate.
		g.pendingItems = nil
		g.delivered = nil
		if g.interval > 0 {
			g.nextPoll = now.Add(g.interval)
		}
	}

	if now.Before(g.nextPoll) {
		return nil
	}

	targets := g.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	var items []WorkItem

	// Phase 1: Metric backfill
	items = append(items, g.pollMetricGaps(now, targets)...)

	// Phase 2: Environment log backfill
	items = append(items, g.pollEnvLogGaps(now, targets)...)

	if len(items) == 0 {
		return nil
	}

	// Cache items; nextPoll advances only when all are delivered.
	g.pendingItems = items
	g.delivered = make(map[string]bool, len(items))
	return items
}

func (g *BackfillGenerator) pollMetricGaps(now time.Time, targets []ServiceTarget) []WorkItem {
	metricRetentionStart := now.Add(-g.metricRetention)
	projectIDs := uniqueProjectIDs(targets)
	itemCount := 0

	groupBy := []railway.MetricTag{
		railway.MetricTagServiceId,
		railway.MetricTagEnvironmentId,
	}

	var items []WorkItem
	for _, projectID := range projectIDs {
		if itemCount >= g.maxItemsPerPoll {
			break
		}

		coverageKey := CoverageKey(projectID, "metric")
		existing, err := LoadCoverage(g.store, coverageKey)
		if err != nil {
			g.logger.Warn("failed to load metric coverage for backfill",
				"project_id", projectID, "error", err)
			continue
		}

		gaps := FindGaps(existing, metricRetentionStart, now)
		if len(gaps) == 0 {
			continue
		}

		prioritized := PrioritizeGaps(gaps, now)

		for _, gap := range prioritized {
			if itemCount >= g.maxItemsPerPoll {
				break
			}

			// Align gap to chunk boundaries
			chunks := g.alignedChunks(gap)
			for _, chunk := range chunks {
				if itemCount >= g.maxItemsPerPoll {
					break
				}

				batchKey := g.backfillMetricBatchKey(chunk.Start, chunk.End)

				items = append(items, WorkItem{
					ID:       fmt.Sprintf("backfill:metric:%s:%s", projectID, chunk.Start.Format(time.RFC3339)),
					Kind:     QueryMetrics,
					TaskType: TaskTypeBackfill,
					AliasKey: projectID,
					BatchKey: batchKey,
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

	return items
}

func (g *BackfillGenerator) pollEnvLogGaps(now time.Time, targets []ServiceTarget) []WorkItem {
	logRetentionStart := now.Add(-g.logRetention)

	// Deduplicate environments
	envSet := make(map[string]bool)
	var items []WorkItem

	for _, t := range targets {
		if envSet[t.EnvironmentID] {
			continue
		}
		envSet[t.EnvironmentID] = true

		coverageKey := CoverageKey(t.EnvironmentID, "log", "environment")
		existing, err := LoadCoverage(g.store, coverageKey)
		if err != nil {
			g.logger.Warn("failed to load log coverage for backfill",
				"environment_id", t.EnvironmentID, "error", err)
			continue
		}

		gaps := FindGaps(existing, logRetentionStart, now)
		if len(gaps) == 0 {
			continue
		}

		prioritized := PrioritizeGaps(gaps, now)

		for _, gap := range prioritized {
			if len(items) >= g.maxItemsPerPoll {
				break
			}

			// Align to chunk boundaries for batching
			chunks := g.alignedChunks(gap)
			for _, chunk := range chunks {
				if len(items) >= g.maxItemsPerPoll {
					break
				}

				batchKey := fmt.Sprintf("bf:envlog:limit=%d,s=%s,e=%s",
					g.logLimit, chunk.Start.Format(time.RFC3339), chunk.End.Format(time.RFC3339))

				items = append(items, WorkItem{
					ID:       fmt.Sprintf("backfill:envlog:%s:%s", t.EnvironmentID, chunk.Start.Format(time.RFC3339)),
					Kind:     QueryEnvironmentLogs,
					TaskType: TaskTypeBackfill,
					AliasKey: t.EnvironmentID,
					BatchKey: batchKey,
					Params: map[string]any{
						"afterDate":  chunk.Start.Format(time.RFC3339Nano),
						"beforeDate": chunk.End.Format(time.RFC3339Nano),
						"afterLimit": g.logLimit,
						"backfill":   "true",
					},
				})
			}
		}
	}

	return items
}

// alignedChunks splits a gap into chunks aligned to fixed boundaries.
func (g *BackfillGenerator) alignedChunks(gap TimeRange) []TimeRange {
	// Snap down to the nearest chunk boundary so that projects with overlapping
	// gaps produce identical chunk windows and can be merged into one request.
	// Coverage tracking handles any overlap with already-collected data.
	alignedStart := AlignToChunkBoundary(gap.Start, g.chunkSize)

	var chunks []TimeRange
	cursor := alignedStart
	for cursor.Before(gap.End) {
		end := cursor.Add(g.chunkSize)
		if end.After(gap.End) {
			// Extend to the full chunk boundary for alignment
			// (coverage tracking will handle the overlap)
			alignedEnd := AlignToChunkBoundary(gap.End, g.chunkSize).Add(g.chunkSize)
			if alignedEnd.After(gap.End) {
				end = gap.End
			}
		}
		chunks = append(chunks, TimeRange{Start: cursor, End: end})
		cursor = end
	}

	return chunks
}

// Deliver processes results from a backfill query.
// Routes to metric or log delivery based on WorkItem Kind.
func (g *BackfillGenerator) Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error) {
	// Mark item as consumed so Poll stops re-emitting it.
	if g.delivered != nil {
		g.delivered[item.ID] = true
	}

	if err != nil {
		g.logger.Error("backfill delivery failed",
			"kind", item.Kind, "alias_key", item.AliasKey, "error", err)
		return
	}

	switch item.Kind {
	case QueryMetrics:
		g.deliverMetrics(ctx, item, data)
	case QueryEnvironmentLogs:
		g.deliverEnvLogs(ctx, item, data)
	default:
		g.logger.Error("unknown backfill work item kind", "kind", item.Kind)
	}
}

func (g *BackfillGenerator) deliverMetrics(ctx context.Context, item WorkItem, data json.RawMessage) {
	projectID := item.AliasKey
	targets := g.discovery.Targets()

	projectName := ""
	for _, t := range targets {
		if t.ProjectID == projectID {
			projectName = t.ProjectName
			break
		}
	}

	var results []rawMetricsResult
	if unmarshalErr := json.Unmarshal(data, &results); unmarshalErr != nil {
		g.logger.Error("failed to parse backfill metrics response",
			"project", projectName, "project_id", projectID, "error", unmarshalErr)
		return
	}

	// Transform to MetricPoints
	var points []sink.MetricPoint
	for _, result := range results {
		metricName := rawMeasurementToPrometheusName(result.Measurement)
		labels := g.buildBackfillLabelsFromRaw(result.Tags, targets)

		for _, v := range result.Values {
			points = append(points, sink.MetricPoint{
				Name:      metricName,
				Value:     v.Value,
				Timestamp: time.Unix(int64(v.Ts), 0),
				Labels:    labels,
			})
		}
	}

	g.logger.Debug("backfill metrics delivered",
		"project", projectName, "project_id", projectID,
		"series", len(results), "points", len(points))

	// Write to sinks
	if len(points) > 0 {
		for _, s := range g.sinks {
			if sinkErr := s.WriteMetrics(ctx, points); sinkErr != nil {
				g.logger.Error("failed to write backfill metrics to sink",
					"sink", s.Name(), "project", projectName, "project_id", projectID, "error", sinkErr)
			}
		}
	}

	// Update coverage
	startDateStr, _ := item.Params["startDate"].(string)
	endDateStr, _ := item.Params["endDate"].(string)
	startTime, _ := time.Parse(time.RFC3339, startDateStr)
	endTime, _ := time.Parse(time.RFC3339, endDateStr)

	if startTime.IsZero() || endTime.IsZero() {
		return
	}

	coverageKey := CoverageKey(projectID, "metric")
	existing, covErr := LoadCoverage(g.store, coverageKey)
	if covErr != nil {
		g.logger.Warn("failed to load metric coverage",
			"project", projectName, "project_id", projectID, "error", covErr)
		return
	}

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
		g.logger.Warn("failed to save backfill metric coverage",
			"project", projectName, "project_id", projectID, "error", saveErr)
	}
}

func (g *BackfillGenerator) deliverEnvLogs(ctx context.Context, item WorkItem, data json.RawMessage) {
	envID := item.AliasKey

	services, envName := environmentServiceLookup(envID, g.discovery.Targets())

	var rawLogs []rawLogEntry
	if unmarshalErr := json.Unmarshal(data, &rawLogs); unmarshalErr != nil {
		g.logger.Error("failed to parse backfill environment logs",
			"environment", envName, "environment_id", envID, "error", unmarshalErr)
		return
	}

	var entries []sink.LogEntry
	for _, log := range rawLogs {
		ts, parseErr := time.Parse(time.RFC3339Nano, log.Timestamp)
		if parseErr != nil {
			continue
		}

		labels := map[string]string{
			"log_type":       "deployment",
			"environment_id": envID,
			"backfill":       "true",
		}

		if log.Tags != nil && log.Tags.ServiceID != nil {
			if target, ok := services[*log.Tags.ServiceID]; ok {
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
			Timestamp:  ts,
			Message:    log.Message,
			Severity:   sev,
			Labels:     labels,
			Attributes: attrs,
		})
	}

	g.logger.Debug("backfill env logs delivered",
		"environment", envName, "environment_id", envID, "entries", len(entries))

	if len(entries) > 0 {
		for _, s := range g.sinks {
			if sinkErr := s.WriteLogs(ctx, entries); sinkErr != nil {
				g.logger.Error("failed to write backfill logs to sink",
					"sink", s.Name(), "environment", envName, "error", sinkErr)
			}
		}
	}

	// Update coverage
	afterDateStr, _ := item.Params["afterDate"].(string)
	beforeDateStr, _ := item.Params["beforeDate"].(string)
	covStart, _ := time.Parse(time.RFC3339Nano, afterDateStr)
	covEnd, _ := time.Parse(time.RFC3339Nano, beforeDateStr)

	if covStart.IsZero() || covEnd.IsZero() {
		return
	}

	coverageKey := CoverageKey(envID, "log", "environment")
	existing, covErr := LoadCoverage(g.store, coverageKey)
	if covErr != nil {
		g.logger.Warn("failed to load log coverage",
			"environment", envName, "environment_id", envID, "error", covErr)
		return
	}

	kind := CoverageCollected
	if len(entries) == 0 {
		kind = CoverageEmpty
	}
	updated := InsertInterval(existing, CoverageInterval{
		Start: covStart, End: covEnd, Kind: kind,
	})
	if saveErr := SaveCoverage(g.store, coverageKey, updated); saveErr != nil {
		g.logger.Warn("failed to save backfill log coverage",
			"environment", envName, "environment_id", envID, "error", saveErr)
	}
}

// buildBackfillLabelsFromRaw builds metric labels from raw JSON tags
// with backfill=true, enriching with target names.
func (g *BackfillGenerator) buildBackfillLabelsFromRaw(tags rawMetricsTags, targets []ServiceTarget) map[string]string {
	return buildMetricLabels(tags, targets, true, false)
}
