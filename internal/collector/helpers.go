package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/collector/coverage"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/logging"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
)

// BaseMetricsConfig holds configuration fields shared across all metrics
// generator configs (project, service, replica).
type BaseMetricsConfig struct {
	Discovery       types.TargetProvider
	Store           types.StateStore
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

// baseMetrics holds runtime fields shared across all metrics generator structs.
type baseMetrics struct {
	discovery       types.TargetProvider
	store           types.StateStore
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

// applyConfigDefaults fills in zero-valued MetricRetention, ChunkSize, and
// MaxItemsPerPoll with sensible defaults, returning the resolved measurements
// slice (which may fall back to defaultMeasurements if cfg.Measurements is empty).
func applyConfigDefaults(cfg *BaseMetricsConfig, defaultMeasurements []railway.MetricMeasurement) []railway.MetricMeasurement {
	measurements := cfg.Measurements
	if len(measurements) == 0 {
		measurements = defaultMeasurements
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
	return measurements
}

// newBaseMetrics builds a baseMetrics from a BaseMetricsConfig and a resolved
// measurements slice. Call applyConfigDefaults first to fill in defaults.
func newBaseMetrics(cfg BaseMetricsConfig, measurements []railway.MetricMeasurement) baseMetrics {
	return baseMetrics{
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

var measurementMap = map[string]railway.MetricMeasurement{
	"cpu":            railway.MetricMeasurementCpuUsage,
	"cpu_limit":      railway.MetricMeasurementCpuLimit,
	"memory":         railway.MetricMeasurementMemoryUsageGb,
	"memory_limit":   railway.MetricMeasurementMemoryLimitGb,
	"network_rx":     railway.MetricMeasurementNetworkRxGb,
	"network_tx":     railway.MetricMeasurementNetworkTxGb,
	"disk":           railway.MetricMeasurementDiskUsageGb,
	"ephemeral_disk": railway.MetricMeasurementEphemeralDiskUsageGb,
	"backup":         railway.MetricMeasurementBackupUsageGb,
}

var metricNameMap = map[railway.MetricMeasurement]string{
	railway.MetricMeasurementCpuUsage:             "railway_cpu_usage_cores",
	railway.MetricMeasurementCpuLimit:             "railway_cpu_limit_cores",
	railway.MetricMeasurementMemoryUsageGb:        "railway_memory_usage_gb",
	railway.MetricMeasurementMemoryLimitGb:        "railway_memory_limit_gb",
	railway.MetricMeasurementNetworkRxGb:          "railway_network_rx_gb",
	railway.MetricMeasurementNetworkTxGb:          "railway_network_tx_gb",
	railway.MetricMeasurementDiskUsageGb:          "railway_disk_usage_gb",
	railway.MetricMeasurementEphemeralDiskUsageGb: "railway_ephemeral_disk_usage_gb",
	railway.MetricMeasurementBackupUsageGb:        "railway_backup_usage_gb",
}

// usageMetricSuffix maps measurement enum values to clean suffixes for usage
// metric names, avoiding redundancy like "railway_usage_cpu_usage".
var usageMetricSuffix = map[string]string{
	"CPU_USAGE":               "cpu",
	"CPU_LIMIT":               "cpu_limit",
	"MEMORY_USAGE_GB":         "memory_gb",
	"MEMORY_LIMIT_GB":         "memory_limit_gb",
	"NETWORK_RX_GB":           "network_rx_gb",
	"NETWORK_TX_GB":           "network_tx_gb",
	"DISK_USAGE_GB":           "disk_gb",
	"EPHEMERAL_DISK_USAGE_GB": "ephemeral_disk_gb",
	"BACKUP_USAGE_GB":         "backup_gb",
}

// usageMetricName returns a clean metric name for billing/usage measurements,
// using the given prefix (e.g. "railway_usage" or "railway_estimated_usage").
func usageMetricName(prefix, measurement string) string {
	if suffix, ok := usageMetricSuffix[measurement]; ok {
		return prefix + "_" + suffix
	}
	return prefix + "_" + strings.ToLower(measurement)
}

// compositeKeyInfo holds the IDs and names resolved from a composite key.
type compositeKeyInfo struct {
	serviceID, environmentID                             string
	serviceName, environmentName, projectName, projectID string
}

// parseCompositeKey splits a "serviceID:environmentID" composite key and
// looks up the human-readable names from the given targets slice.
func parseCompositeKey(key string, targets []types.ServiceTarget) compositeKeyInfo {
	parts := strings.SplitN(key, ":", 2)
	info := compositeKeyInfo{serviceID: parts[0]}
	if len(parts) > 1 {
		info.environmentID = parts[1]
	}
	for _, t := range targets {
		if t.ServiceID == info.serviceID && t.EnvironmentID == info.environmentID {
			info.serviceName = t.ServiceName
			info.environmentName = t.EnvironmentName
			info.projectName = t.ProjectName
			info.projectID = t.ProjectID
			break
		}
	}
	return info
}

func uniqueProjectIDs(targets []types.ServiceTarget) []string {
	seen := map[string]bool{}
	var ids []string
	for _, t := range targets {
		if !seen[t.ProjectID] {
			seen[t.ProjectID] = true
			ids = append(ids, t.ProjectID)
		}
	}
	return ids
}

// uniqueServiceEnvironments returns deduplicated ServiceTargets by (serviceID, environmentID) pair.
func uniqueServiceEnvironments(targets []types.ServiceTarget) []types.ServiceTarget {
	type key struct{ svc, env string }
	seen := map[key]bool{}
	var result []types.ServiceTarget
	for _, t := range targets {
		k := key{t.ServiceID, t.EnvironmentID}
		if !seen[k] {
			seen[k] = true
			result = append(result, t)
		}
	}
	return result
}

func copyLabels(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// pollEntity represents a single entity (project or service+environment) to
// scan for coverage gaps during a Poll cycle.
type pollEntity struct {
	Key          string // entity key for coverage lookup (projectID or compositeKey)
	CoverageType string // coverage type suffix (CoverageTypeMetric, etc.)
	LogAttrs     []any  // structured log key-value pairs for debug messages
	Data         any    // generator-specific data (e.g. types.ServiceTarget) to avoid re-querying discovery
}

// gapPollParams configures the shared gap-polling loop used by all metrics
// generators. The varying behavior is captured by the entities and buildItems
// callbacks.
type gapPollParams struct {
	store           types.StateStore
	discovery       types.TargetProvider
	logger          *slog.Logger
	metricRetention time.Duration
	chunkSize       time.Duration
	maxItemsPerPoll int
	nextPoll        time.Time // guard: skip if now < nextPoll

	// entities extracts the iteration list from discovered targets.
	entities func(targets []types.ServiceTarget) []pollEntity

	// buildItems constructs types.WorkItem(s) for a gap/chunk. Returns 1 item for
	// most generators, 2 for HTTP (duration + status pair). isLiveEdge tells
	// the callback whether to omit endDate or use a live batchKey.
	buildItems func(entity pollEntity, chunk coverage.TimeRange, isLiveEdge bool) []types.WorkItem

	// itemsPerEmit is the budget cost per buildItems call (1 for most, 2 for HTTP).
	itemsPerEmit int

	// logPrefix labels debug messages (e.g. "metric", "service metric").
	logPrefix string
}

// pollCoverageGaps implements the shared gap-walking loop used by all metrics
// generators' Poll methods. It iterates entities, loads coverage, finds gaps,
// prioritizes them, splits oversized live-edge gaps into chunks, and emits
// WorkItems via the buildItems callback.
func pollCoverageGaps(now time.Time, p gapPollParams) []types.WorkItem {
	if now.Before(p.nextPoll) {
		return nil
	}

	targets := p.discovery.Targets()
	if len(targets) == 0 {
		return nil
	}

	entities := p.entities(targets)
	if len(entities) == 0 {
		return nil
	}

	retentionStart := now.Add(-p.metricRetention)

	var items []types.WorkItem
	itemCount := 0

	for _, entity := range entities {
		if itemCount+p.itemsPerEmit > p.maxItemsPerPoll {
			break
		}

		coverageKey := coverage.CoverageKey(entity.Key, entity.CoverageType)
		existing, err := coverage.LoadCoverage(p.store, coverageKey)
		if err != nil {
			args := append([]any{"error", err}, entity.LogAttrs...)
			p.logger.Warn("failed to load "+p.logPrefix+" coverage", args...)
			continue
		}

		gaps := coverage.FindGaps(existing, retentionStart, now)
		if len(gaps) == 0 {
			continue
		}

		var totalGapDuration time.Duration
		for _, gap := range gaps {
			totalGapDuration += gap.End.Sub(gap.Start)
		}
		args := append([]any{
			"gaps", len(gaps),
			"total_gap_duration", totalGapDuration,
			"oldest_gap", gaps[0].Start.Format(time.RFC3339),
		}, entity.LogAttrs...)
		p.logger.Debug(p.logPrefix+" coverage gaps found", args...)

		prioritized := coverage.PrioritizeGaps(gaps, now)

		for _, gap := range prioritized {
			if itemCount+p.itemsPerEmit > p.maxItemsPerPoll {
				break
			}

			isLiveEdge := now.Sub(gap.End) < time.Minute

			if isLiveEdge {
				// If the gap is larger than one chunk, split the older
				// portion into fixed chunks and only keep the tail as
				// the live-edge query.
				gapDuration := gap.End.Sub(gap.Start)
				if gapDuration > p.chunkSize {
					olderGap := coverage.TimeRange{Start: gap.Start, End: gap.End.Add(-p.chunkSize)}
					chunks := alignedChunks(olderGap, p.chunkSize)
					for _, chunk := range chunks {
						if itemCount+p.itemsPerEmit > p.maxItemsPerPoll {
							break
						}
						built := p.buildItems(entity, chunk, false)
						items = append(items, built...)
						itemCount += p.itemsPerEmit
					}
					// Adjust the live-edge start to the tail
					gap = coverage.TimeRange{Start: gap.End.Add(-p.chunkSize), End: gap.End}
				}

				if itemCount+p.itemsPerEmit > p.maxItemsPerPoll {
					break
				}

				built := p.buildItems(entity, gap, true)
				items = append(items, built...)
				itemCount += p.itemsPerEmit
			} else {
				// Older gap: chunk into fixed intervals for batching
				chunks := alignedChunks(gap, p.chunkSize)
				for _, chunk := range chunks {
					if itemCount+p.itemsPerEmit > p.maxItemsPerPoll {
						break
					}
					built := p.buildItems(entity, chunk, false)
					items = append(items, built...)
					itemCount += p.itemsPerEmit
				}
			}
		}
	}

	return items
}

// deploymentBaseLabels finds the types.ServiceTarget matching a deployment ID and
// returns a base label map. Falls back to just {"deployment_id": id}.
func deploymentBaseLabels(deploymentID string, targets []types.ServiceTarget) map[string]string {
	for _, t := range targets {
		if t.DeploymentID == deploymentID {
			return map[string]string{
				"project_id":       t.ProjectID,
				"project_name":     t.ProjectName,
				"service_id":       t.ServiceID,
				"service_name":     t.ServiceName,
				"environment_id":   t.EnvironmentID,
				"environment_name": t.EnvironmentName,
				"deployment_id":    t.DeploymentID,
			}
		}
	}
	return map[string]string{"deployment_id": deploymentID}
}

// environmentServiceLookup builds a map of serviceID → types.ServiceTarget for all
// targets in the given environment, also returning the environment name.
func environmentServiceLookup(envID string, targets []types.ServiceTarget) (services map[string]types.ServiceTarget, envName string) {
	services = make(map[string]types.ServiceTarget)
	for _, t := range targets {
		if t.EnvironmentID == envID {
			services[t.ServiceID] = t
			if envName == "" {
				envName = t.EnvironmentName
			}
		}
	}
	return
}

// ResolveMeasurements converts human-friendly measurement names (e.g. "cpu", "memory")
// to the Railway API enum values. Unknown names are skipped.
func ResolveMeasurements(names []string) []railway.MetricMeasurement {
	var result []railway.MetricMeasurement
	for _, name := range names {
		if m, ok := measurementMap[name]; ok {
			result = append(result, m)
		}
	}
	return result
}

// updateCoverage loads existing coverage for a key, inserts a new interval, and saves.
// start must be non-zero; callers should guard with !start.IsZero() before calling.
// resolution is the sample rate in seconds (0 for logs or when unset).
func updateCoverage(store types.StateStore, coverageKey string, start, end time.Time, empty bool, resolution int) error {
	existing, err := coverage.LoadCoverage(store, coverageKey)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	kind := coverage.CoverageCollected
	if empty {
		kind = coverage.CoverageEmpty
	}
	updated := coverage.InsertInterval(existing, coverage.CoverageInterval{
		Start:      start,
		End:        end,
		Kind:       kind,
		Resolution: resolution,
	})
	if err := coverage.SaveCoverage(store, coverageKey, updated); err != nil {
		return fmt.Errorf("save: %w", err)
	}
	return nil
}

// metricsBatchKey computes a batch key from shared query parameters.
// Items with the same batch key can be merged into one aliased request.
func metricsBatchKey(measurements []railway.MetricMeasurement, sampleRate, avgWindow int) string {
	var parts []string
	for _, m := range measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("sr=%d,aw=%d,m=%s", sampleRate, avgWindow, strings.Join(parts, "+"))
}

// metricsBatchKeyChunk computes a batch key for a chunked (older gap) query.
// Includes chunk boundaries so that items for the same time window can be merged.
func metricsBatchKeyChunk(measurements []railway.MetricMeasurement, sampleRate, avgWindow int, chunkStart, chunkEnd time.Time) string {
	var parts []string
	for _, m := range measurements {
		parts = append(parts, string(m))
	}
	return fmt.Sprintf("sr=%d,aw=%d,m=%s,s=%s,e=%s",
		sampleRate, avgWindow, strings.Join(parts, "+"),
		chunkStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))
}

// deliveryLogLevel returns LevelTrace for empty deliveries, LevelDebug otherwise.
func deliveryLogLevel(count int) slog.Level {
	if count == 0 {
		return logging.LevelTrace
	}
	return slog.LevelDebug
}

// writeMetricsToSinks writes metric points to all sinks, logging errors.
// Does nothing if points is empty.
func writeMetricsToSinks(ctx context.Context, sinks []sink.Sink, points []sink.MetricPoint, logger *slog.Logger) {
	if len(points) == 0 {
		return
	}
	for _, s := range sinks {
		if err := s.WriteMetrics(ctx, points); err != nil {
			logger.Error("failed to write metrics to sink", "sink", s.Name(), "error", err)
		}
	}
}

// writeLogsToSinks writes log entries to all sinks, logging errors.
// Does nothing if entries is empty.
func writeLogsToSinks(ctx context.Context, sinks []sink.Sink, entries []sink.LogEntry, logger *slog.Logger) {
	if len(entries) == 0 {
		return
	}
	for _, s := range sinks {
		if err := s.WriteLogs(ctx, entries); err != nil {
			logger.Error("failed to write logs to sink", "sink", s.Name(), "error", err)
		}
	}
}
