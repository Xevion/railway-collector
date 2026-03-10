package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/logging"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
)

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

var prometheusNameMap = map[railway.MetricMeasurement]string{
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

type MetricsCollector struct {
	client       RailwayAPI
	discovery    TargetProvider
	sinks        []sink.Sink
	store        StateStore
	measurements []railway.MetricMeasurement
	sampleRate   *int
	avgWindow    *int
	lookback     time.Duration
	interval     time.Duration
	clock        clockwork.Clock
	firstRun     bool
	logger       *slog.Logger
}

func NewMetricsCollector(
	client RailwayAPI,
	discovery TargetProvider,
	sinks []sink.Sink,
	store StateStore,
	measurementNames []string,
	sampleRate, avgWindow int,
	lookback time.Duration,
	interval time.Duration,
	clock clockwork.Clock,
	logger *slog.Logger,
) *MetricsCollector {
	var measurements []railway.MetricMeasurement
	for _, name := range measurementNames {
		if m, ok := measurementMap[strings.ToLower(name)]; ok {
			measurements = append(measurements, m)
		} else {
			logger.Warn("unknown measurement name, skipping", "name", name)
		}
	}

	if len(measurements) == 0 {
		measurements = []railway.MetricMeasurement{
			railway.MetricMeasurementCpuUsage,
			railway.MetricMeasurementMemoryUsageGb,
			railway.MetricMeasurementNetworkRxGb,
			railway.MetricMeasurementNetworkTxGb,
			railway.MetricMeasurementDiskUsageGb,
		}
	}

	return &MetricsCollector{
		client:       client,
		discovery:    discovery,
		sinks:        sinks,
		store:        store,
		measurements: measurements,
		sampleRate:   &sampleRate,
		avgWindow:    &avgWindow,
		lookback:     lookback,
		interval:     interval,
		clock:        clock,
		firstRun:     true,
		logger:       logger,
	}
}

func (mc *MetricsCollector) Collect(ctx context.Context) error {
	targets := mc.discovery.Targets()
	if len(targets) == 0 {
		mc.logger.Debug("no targets for metrics collection")
		return nil
	}

	start := time.Now()
	now := mc.clock.Now().UTC()
	fallbackStart := now.Add(-mc.lookback)

	groupBy := []railway.MetricTag{
		railway.MetricTagServiceId,
		railway.MetricTagEnvironmentId,
	}

	projectIDs := uniqueProjectIDs(targets)

	// Skip jitter on the initial collection after startup
	applyJitter := !mc.firstRun
	if mc.firstRun {
		mc.firstRun = false
	}

	var (
		mu        sync.Mutex
		allPoints []sink.MetricPoint
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(4)

	for _, projectID := range projectIDs {
		g.Go(func() error {
			projectName := ""
			for _, t := range targets {
				if t.ProjectID == projectID {
					projectName = t.ProjectName
					break
				}
			}

			// Spread API calls across 80% of the interval to avoid thundering herd
			if applyJitter {
				maxJitter := time.Duration(float64(mc.interval) * 0.8)
				if maxJitter > 0 {
					jitter := time.Duration(rand.Int64N(int64(maxJitter)))
					select {
					case <-gCtx.Done():
						return gCtx.Err()
					case <-mc.clock.After(jitter):
					}
				}
			}

			// Use persisted cursor if available, otherwise fall back to lookback window
			startTime := mc.store.GetMetricCursor(projectID)
			if startTime.IsZero() || startTime.Before(fallbackStart) {
				startTime = fallbackStart
			}
			startDate := startTime.Format(time.RFC3339)

			resp, err := mc.client.GetMetrics(
				gCtx, &projectID, nil, nil,
				startDate, nil,
				mc.measurements,
				groupBy,
				mc.sampleRate, mc.avgWindow,
			)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					mc.logger.Debug("collection cancelled", "project", projectName, "project_id", projectID)
				} else {
					mc.logger.Error("failed to collect metrics", "project", projectName, "project_id", projectID, "error", err)
				}
				return nil
			}

			// Persist the current time as the cursor for next fetch
			if err := mc.store.SetMetricCursor(projectID, now); err != nil {
				mc.logger.Error("failed to persist metric cursor", "project", projectName, "project_id", projectID, "error", err)
			}

			// Record coverage interval for this fetch window
			coverageKey := CoverageKey(projectID, "metric")
			existing, err := LoadCoverage(mc.store, coverageKey)
			if err != nil {
				mc.logger.Warn("failed to load metric coverage", "project", projectName, "project_id", projectID, "error", err)
			} else {
				newInterval := CoverageInterval{
					Start:      startTime,
					End:        now,
					Kind:       CoverageCollected,
					Resolution: *mc.sampleRate,
				}
				if len(resp.Metrics) == 0 {
					newInterval.Kind = CoverageEmpty
				}
				updated := InsertInterval(existing, newInterval)
				if err := SaveCoverage(mc.store, coverageKey, updated); err != nil {
					mc.logger.Warn("failed to save metric coverage", "project", projectName, "project_id", projectID, "error", err)
				}
			}

			mc.logger.Debug("metrics API response", "project", projectName, "project_id", projectID, "results", len(resp.Metrics))

			var points []sink.MetricPoint
			for _, result := range resp.Metrics {
				metricName, ok := prometheusNameMap[result.Measurement]
				if !ok {
					metricName = fmt.Sprintf("railway_%s", strings.ToLower(string(result.Measurement)))
				}

				labels := mc.buildLabels(result.Tags, targets)

				if len(result.Values) == 0 {
					mc.logger.Log(gCtx, logging.LevelTrace, "metric returned no values",
						"measurement", result.Measurement,
						"metric", metricName,
						"service_id", labels["service_id"],
						"service_name", labels["service_name"],
					)
					continue
				}

				mc.logger.Log(gCtx, logging.LevelTrace, "metric data points",
					"metric", metricName,
					"count", len(result.Values),
					"service_name", labels["service_name"],
				)
				for _, v := range result.Values {
					points = append(points, sink.MetricPoint{
						Name:      metricName,
						Value:     v.Value,
						Timestamp: time.Unix(int64(v.Ts), 0),
						Labels:    labels,
					})
				}
			}

			mu.Lock()
			allPoints = append(allPoints, points...)
			mu.Unlock()

			return nil
		})
	}

	_ = g.Wait()

	mc.logger.Info("collected metric points", "count", len(allPoints), "projects", len(projectIDs), "duration", time.Since(start))

	// Use a fresh bounded context for sink writes if the original was cancelled,
	// so we don't lose data that was already collected.
	sinkCtx := ctx
	if ctx.Err() != nil {
		mc.logger.Info("original context cancelled, flushing collected data with 10s deadline", "points", len(allPoints))
		var sinkCancel context.CancelFunc
		sinkCtx, sinkCancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer sinkCancel()
	}

	for _, s := range mc.sinks {
		if err := s.WriteMetrics(sinkCtx, allPoints); err != nil {
			mc.logger.Error("failed to write metrics to sink", "sink", s.Name(), "error", err)
		} else {
			mc.logger.Debug("wrote metrics to sink", "sink", s.Name(), "points", len(allPoints))
		}
	}

	if ctx.Err() != nil {
		if sinkCtx.Err() != nil {
			mc.logger.Warn("sink flush may have been truncated by timeout", "points", len(allPoints))
		} else {
			mc.logger.Info("sink flush completed successfully during shutdown", "points", len(allPoints))
		}
	}

	return nil
}

func (mc *MetricsCollector) buildLabels(tags railway.MetricsMetricsMetricsResultTagsMetricTags, targets []ServiceTarget) map[string]string {
	labels := map[string]string{}

	if tags.ProjectId != nil {
		labels["project_id"] = *tags.ProjectId
	}
	if tags.ServiceId != nil {
		labels["service_id"] = *tags.ServiceId
	}
	if tags.EnvironmentId != nil {
		labels["environment_id"] = *tags.EnvironmentId
	}
	if tags.DeploymentId != nil {
		labels["deployment_id"] = *tags.DeploymentId
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

func uniqueProjectIDs(targets []ServiceTarget) []string {
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
