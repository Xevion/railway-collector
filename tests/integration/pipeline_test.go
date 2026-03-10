package integration

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/mocks"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
	"github.com/xevion/railway-collector/internal/state"
	"go.uber.org/mock/gomock"
)

func strPtr(s string) *string { return &s }

// recordingSink captures all writes for assertion.
type recordingSink struct {
	mu      sync.Mutex
	metrics []sink.MetricPoint
	logs    []sink.LogEntry
}

func (r *recordingSink) Name() string { return "recording" }
func (r *recordingSink) WriteMetrics(_ context.Context, pts []sink.MetricPoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics = append(r.metrics, pts...)
	return nil
}
func (r *recordingSink) WriteLogs(_ context.Context, entries []sink.LogEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = append(r.logs, entries...)
	return nil
}
func (r *recordingSink) Close() error { return nil }

func openTestStore(t *testing.T) *state.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	store, err := state.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { store.Close() })
	return store
}

func dumpOnFailure(t *testing.T, rec *recordingSink) {
	t.Helper()
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		rec.mu.Lock()
		defer rec.mu.Unlock()
		t.Logf("--- recorded %d metric(s) ---", len(rec.metrics))
		for i, m := range rec.metrics {
			t.Logf("  [%d] %s=%.4f labels=%v ts=%s", i, m.Name, m.Value, m.Labels, m.Timestamp.Format(time.RFC3339))
		}
		t.Logf("--- recorded %d log(s) ---", len(rec.logs))
		for i, l := range rec.logs {
			t.Logf("  [%d] msg=%q sev=%s labels=%v", i, l.Message, l.Severity, l.Labels)
		}
	})
}

func filterMetricsByName(pts []sink.MetricPoint, name string) []sink.MetricPoint {
	var result []sink.MetricPoint
	for _, p := range pts {
		if p.Name == name {
			result = append(result, p)
		}
	}
	return result
}

func TestFullPipeline_FirstRunBackfill(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	disc := mocks.NewMockTargetProvider(ctrl)
	store := openTestStore(t)
	rec := &recordingSink{}
	dumpOnFailure(t, rec)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	target := collector.ServiceTarget{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "web",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}
	disc.EXPECT().Targets().Return([]collector.ServiceTarget{target}).AnyTimes()

	// No cursor in store -- MetricsCollector falls back to lookback window (10 min).
	// The collector will call GetMetrics with startDate = now - 10min.
	lookback := 10 * time.Minute
	expectedStart := now.Add(-lookback).Format(time.RFC3339)
	sampleRate := 30
	avgWindow := 30

	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&target.ProjectID), gomock.Nil(), gomock.Nil(),
		gomock.Eq(expectedStart),
		gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{
			{
				Measurement: railway.MetricMeasurementCpuUsage,
				Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
					ServiceId:     strPtr("svc-1"),
					EnvironmentId: strPtr("env-1"),
				},
				Values: []railway.MetricsMetricsMetricsResultValuesMetric{
					{Ts: int(now.Add(-8 * time.Minute).Unix()), Value: 0.3},
					{Ts: int(now.Add(-5 * time.Minute).Unix()), Value: 0.5},
					{Ts: int(now.Add(-2 * time.Minute).Unix()), Value: 0.7},
				},
			},
			{
				Measurement: railway.MetricMeasurementMemoryUsageGb,
				Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
					ServiceId:     strPtr("svc-1"),
					EnvironmentId: strPtr("env-1"),
				},
				Values: []railway.MetricsMetricsMetricsResultValuesMetric{
					{Ts: int(now.Add(-5 * time.Minute).Unix()), Value: 1.2},
				},
			},
		},
	}, nil)

	mc := collector.NewMetricsCollector(
		api, disc, []sink.Sink{rec}, store,
		[]string{"cpu", "memory"},
		30, 30,
		lookback,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)

	// Should have all 4 data points: 3 CPU values + 1 memory value
	assert.Len(t, rec.metrics, 4)

	// Verify metric breakdown by measurement type
	cpuPoints := filterMetricsByName(rec.metrics, "railway_cpu_usage_cores")
	assert.Len(t, cpuPoints, 3, "should have 3 CPU data points")
	memPoints := filterMetricsByName(rec.metrics, "railway_memory_usage_gb")
	assert.Len(t, memPoints, 1, "should have 1 memory data point")

	// Verify labels are enriched from target
	for _, pt := range rec.metrics {
		assert.Equal(t, "svc-1", pt.Labels["service_id"], "metric should have service_id label")
		assert.Equal(t, "env-1", pt.Labels["environment_id"], "metric should have environment_id label")
		assert.Equal(t, "web", pt.Labels["service_name"], "metric should have service_name label")
		assert.Equal(t, "test-project", pt.Labels["project_name"], "metric should have project_name label")
		assert.Equal(t, "production", pt.Labels["environment_name"], "metric should have environment_name label")
	}

	// Verify timestamps are correct
	assert.Equal(t, now.Add(-8*time.Minute).Unix(), cpuPoints[0].Timestamp.Unix())
	assert.Equal(t, now.Add(-5*time.Minute).Unix(), cpuPoints[1].Timestamp.Unix())
	assert.Equal(t, now.Add(-2*time.Minute).Unix(), cpuPoints[2].Timestamp.Unix())
	assert.Equal(t, 1.2, memPoints[0].Value)

	// Cursor should be persisted for next run
	cursor := store.GetMetricCursor("proj-1")
	assert.False(t, cursor.IsZero(), "metric cursor should be set after first collection")
}

func TestFullPipeline_SteadyStateCollection(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	disc := mocks.NewMockTargetProvider(ctrl)
	store := openTestStore(t)
	rec := &recordingSink{}
	dumpOnFailure(t, rec)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	target := collector.ServiceTarget{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "web",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}
	disc.EXPECT().Targets().Return([]collector.ServiceTarget{target}).AnyTimes()

	// Pre-populate cursor -- simulates previous collection
	prevCursor := now.Add(-5 * time.Minute)
	require.NoError(t, store.SetMetricCursor("proj-1", prevCursor))

	sampleRate := 30
	avgWindow := 30

	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&target.ProjectID), gomock.Nil(), gomock.Nil(),
		gomock.Eq(prevCursor.Format(time.RFC3339)),
		gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{{
			Measurement: railway.MetricMeasurementCpuUsage,
			Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
				ServiceId:     strPtr("svc-1"),
				EnvironmentId: strPtr("env-1"),
			},
			Values: []railway.MetricsMetricsMetricsResultValuesMetric{
				{Ts: int(now.Add(-2 * time.Minute).Unix()), Value: 0.4},
			},
		}},
	}, nil)

	mc := collector.NewMetricsCollector(
		api, disc, []sink.Sink{rec}, store,
		[]string{"cpu"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)

	assert.Len(t, rec.metrics, 1)
	assert.Equal(t, 0.4, rec.metrics[0].Value)
	assert.Equal(t, "railway_cpu_usage_cores", rec.metrics[0].Name)
	assert.Equal(t, "svc-1", rec.metrics[0].Labels["service_id"])
	assert.Equal(t, now.Add(-2*time.Minute).Unix(), rec.metrics[0].Timestamp.Unix())

	// Cursor should advance to "now"
	cursor := store.GetMetricCursor("proj-1")
	assert.Equal(t, now.UTC(), cursor.UTC(), "cursor should advance to current time")
}

func TestFullPipeline_PartialAPIFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	disc := mocks.NewMockTargetProvider(ctrl)
	store := openTestStore(t)
	rec := &recordingSink{}
	dumpOnFailure(t, rec)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	// Two targets in different projects -- one API call succeeds, one fails
	target1 := collector.ServiceTarget{
		ProjectID: "proj-1", ProjectName: "project-one",
		ServiceID: "svc-1", ServiceName: "web",
		EnvironmentID: "env-1", EnvironmentName: "production",
		DeploymentID: "dep-1",
	}
	target2 := collector.ServiceTarget{
		ProjectID: "proj-2", ProjectName: "project-two",
		ServiceID: "svc-2", ServiceName: "api",
		EnvironmentID: "env-2", EnvironmentName: "staging",
		DeploymentID: "dep-2",
	}
	disc.EXPECT().Targets().Return([]collector.ServiceTarget{target1, target2}).AnyTimes()

	sampleRate := 30
	avgWindow := 30

	// proj-1 succeeds
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&target1.ProjectID), gomock.Nil(), gomock.Nil(),
		gomock.Any(), gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{{
			Measurement: railway.MetricMeasurementCpuUsage,
			Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
				ServiceId:     strPtr("svc-1"),
				EnvironmentId: strPtr("env-1"),
			},
			Values: []railway.MetricsMetricsMetricsResultValuesMetric{
				{Ts: int(now.Add(-1 * time.Minute).Unix()), Value: 0.8},
			},
		}},
	}, nil)

	// proj-2 fails with API error
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&target2.ProjectID), gomock.Nil(), gomock.Nil(),
		gomock.Any(), gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(nil, assert.AnError)

	mc := collector.NewMetricsCollector(
		api, disc, []sink.Sink{rec}, store,
		[]string{"cpu"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err) // Collect returns nil even on partial failure

	// Only proj-1 data should appear in sink
	assert.Len(t, rec.metrics, 1)
	assert.Equal(t, 0.8, rec.metrics[0].Value)
	// Verify the metric came from the successful project, not the failed one
	assert.Equal(t, "svc-1", rec.metrics[0].Labels["service_id"],
		"metric should come from proj-1's service")
	assert.Equal(t, "project-one", rec.metrics[0].Labels["project_name"],
		"metric should come from the successful project")

	// Cursor should be set for proj-1 (succeeded) but not proj-2 (failed)
	cursor1 := store.GetMetricCursor("proj-1")
	assert.False(t, cursor1.IsZero(), "proj-1 cursor should be set")
	cursor2 := store.GetMetricCursor("proj-2")
	assert.True(t, cursor2.IsZero(), "proj-2 cursor should NOT be set after failure")
}

func TestFullPipeline_DeploymentRollover(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	disc := mocks.NewMockTargetProvider(ctrl)
	store := openTestStore(t)
	rec := &recordingSink{}
	dumpOnFailure(t, rec)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	// First collection: dep-1
	target1 := collector.ServiceTarget{
		ProjectID: "proj-1", ProjectName: "test-project",
		ServiceID: "svc-1", ServiceName: "web",
		EnvironmentID: "env-1", EnvironmentName: "production",
		DeploymentID: "dep-1",
	}

	disc.EXPECT().Targets().Return([]collector.ServiceTarget{target1}).Times(1)

	logTs1 := now.Add(-3 * time.Minute)
	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(),
		gomock.Eq("env-1"),
		gomock.Nil(), gomock.Nil(), gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{
		EnvironmentLogs: []railway.EnvironmentLogsQueryEnvironmentLogsLog{{
			Timestamp: logTs1.Format(time.RFC3339Nano),
			Message:   "dep-1 log",
			Tags: &railway.EnvironmentLogsQueryEnvironmentLogsLogTags{
				ServiceId:    strPtr("svc-1"),
				DeploymentId: strPtr("dep-1"),
			},
		}},
	}, nil).Times(1)

	lc := collector.NewLogsCollector(
		api, disc, []sink.Sink{rec},
		[]string{"deployment"},
		100,
		5*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, rec.logs, 1)
	assert.Equal(t, "dep-1 log", rec.logs[0].Message)
	assert.Equal(t, "deployment", rec.logs[0].Labels["log_type"])
	assert.Equal(t, "env-1", rec.logs[0].Labels["environment_id"])

	// Verify cursor was set for env-1 (environment-level, not deployment-level)
	envCursor := store.GetLogCursor("env-1", "environment")
	assert.Equal(t, logTs1.UTC(), envCursor.UTC())

	// Second collection: dep-2 (deployment rollover via discovery)
	target2 := collector.ServiceTarget{
		ProjectID: "proj-1", ProjectName: "test-project",
		ServiceID: "svc-1", ServiceName: "web",
		EnvironmentID: "env-1", EnvironmentName: "production",
		DeploymentID: "dep-2",
	}

	disc.EXPECT().Targets().Return([]collector.ServiceTarget{target2}).Times(1)

	logTs2 := now.Add(-1 * time.Minute)
	// The environment log cursor means this call passes afterDate = logTs1
	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(),
		gomock.Eq("env-1"),
		gomock.Nil(),
		gomock.Any(), // afterDate should be set from cursor
		gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{
		EnvironmentLogs: []railway.EnvironmentLogsQueryEnvironmentLogsLog{{
			Timestamp: logTs2.Format(time.RFC3339Nano),
			Message:   "dep-2 log",
			Tags: &railway.EnvironmentLogsQueryEnvironmentLogsLogTags{
				ServiceId:    strPtr("svc-1"),
				DeploymentId: strPtr("dep-2"),
			},
		}},
	}, nil).Times(1)

	// Create a new LogsCollector for the second collection (simulates next cycle)
	lc2 := collector.NewLogsCollector(
		api, disc, []sink.Sink{rec},
		[]string{"deployment"},
		100,
		5*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err = lc2.Collect(context.Background())
	require.NoError(t, err)

	// Should now have 2 total logs (from both collections)
	assert.Len(t, rec.logs, 2)
	assert.Equal(t, "dep-2 log", rec.logs[1].Message)
	assert.Equal(t, "deployment", rec.logs[1].Labels["log_type"])

	// Cursor should advance to the newer timestamp
	envCursor2 := store.GetLogCursor("env-1", "environment")
	assert.Equal(t, logTs2.UTC(), envCursor2.UTC())

	// The key insight: environment logs use environment-level cursors,
	// so deployment rollover naturally works -- the cursor tracks
	// what we've seen from the environment, not the deployment.
}
