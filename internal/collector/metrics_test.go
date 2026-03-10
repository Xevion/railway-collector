package collector_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/mocks"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
	"go.uber.org/mock/gomock"
)

func TestMetricsCollector_Collect_SteadyState(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	cursorTime := now.Add(-5 * time.Minute)
	projectID := "proj-1"

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       projectID,
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	store.EXPECT().GetMetricCursor(projectID).Return(cursorTime)

	sampleRate := 30
	avgWindow := 30
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&projectID), gomock.Nil(), gomock.Nil(),
		gomock.Eq(cursorTime.Format(time.RFC3339)),
		gomock.Nil(),
		gomock.Any(),
		gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{{
			Measurement: railway.MetricMeasurementMemoryUsageGb,
			Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
				ServiceId:     strPtr("svc-1"),
				EnvironmentId: strPtr("env-1"),
			},
			Values: []railway.MetricsMetricsMetricsResultValuesMetric{
				{Ts: int(now.Add(-2 * time.Minute).Unix()), Value: 0.5},
				{Ts: int(now.Add(-1 * time.Minute).Unix()), Value: 0.6},
			},
		}},
	}, nil)

	store.EXPECT().SetMetricCursor(projectID, now.UTC()).Return(nil)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	mc := collector.NewMetricsCollector(
		api, targets, []sink.Sink{fakeSink}, store,
		[]string{"memory"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 2)
	assert.Equal(t, "railway_memory_usage_gb", collected[0].Name)
	assert.Equal(t, 0.5, collected[0].Value) // first point
	assert.Equal(t, "railway_memory_usage_gb", collected[1].Name)
	assert.Equal(t, 0.6, collected[1].Value) // second point

	// Label assertions
	assert.Equal(t, "svc-1", collected[0].Labels["service_id"])
	assert.Equal(t, "env-1", collected[0].Labels["environment_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
	assert.Equal(t, "production", collected[0].Labels["environment_name"])

	// Timestamp assertions
	assert.Equal(t, now.Add(-2*time.Minute).Unix(), collected[0].Timestamp.Unix())
	assert.Equal(t, now.Add(-1*time.Minute).Unix(), collected[1].Timestamp.Unix())
}

func TestMetricsCollector_Collect_FirstRunNoCursor(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	lookback := 10 * time.Minute
	expectedStart := now.Add(-lookback)
	projectID := "proj-1"

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       projectID,
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}})

	// No cursor exists
	store.EXPECT().GetMetricCursor(projectID).Return(time.Time{})

	sampleRate := 30
	avgWindow := 30
	// Should use lookback-based start time
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&projectID), gomock.Nil(), gomock.Nil(),
		gomock.Eq(expectedStart.Format(time.RFC3339)),
		gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{{
			Measurement: railway.MetricMeasurementMemoryUsageGb,
			Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
				ServiceId:     strPtr("svc-1"),
				EnvironmentId: strPtr("env-1"),
			},
			Values: []railway.MetricsMetricsMetricsResultValuesMetric{
				{Ts: int(now.Add(-5 * time.Minute).Unix()), Value: 0.5},
			},
		}},
	}, nil)

	store.EXPECT().SetMetricCursor(projectID, now.UTC()).Return(nil)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	mc := collector.NewMetricsCollector(
		api, targets, []sink.Sink{fakeSink}, store,
		[]string{"memory"},
		30, 30,
		lookback,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 1)
}

func TestMetricsCollector_Collect_APIError(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	projectID := "proj-1"
	cursorTime := fakeClock.Now().Add(-5 * time.Minute)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       projectID,
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}})

	store.EXPECT().GetMetricCursor(projectID).Return(cursorTime)

	// API returns an error
	api.EXPECT().GetMetrics(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(),
	).Return(nil, context.DeadlineExceeded)

	// SetMetricCursor should NOT be called on API error
	store.EXPECT().SetMetricCursor(gomock.Any(), gomock.Any()).Times(0)

	fakeSink := &recordingSink{}

	mc := collector.NewMetricsCollector(
		api, targets, []sink.Sink{fakeSink}, store,
		[]string{"memory"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err) // Collect returns nil even on API errors (it logs them)
}

func TestMetricsCollector_Collect_RecordsCoverage(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	cursorTime := now.Add(-5 * time.Minute)
	projectID := "proj-1"

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       projectID,
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	store.EXPECT().GetMetricCursor(projectID).Return(cursorTime)

	sampleRate := 30
	avgWindow := 30
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&projectID), gomock.Nil(), gomock.Nil(),
		gomock.Eq(cursorTime.Format(time.RFC3339)),
		gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{{
			Measurement: railway.MetricMeasurementMemoryUsageGb,
			Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
				ServiceId:     strPtr("svc-1"),
				EnvironmentId: strPtr("env-1"),
			},
			Values: []railway.MetricsMetricsMetricsResultValuesMetric{
				{Ts: int(now.Add(-2 * time.Minute).Unix()), Value: 0.5},
			},
		}},
	}, nil)

	store.EXPECT().SetMetricCursor(projectID, now.UTC()).Return(nil)

	// Expect coverage to be recorded: GetCoverage then SetCoverage
	coverageKey := collector.CoverageKey(projectID, "metric")
	store.EXPECT().GetCoverage(coverageKey).Return(nil, nil)
	store.EXPECT().SetCoverage(coverageKey, gomock.Any()).Return(nil)

	fakeSink := &recordingSink{}

	mc := collector.NewMetricsCollector(
		api, targets, []sink.Sink{fakeSink}, store,
		[]string{"memory"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)
}

func TestMetricsCollector_Collect_EmptyResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	projectID := "proj-1"
	cursorTime := now.Add(-5 * time.Minute)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       projectID,
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}})

	store.EXPECT().GetMetricCursor(projectID).Return(cursorTime)

	// API returns empty metrics
	sampleRate := 30
	avgWindow := 30
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&projectID), gomock.Nil(), gomock.Nil(),
		gomock.Eq(cursorTime.Format(time.RFC3339)),
		gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{},
	}, nil)

	// Cursor advances even on empty response (API succeeded)
	store.EXPECT().SetMetricCursor(projectID, now.UTC()).Return(nil)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	fakeSink := &recordingSink{}

	mc := collector.NewMetricsCollector(
		api, targets, []sink.Sink{fakeSink}, store,
		[]string{"memory"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)
	// No sink writes expected (no points collected)
}

func TestMetricsCollector_Collect_MultipleProjects(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	proj1 := "proj-1"
	proj2 := "proj-2"
	cursor1 := now.Add(-5 * time.Minute)
	cursor2 := now.Add(-3 * time.Minute)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{
			ProjectID:       proj1,
			ProjectName:     "project-one",
			ServiceID:       "svc-1",
			ServiceName:     "service-one",
			EnvironmentID:   "env-1",
			EnvironmentName: "production",
		},
		{
			ProjectID:       proj2,
			ProjectName:     "project-two",
			ServiceID:       "svc-2",
			ServiceName:     "service-two",
			EnvironmentID:   "env-2",
			EnvironmentName: "staging",
		},
	})

	store.EXPECT().GetMetricCursor(proj1).Return(cursor1)
	store.EXPECT().GetMetricCursor(proj2).Return(cursor2)

	sampleRate := 30
	avgWindow := 30

	// Project 1 metrics
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&proj1), gomock.Nil(), gomock.Nil(),
		gomock.Eq(cursor1.Format(time.RFC3339)),
		gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{{
			Measurement: railway.MetricMeasurementMemoryUsageGb,
			Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
				ServiceId:     strPtr("svc-1"),
				EnvironmentId: strPtr("env-1"),
			},
			Values: []railway.MetricsMetricsMetricsResultValuesMetric{
				{Ts: int(now.Add(-2 * time.Minute).Unix()), Value: 1.0},
			},
		}},
	}, nil)

	// Project 2 metrics
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&proj2), gomock.Nil(), gomock.Nil(),
		gomock.Eq(cursor2.Format(time.RFC3339)),
		gomock.Nil(),
		gomock.Any(), gomock.Any(),
		gomock.Eq(&sampleRate), gomock.Eq(&avgWindow),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{{
			Measurement: railway.MetricMeasurementCpuUsage,
			Tags: railway.MetricsMetricsMetricsResultTagsMetricTags{
				ServiceId:     strPtr("svc-2"),
				EnvironmentId: strPtr("env-2"),
			},
			Values: []railway.MetricsMetricsMetricsResultValuesMetric{
				{Ts: int(now.Add(-1 * time.Minute).Unix()), Value: 0.75},
			},
		}},
	}, nil)

	// Both cursors advance
	store.EXPECT().SetMetricCursor(proj1, now.UTC()).Return(nil)
	store.EXPECT().SetMetricCursor(proj2, now.UTC()).Return(nil)

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	mc := collector.NewMetricsCollector(
		api, targets, []sink.Sink{fakeSink}, store,
		[]string{"memory", "cpu"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 2)

	// Verify both projects' data is present
	names := map[string]bool{}
	for _, p := range collected {
		names[p.Name] = true
	}
	assert.True(t, names["railway_memory_usage_gb"])
	assert.True(t, names["railway_cpu_usage_cores"])
}

func TestMetricsCollector_Collect_NoTargets(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{})

	// No API, store, or sink calls expected

	fakeSink := &recordingSink{}

	mc := collector.NewMetricsCollector(
		api, targets, []sink.Sink{fakeSink}, store,
		[]string{"memory"},
		30, 30,
		10*time.Minute,
		5*time.Minute,
		fakeClock,
		slog.Default(),
	)

	err := mc.Collect(context.Background())
	require.NoError(t, err)
}
