package collector_test

import (
	"context"
	"encoding/json"
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

func TestMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, collector.TaskTypeMetrics, gen.Type())
}

func TestMetricsGenerator_Poll_EmitsItemsPerProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	cursor1 := now.Add(-5 * time.Minute)
	cursor2 := now.Add(-3 * time.Minute)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", EnvironmentID: "env-1"},
		{ProjectID: "proj-2", ProjectName: "two", ServiceID: "svc-2", EnvironmentID: "env-2"},
	})

	store.EXPECT().GetMetricCursor("proj-1").Return(cursor1)
	store.EXPECT().GetMetricCursor("proj-2").Return(cursor2)

	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Discovery:    targets,
		Store:        store,
		Clock:        fakeClock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:   30,
		AvgWindow:    30,
		Lookback:     10 * time.Minute,
		Interval:     30 * time.Second,
		Logger:       slog.Default(),
	})

	items := gen.Poll(now)
	require.Len(t, items, 2)

	assert.Equal(t, "metrics:proj-1", items[0].ID)
	assert.Equal(t, collector.QueryMetrics, items[0].Kind)
	assert.Equal(t, collector.TaskTypeMetrics, items[0].TaskType)
	assert.Equal(t, "proj-1", items[0].AliasKey)
	assert.Equal(t, cursor1.Format(time.RFC3339), items[0].Params["startDate"])

	assert.Equal(t, "metrics:proj-2", items[1].ID)
	assert.Equal(t, "proj-2", items[1].AliasKey)
	assert.Equal(t, cursor2.Format(time.RFC3339), items[1].Params["startDate"])

	// Both items should share the same batch key
	assert.Equal(t, items[0].BatchKey, items[1].BatchKey)
}

func TestMetricsGenerator_Poll_RespectsInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	}).AnyTimes()
	store.EXPECT().GetMetricCursor("proj-1").Return(now.Add(-5 * time.Minute)).AnyTimes()

	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Discovery:    targets,
		Store:        store,
		Clock:        fakeClock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:   30,
		AvgWindow:    30,
		Lookback:     10 * time.Minute,
		Interval:     30 * time.Second,
		Logger:       slog.Default(),
	})

	// First poll should return items
	items := gen.Poll(now)
	require.Len(t, items, 1)

	// Immediate second poll should return nil (interval not elapsed)
	items = gen.Poll(now.Add(1 * time.Second))
	assert.Nil(t, items)

	// After interval, should return items again
	items = gen.Poll(now.Add(30 * time.Second))
	require.Len(t, items, 1)
}

func TestMetricsGenerator_Poll_NoCursor_FallsBackToLookback(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	lookback := 10 * time.Minute

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	})

	store.EXPECT().GetMetricCursor("proj-1").Return(time.Time{})

	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Discovery:    targets,
		Store:        store,
		Clock:        fakeClock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:   30,
		AvgWindow:    30,
		Lookback:     lookback,
		Interval:     30 * time.Second,
		Logger:       slog.Default(),
	})

	items := gen.Poll(now)
	require.Len(t, items, 1)

	expectedStart := now.Add(-lookback).Format(time.RFC3339)
	assert.Equal(t, expectedStart, items[0].Params["startDate"])
}

func TestMetricsGenerator_Poll_NoTargets_ReturnsNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{})

	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Discovery: targets,
		Clock:     fakeClock,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	items := gen.Poll(fakeClock.Now())
	assert.Nil(t, items)
}

func TestMetricsGenerator_Deliver_ProcessesResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	projectID := "proj-1"

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       projectID,
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}})

	// Expect cursor update
	store.EXPECT().SetMetricCursor(projectID, now.UTC()).Return(nil)
	// Expect coverage recording
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Discovery:    targets,
		Store:        store,
		Sinks:        []sink.Sink{fakeSink},
		Clock:        fakeClock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementMemoryUsageGb},
		SampleRate:   30,
		AvgWindow:    30,
		Lookback:     10 * time.Minute,
		Interval:     30 * time.Second,
		Logger:       slog.Default(),
	})

	// Simulate raw JSON response for one project's metrics
	rawData := []map[string]any{
		{
			"measurement": "MEMORY_USAGE_GB",
			"tags": map[string]any{
				"serviceId":     "svc-1",
				"environmentId": "env-1",
			},
			"values": []map[string]any{
				{"ts": now.Add(-2 * time.Minute).Unix(), "value": 0.5},
				{"ts": now.Add(-1 * time.Minute).Unix(), "value": 0.6},
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID:       "metrics:" + projectID,
		Kind:     collector.QueryMetrics,
		TaskType: collector.TaskTypeMetrics,
		AliasKey: projectID,
		Params: map[string]any{
			"startDate": now.Add(-5 * time.Minute).Format(time.RFC3339),
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 2)
	assert.Equal(t, "railway_memory_usage_gb", collected[0].Name)
	assert.Equal(t, 0.5, collected[0].Value)
	assert.Equal(t, "railway_memory_usage_gb", collected[1].Name)
	assert.Equal(t, 0.6, collected[1].Value)

	// Label assertions
	assert.Equal(t, "svc-1", collected[0].Labels["service_id"])
	assert.Equal(t, "env-1", collected[0].Labels["environment_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
	assert.NotContains(t, collected[0].Labels, "backfill")
}

func TestMetricsGenerator_Deliver_HandlesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "test"},
	})

	fakeSink := &recordingSink{}

	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Discovery: targets,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     fakeClock,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	item := collector.WorkItem{
		ID:       "metrics:proj-1",
		AliasKey: "proj-1",
	}

	// Deliver with error should not panic or write to sinks
	gen.Deliver(context.Background(), item, nil, assert.AnError)
}

func TestMetricsGenerator_Deliver_EmptyResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	projectID := "proj-1"

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: projectID, ProjectName: "test"},
	})

	store.EXPECT().SetMetricCursor(projectID, now.UTC()).Return(nil)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	sinkCalled := false
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			sinkCalled = true
			return nil
		},
	}

	gen := collector.NewMetricsGenerator(collector.MetricsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     fakeClock,
		Lookback:  10 * time.Minute,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	data, _ := json.Marshal([]map[string]any{})
	item := collector.WorkItem{
		ID:       "metrics:" + projectID,
		AliasKey: projectID,
		Params: map[string]any{
			"startDate": now.Add(-5 * time.Minute).Format(time.RFC3339),
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	// Cursor and coverage should still be updated even with empty results
	// Sink should be called (with empty slice)
	assert.True(t, sinkCalled)
}
