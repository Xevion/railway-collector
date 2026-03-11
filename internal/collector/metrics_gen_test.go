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

func TestProjectMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, collector.TaskTypeMetrics, gen.Type())
}

func TestProjectMetricsGenerator_Poll_EmitsItemsPerProject(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", EnvironmentID: "env-1"},
		{ProjectID: "proj-2", ProjectName: "two", ServiceID: "svc-2", EnvironmentID: "env-2"},
	})

	// No coverage -- gaps exist for both projects
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:      30,
		AvgWindow:       30,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		ChunkSize:       6 * time.Hour,
		MaxItemsPerPoll: 10,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// Should have items for both projects
	projectIDs := make(map[string]bool)
	for _, item := range items {
		assert.Equal(t, collector.QueryMetrics, item.Kind)
		assert.Equal(t, collector.TaskTypeMetrics, item.TaskType)
		projectIDs[item.AliasKey] = true
	}
	assert.True(t, projectIDs["proj-1"], "should have items for proj-1")
	assert.True(t, projectIDs["proj-2"], "should have items for proj-2")
}

func TestProjectMetricsGenerator_Poll_RespectsInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	}).AnyTimes()
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:      30,
		AvgWindow:       30,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		MaxItemsPerPoll: 10,
		Logger:          slog.Default(),
	})

	// First poll should return items
	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// Immediate second poll should return nil (interval not elapsed)
	items = gen.Poll(now.Add(1 * time.Second))
	assert.Nil(t, items)

	// After interval, should return items again
	items = gen.Poll(now.Add(30 * time.Second))
	require.NotEmpty(t, items)
}

func TestProjectMetricsGenerator_Poll_NoCoverage_ScansFullRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	retention := 1 * time.Hour

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	})

	// No coverage at all
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:      30,
		AvgWindow:       30,
		Interval:        30 * time.Second,
		MetricRetention: retention,
		MaxItemsPerPoll: 10,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// The live edge gap should be prioritized first (most recent)
	// All items should be for proj-1
	for _, item := range items {
		assert.Equal(t, "proj-1", item.AliasKey)
	}
}

func TestProjectMetricsGenerator_Poll_NoTargets_ReturnsNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{})

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Discovery: targets,
		Clock:     fakeClock,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	items := gen.Poll(fakeClock.Now())
	assert.Nil(t, items)
}

func TestProjectMetricsGenerator_Deliver_ProcessesResults(t *testing.T) {
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

	// Expect coverage recording (no cursor update)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Sinks:           []sink.Sink{fakeSink},
		Clock:           fakeClock,
		Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementMemoryUsageGb},
		SampleRate:      30,
		AvgWindow:       30,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		Logger:          slog.Default(),
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

func TestProjectMetricsGenerator_Deliver_HandlesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "test"},
	})

	fakeSink := &recordingSink{}

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
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

func TestProjectMetricsGenerator_Deliver_EmptyResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	projectID := "proj-1"

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: projectID, ProjectName: "test"},
	})

	// Coverage should still be updated even with empty results
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Clock:     fakeClock,
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
	// No panic, coverage updated (gomock verifies SetCoverage was called)
}

func TestProjectMetricsGenerator_Poll_LargeLiveEdgeGap_ChunksOlderPortion(t *testing.T) {
	// BUG: When a coverage gap extends from far in the past to "now", the
	// isLiveEdge check (now.Sub(gap.End) < time.Minute) is true because
	// gap.End IS near now. This causes the entire gap to be emitted as a
	// single open-ended work item, bypassing chunking. A 90-day gap at 30s
	// granularity would request ~260k points — well beyond the API limit.
	//
	// Expected: the older portion of the gap should be chunked, with only
	// the most recent portion using an open-ended live-edge query.

	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	chunkSize := 6 * time.Hour
	// Retention of 7 days means the gap spans 7 days — far larger than one chunk.
	retention := 7 * 24 * time.Hour

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", EnvironmentID: "env-1"},
	})

	// No coverage at all — produces one gap from (now - retention) to now
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:      30,
		AvgWindow:       30,
		Interval:        30 * time.Second,
		MetricRetention: retention,
		ChunkSize:       chunkSize,
		MaxItemsPerPoll: 50, // high limit so we can see all items
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// The gap spans 7 days. With 6h chunks that's 28 chunks. Regardless of
	// exact count, emitting a SINGLE item for the whole gap is the bug.
	assert.Greater(t, len(items), 1,
		"a 7-day live-edge gap must be split into multiple work items, not emitted as one")

	// Count how many items have an endDate (chunked) vs open-ended (live edge).
	var chunked, openEnded int
	for _, item := range items {
		if _, hasEnd := item.Params["endDate"]; hasEnd {
			chunked++
		} else {
			openEnded++
		}
	}

	// At most one item should be open-ended (the live-edge tail).
	assert.LessOrEqual(t, openEnded, 1,
		"at most one work item should be open-ended (live edge)")

	// The majority should be chunked with explicit endDate.
	assert.Greater(t, chunked, 0,
		"older portions of the gap must be chunked with explicit endDate")
}
