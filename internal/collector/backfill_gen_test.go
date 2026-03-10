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
	"github.com/xevion/railway-collector/internal/sink"
	"go.uber.org/mock/gomock"
)

func TestBackfillGenerator_Type(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		Logger:          slog.Default(),
	})

	assert.Equal(t, collector.TaskTypeBackfill, gen.Type())
}

func TestBackfillGenerator_PollReturnsNilWhenNoTargets(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	targets.EXPECT().Targets().Return(nil)

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		Logger:          slog.Default(),
	})

	items := gen.Poll(fakeClock.Now())
	assert.Nil(t, items)
}

func TestBackfillGenerator_PollRespectsInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test",
		ServiceID:       "svc-1",
		ServiceName:     "svc",
		EnvironmentID:   "env-1",
		EnvironmentName: "prod",
		DeploymentID:    "dep-1",
	}}).AnyTimes()

	// No coverage at all -- huge gap
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		Interval:        30 * time.Second,
		Logger:          slog.Default(),
	})

	now := fakeClock.Now()

	// First poll should return items
	items := gen.Poll(now)
	assert.NotNil(t, items)

	// Immediate second poll should return nil (interval not elapsed)
	items = gen.Poll(now)
	assert.Nil(t, items)

	// After interval, should return items again
	items = gen.Poll(now.Add(31 * time.Second))
	assert.NotNil(t, items)
}

func TestBackfillGenerator_PollEmitsMetricWorkItems(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "p1", ServiceID: "svc-1", ServiceName: "s1",
			EnvironmentID: "env-1", EnvironmentName: "prod", DeploymentID: "dep-1"},
	}).AnyTimes()

	// Coverage with a 6-hour gap: covered [now-12h, now-6h], gap is [now-18h, now-12h] and [now-6h, now]
	coverageKey := collector.CoverageKey("proj-1", "metric")
	coverageJSON := mustMarshalCoverage(t, []collector.CoverageInterval{{
		Start:      now.Add(-12 * time.Hour),
		End:        now.Add(-6 * time.Hour),
		Kind:       collector.CoverageCollected,
		Resolution: 30,
	}})
	store.EXPECT().GetCoverage(coverageKey).Return(coverageJSON, nil)

	// Env log coverage -- no gaps
	envCoverageKey := collector.CoverageKey("env-1", "log", "environment")
	fullEnvCoverage := mustMarshalCoverage(t, []collector.CoverageInterval{{
		Start: now.Add(-5 * 24 * time.Hour),
		End:   now,
		Kind:  collector.CoverageCollected,
	}})
	store.EXPECT().GetCoverage(envCoverageKey).Return(fullEnvCoverage, nil)

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 24 * time.Hour, // only look back 1 day for simplicity
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// Should have metric backfill items
	hasMetrics := false
	for _, item := range items {
		if item.Kind == collector.QueryMetrics {
			hasMetrics = true
			assert.Equal(t, collector.TaskTypeBackfill, item.TaskType)
			assert.Equal(t, "proj-1", item.AliasKey)
			// Params should have startDate and endDate (chunk boundaries)
			_, hasStart := item.Params["startDate"]
			_, hasEnd := item.Params["endDate"]
			assert.True(t, hasStart, "should have startDate param")
			assert.True(t, hasEnd, "should have endDate param")
		}
	}
	assert.True(t, hasMetrics, "should have metric backfill items")
}

func TestBackfillGenerator_PollGroupsProjectsByChunk(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	// Two projects, both with no coverage at all
	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "p1", ServiceID: "svc-1", ServiceName: "s1",
			EnvironmentID: "env-1", EnvironmentName: "prod", DeploymentID: "dep-1"},
		{ProjectID: "proj-2", ProjectName: "p2", ServiceID: "svc-2", ServiceName: "s2",
			EnvironmentID: "env-1", EnvironmentName: "prod", DeploymentID: "dep-2"},
	}).AnyTimes()

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 12 * time.Hour,
		LogRetention:    5 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		MaxItemsPerPoll: 10,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// Items for the same chunk should share the same BatchKey
	metricItems := make([]collector.WorkItem, 0)
	for _, item := range items {
		if item.Kind == collector.QueryMetrics {
			metricItems = append(metricItems, item)
		}
	}

	// Both projects should produce items, and items for the same time chunk
	// should have matching BatchKeys (so they can be batched together)
	if len(metricItems) >= 2 {
		// Find items that share a chunk window
		batchKeys := make(map[string][]string)
		for _, item := range metricItems {
			batchKeys[item.BatchKey] = append(batchKeys[item.BatchKey], item.AliasKey)
		}
		// At least one batch key should have both projects
		foundShared := false
		for _, projects := range batchKeys {
			if len(projects) >= 2 {
				foundShared = true
				break
			}
		}
		assert.True(t, foundShared, "projects with gaps in the same chunk should share BatchKey")
	}
}

func TestBackfillGenerator_PollReturnsNilWhenFullCoverage(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "p1", ServiceID: "svc-1", ServiceName: "s1",
			EnvironmentID: "env-1", EnvironmentName: "prod", DeploymentID: "dep-1"},
	}).AnyTimes()

	// Full metric coverage
	metricKey := collector.CoverageKey("proj-1", "metric")
	fullMetricCov := mustMarshalCoverage(t, []collector.CoverageInterval{{
		Start:      now.Add(-90 * 24 * time.Hour),
		End:        now,
		Kind:       collector.CoverageCollected,
		Resolution: 30,
	}})
	store.EXPECT().GetCoverage(metricKey).Return(fullMetricCov, nil)

	// Full log coverage
	envLogKey := collector.CoverageKey("env-1", "log", "environment")
	fullLogCov := mustMarshalCoverage(t, []collector.CoverageInterval{{
		Start: now.Add(-5 * 24 * time.Hour),
		End:   now,
		Kind:  collector.CoverageCollected,
	}})
	store.EXPECT().GetCoverage(envLogKey).Return(fullLogCov, nil)

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		LogRetention:    5 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	assert.Nil(t, items)
}

func TestBackfillGenerator_DeliverMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "p1", ServiceID: "svc-1", ServiceName: "s1",
			EnvironmentID: "env-1", EnvironmentName: "prod", DeploymentID: "dep-1"},
	}).AnyTimes()

	var collectedPoints []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collectedPoints = append(collectedPoints, pts...)
			return nil
		},
	}

	// Expect coverage to be loaded and saved
	coverageKey := collector.CoverageKey("proj-1", "metric")
	store.EXPECT().GetCoverage(coverageKey).Return(nil, nil)
	store.EXPECT().SetCoverage(coverageKey, gomock.Any()).Return(nil)

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Sinks:           []sink.Sink{fakeSink},
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		Logger:          slog.Default(),
	})

	chunkStart := now.Add(-6 * time.Hour)
	chunkEnd := now

	rawData := []map[string]any{{
		"measurement": "CPU_USAGE",
		"tags": map[string]any{
			"projectId":     "proj-1",
			"serviceId":     "svc-1",
			"environmentId": "env-1",
		},
		"values": []map[string]any{
			{"ts": chunkStart.Add(time.Minute).Unix(), "value": 0.42},
			{"ts": chunkStart.Add(2 * time.Minute).Unix(), "value": 0.55},
		},
	}}

	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID:       "backfill:metric:proj-1:chunk1",
		Kind:     collector.QueryMetrics,
		TaskType: collector.TaskTypeBackfill,
		AliasKey: "proj-1",
		BatchKey: "test",
		Params: map[string]any{
			"startDate": chunkStart.Format(time.RFC3339),
			"endDate":   chunkEnd.Format(time.RFC3339),
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collectedPoints, 2)
	assert.Equal(t, "railway_cpu_usage_cores", collectedPoints[0].Name)
	assert.Equal(t, 0.42, collectedPoints[0].Value)
	assert.Equal(t, "true", collectedPoints[0].Labels["backfill"])
}

func TestBackfillGenerator_DeliverEnvLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "p1", ServiceID: "svc-1", ServiceName: "s1",
			EnvironmentID: "env-1", EnvironmentName: "prod", DeploymentID: "dep-1"},
	}).AnyTimes()

	var collectedLogs []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, entries []sink.LogEntry) error {
			collectedLogs = append(collectedLogs, entries...)
			return nil
		},
	}

	// Expect coverage to be loaded and saved
	coverageKey := collector.CoverageKey("env-1", "log", "environment")
	store.EXPECT().GetCoverage(coverageKey).Return(nil, nil)
	store.EXPECT().SetCoverage(coverageKey, gomock.Any()).Return(nil)

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Sinks:           []sink.Sink{fakeSink},
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		LogRetention:    5 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		LogLimit:        5000,
		Logger:          slog.Default(),
	})

	chunkStart := now.Add(-6 * time.Hour)
	chunkEnd := now

	rawLogs := []map[string]any{{
		"timestamp": chunkStart.Add(time.Minute).Format(time.RFC3339Nano),
		"message":   "test log message",
		"severity":  strPtr("info"),
		"tags": map[string]any{
			"serviceId": "svc-1",
		},
		"attributes": []map[string]string{
			{"key": "foo", "value": "bar"},
		},
	}}

	data, err := json.Marshal(rawLogs)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID:       "backfill:envlog:env-1:chunk1",
		Kind:     collector.QueryEnvironmentLogs,
		TaskType: collector.TaskTypeBackfill,
		AliasKey: "env-1",
		BatchKey: "test",
		Params: map[string]any{
			"afterDate":  chunkStart.Format(time.RFC3339Nano),
			"beforeDate": chunkEnd.Format(time.RFC3339Nano),
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collectedLogs, 1)
	assert.Equal(t, "test log message", collectedLogs[0].Message)
	assert.Equal(t, "true", collectedLogs[0].Labels["backfill"])
}

func TestBackfillGenerator_DeliverError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	targets.EXPECT().Targets().Return(nil).AnyTimes()

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		Logger:          slog.Default(),
	})

	item := collector.WorkItem{
		ID:       "backfill:metric:proj-1:chunk1",
		Kind:     collector.QueryMetrics,
		TaskType: collector.TaskTypeBackfill,
		AliasKey: "proj-1",
	}

	// Should not panic on error delivery
	gen.Deliver(context.Background(), item, nil, assert.AnError)
}

func TestBackfillGenerator_PollEmitsEnvLogItems(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	fakeClock := clockwork.NewFakeClockAt(now)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "p1", ServiceID: "svc-1", ServiceName: "s1",
			EnvironmentID: "env-1", EnvironmentName: "prod", DeploymentID: "dep-1"},
	}).AnyTimes()

	// Full metric coverage so only log gaps remain
	metricKey := collector.CoverageKey("proj-1", "metric")
	fullMetricCov := mustMarshalCoverage(t, []collector.CoverageInterval{{
		Start:      now.Add(-90 * 24 * time.Hour),
		End:        now,
		Kind:       collector.CoverageCollected,
		Resolution: 30,
	}})
	store.EXPECT().GetCoverage(metricKey).Return(fullMetricCov, nil)

	// No env log coverage -- gap exists
	envLogKey := collector.CoverageKey("env-1", "log", "environment")
	store.EXPECT().GetCoverage(envLogKey).Return(nil, nil)

	gen := collector.NewBackfillGenerator(collector.BackfillGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		MetricRetention: 90 * 24 * time.Hour,
		LogRetention:    5 * 24 * time.Hour,
		ChunkSize:       6 * time.Hour,
		SampleRate:      30,
		AvgWindow:       30,
		LogLimit:        5000,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	hasEnvLogs := false
	for _, item := range items {
		if item.Kind == collector.QueryEnvironmentLogs {
			hasEnvLogs = true
			assert.Equal(t, collector.TaskTypeBackfill, item.TaskType)
			assert.Equal(t, "env-1", item.AliasKey)
			assert.Equal(t, "true", item.Params["backfill"])
		}
	}
	assert.True(t, hasEnvLogs, "should have env log backfill items")
}

func TestBackfillGenerator_ChunkAlignment(t *testing.T) {
	// Verify that chunks are aligned to fixed boundaries (multiples of ChunkSize from epoch)
	now := time.Date(2026, 3, 9, 14, 30, 0, 0, time.UTC) // 14:30 -- not on a 6h boundary

	aligned := collector.AlignToChunkBoundary(now, 6*time.Hour)
	// Should snap to 12:00 (the previous 6h boundary)
	expected := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, aligned)

	// On a boundary should stay there
	onBoundary := time.Date(2026, 3, 9, 18, 0, 0, 0, time.UTC)
	aligned2 := collector.AlignToChunkBoundary(onBoundary, 6*time.Hour)
	assert.Equal(t, onBoundary, aligned2)
}
