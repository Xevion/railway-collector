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

func mustMarshalCoverage(t *testing.T, intervals []collector.CoverageInterval) []byte {
	t.Helper()
	data, err := json.Marshal(intervals)
	require.NoError(t, err)
	return data
}

func TestBackfillManager_FindsAndFillsMetricGap(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
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
		DeploymentID:    "dep-1",
	}})

	// Coverage shows a gap: we have [now-2h, now-1h] but not [now-3h, now-2h]
	coveredStart := now.Add(-2 * time.Hour)
	coveredEnd := now.Add(-1 * time.Hour)
	coverageKey := collector.CoverageKey(projectID, "metric")

	coverageJSON := mustMarshalCoverage(t, []collector.CoverageInterval{{
		Start:      coveredStart,
		End:        coveredEnd,
		Kind:       collector.CoverageCollected,
		Resolution: 30,
	}})

	store.EXPECT().GetCoverage(coverageKey).Return(coverageJSON, nil)

	// BackfillManager should request metrics for gap chunks
	sampleRate := 30
	avgWindow := 30
	api.EXPECT().GetMetrics(
		gomock.Any(),
		gomock.Eq(&projectID), gomock.Nil(), gomock.Nil(),
		gomock.Any(), // start of gap chunk
		gomock.Any(), // end of gap chunk
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
				{Ts: int(now.Add(-150 * time.Minute).Unix()), Value: 0.4},
			},
		}},
	}, nil).AnyTimes()

	// Coverage should be updated after backfill
	store.EXPECT().SetCoverage(coverageKey, gomock.Any()).Return(nil).AnyTimes()

	// Env log coverage (will be checked but no gaps or all chunks used)
	envCoverageKey := collector.CoverageKey("env-1", "log", "environment")
	store.EXPECT().GetCoverage(envCoverageKey).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(envCoverageKey, gomock.Any()).Return(nil).AnyTimes()

	// Env log backfill API call if it gets to it
	api.EXPECT().GetEnvironmentLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&railway.EnvironmentLogsQueryResponse{}, nil).AnyTimes()

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = append(collected, pts...)
			return nil
		},
	}

	bm := collector.NewBackfillManager(collector.BackfillConfig{
		API:              api,
		Store:            store,
		Discovery:        targets,
		Sinks:            []sink.Sink{fakeSink},
		Clock:            fakeClock,
		MetricSampleRate: 30,
		MetricAvgWindow:  30,
		MetricRetention:  90 * 24 * time.Hour,
		LogRetention:     5 * 24 * time.Hour,
		MaxChunksPerRun:  2,
		MetricChunkSize:  10 * 24 * time.Hour,
		Logger:           slog.Default(),
	})

	err := bm.RunOnce(context.Background())
	require.NoError(t, err)
	assert.Greater(t, len(collected), 0, "backfill should have produced metric points")
}

func TestBackfillManager_NoCoverage_BackfillsFromRetentionStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
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
		DeploymentID:    "dep-1",
	}})

	coverageKey := collector.CoverageKey(projectID, "metric")
	// No existing coverage
	store.EXPECT().GetCoverage(coverageKey).Return(nil, nil)

	// Should request the oldest gap chunks (near retention boundary, prioritized first)
	api.EXPECT().GetMetrics(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(),
	).Return(&railway.MetricsResponse{
		Metrics: []railway.MetricsMetricsMetricsResult{},
	}, nil).Times(3) // MaxChunksPerRun = 3

	// Coverage saved 3 times (once per chunk)
	store.EXPECT().SetCoverage(coverageKey, gomock.Any()).Return(nil).Times(3)

	// Env log coverage - won't be reached because all 3 chunks used on metrics
	// But the code may still try to load it. Use AnyTimes to be safe.
	envCoverageKey := collector.CoverageKey("env-1", "log", "environment")
	store.EXPECT().GetCoverage(envCoverageKey).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(envCoverageKey, gomock.Any()).Return(nil).AnyTimes()
	api.EXPECT().GetEnvironmentLogs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(&railway.EnvironmentLogsQueryResponse{}, nil).AnyTimes()

	fakeSink := &recordingSink{}

	bm := collector.NewBackfillManager(collector.BackfillConfig{
		API:              api,
		Store:            store,
		Discovery:        targets,
		Sinks:            []sink.Sink{fakeSink},
		Clock:            fakeClock,
		MetricSampleRate: 30,
		MetricAvgWindow:  30,
		MetricRetention:  90 * 24 * time.Hour,
		LogRetention:     5 * 24 * time.Hour,
		MaxChunksPerRun:  3,
		MetricChunkSize:  10 * 24 * time.Hour,
		Logger:           slog.Default(),
	})

	err := bm.RunOnce(context.Background())
	require.NoError(t, err)

	// After one RunOnce with 3 chunks of 10 days each, 30 days should be covered.
	// This is verified indirectly by the mock expectations: exactly 3 GetMetrics calls
	// and 3 SetCoverage calls.
	_ = now
}

func TestChunkTimeRange(t *testing.T) {
	start := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

	t.Run("single chunk when range smaller than chunk size", func(t *testing.T) {
		tr := collector.TimeRange{Start: start, End: start.Add(5 * time.Hour)}
		chunks := collector.ChunkTimeRange(tr, 10*24*time.Hour)
		assert.Len(t, chunks, 1)
		assert.Equal(t, start, chunks[0].Start)
		assert.Equal(t, start.Add(5*time.Hour), chunks[0].End)
	})

	t.Run("multiple chunks for large range", func(t *testing.T) {
		tr := collector.TimeRange{Start: start, End: start.Add(25 * 24 * time.Hour)}
		chunks := collector.ChunkTimeRange(tr, 10*24*time.Hour)
		assert.Len(t, chunks, 3) // 10d + 10d + 5d
		assert.Equal(t, start, chunks[0].Start)
		assert.Equal(t, start.Add(10*24*time.Hour), chunks[0].End)
		assert.Equal(t, start.Add(25*24*time.Hour), chunks[2].End)
	})

	t.Run("exact multiple", func(t *testing.T) {
		tr := collector.TimeRange{Start: start, End: start.Add(20 * 24 * time.Hour)}
		chunks := collector.ChunkTimeRange(tr, 10*24*time.Hour)
		assert.Len(t, chunks, 2)
	})
}
