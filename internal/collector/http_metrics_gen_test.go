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

func TestHttpMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, collector.TaskTypeMetrics, gen.Type())
}

func TestHttpMetricsGenerator_Poll_EmitsTwoItemsPerTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"},
	})

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		ChunkSize:       6 * time.Hour,
		MaxItemsPerPoll: 10,
		StepSeconds:     60,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// Should have both QueryHttpDurationMetrics and QueryHttpMetricsGroupedByStatus
	kindCounts := make(map[collector.QueryKind]int)
	for _, item := range items {
		kindCounts[item.Kind]++
		assert.Equal(t, collector.TaskTypeMetrics, item.TaskType)
	}
	assert.Greater(t, kindCounts[collector.QueryHttpDurationMetrics], 0, "should have duration items")
	assert.Greater(t, kindCounts[collector.QueryHttpMetricsGroupedByStatus], 0, "should have status items")
}

func TestHttpMetricsGenerator_Poll_LiveEdge_IncludesEndDate(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	})

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Clock:           fakeClock,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		ChunkSize:       6 * time.Hour,
		MaxItemsPerPoll: 10,
		StepSeconds:     60,
		Logger:          slog.Default(),
	})

	items := gen.Poll(now)
	require.NotEmpty(t, items)

	// All items should include endDate (HTTP metrics endpoints require it)
	for _, item := range items {
		endDate, hasEndDate := item.Params["endDate"]
		assert.True(t, hasEndDate, "live-edge HTTP metric items should include endDate")
		assert.NotEmpty(t, endDate, "endDate should not be empty")
	}
}

func TestHttpMetricsGenerator_Deliver_Duration(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}}).AnyTimes()

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Sinks:           []sink.Sink{fakeSink},
		Clock:           fakeClock,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		StepSeconds:     60,
		Logger:          slog.Default(),
	})

	// HTTP duration response shape: {"samples": [{ts, p50, p90, p95, p99}]}
	rawData := map[string]any{
		"samples": []map[string]any{
			{
				"ts":  now.Add(-2 * time.Minute).Unix(),
				"p50": 0.1,
				"p90": 0.2,
				"p95": 0.3,
				"p99": 0.5,
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID:       "http-duration:svc-1:env-1",
		Kind:     collector.QueryHttpDurationMetrics,
		TaskType: collector.TaskTypeMetrics,
		AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"startDate": now.Add(-5 * time.Minute).Format(time.RFC3339),
			"endDate":   now.Format(time.RFC3339),
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	// 1 sample produces 4 metric points (p50, p90, p95, p99)
	require.Len(t, collected, 4)

	names := make(map[string]bool)
	for _, pt := range collected {
		names[pt.Name] = true
	}
	assert.True(t, names["railway_http_duration_p50"])
	assert.True(t, names["railway_http_duration_p90"])
	assert.True(t, names["railway_http_duration_p95"])
	assert.True(t, names["railway_http_duration_p99"])

	// Verify values
	for _, pt := range collected {
		switch pt.Name {
		case "railway_http_duration_p50":
			assert.Equal(t, 0.1, pt.Value)
		case "railway_http_duration_p90":
			assert.Equal(t, 0.2, pt.Value)
		case "railway_http_duration_p95":
			assert.Equal(t, 0.3, pt.Value)
		case "railway_http_duration_p99":
			assert.Equal(t, 0.5, pt.Value)
		}
	}
}

func TestHttpMetricsGenerator_Deliver_Status(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}}).AnyTimes()

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Discovery:   targets,
		Sinks:       []sink.Sink{fakeSink},
		Clock:       fakeClock,
		Interval:    30 * time.Second,
		StepSeconds: 60,
		Logger:      slog.Default(),
	})

	// HTTP status response shape: [{"statusCode": 200, "samples": [{ts, value}]}]
	rawData := []map[string]any{
		{
			"statusCode": 200,
			"samples": []map[string]any{
				{"ts": now.Add(-2 * time.Minute).Unix(), "value": 42.0},
			},
		},
		{
			"statusCode": 500,
			"samples": []map[string]any{
				{"ts": now.Add(-2 * time.Minute).Unix(), "value": 3.0},
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID:       "http-status:svc-1:env-1",
		Kind:     collector.QueryHttpMetricsGroupedByStatus,
		TaskType: collector.TaskTypeMetrics,
		AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"startDate": now.Add(-5 * time.Minute).Format(time.RFC3339),
			"endDate":   now.Format(time.RFC3339),
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 2)

	// All points should be named railway_http_requests with status_code label
	for _, pt := range collected {
		assert.Equal(t, "railway_http_requests", pt.Name)
		assert.Contains(t, pt.Labels, "status_code")
	}

	// Verify status code values
	statusValues := make(map[string]float64)
	for _, pt := range collected {
		statusValues[pt.Labels["status_code"]] = pt.Value
	}
	assert.Equal(t, 42.0, statusValues["200"])
	assert.Equal(t, 3.0, statusValues["500"])
}
