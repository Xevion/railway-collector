package collector_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/sink"
	"go.uber.org/mock/gomock"
)

func TestHttpMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, types.TaskTypeMetrics, gen.Type())
}

func TestHttpMetricsGenerator_Poll_EmitsTwoItemsPerTarget(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"},
	})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Discovery:       env.Targets,
		Store:           env.Store,
		Clock:           env.Clock,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		ChunkSize:       6 * time.Hour,
		MaxItemsPerPoll: 10,
		StepSeconds:     60,
		Logger:          slog.Default(),
	})

	items := gen.Poll(env.Now)
	require.NotEmpty(t, items)

	// Should have both QueryHttpDurationMetrics and QueryHttpMetricsGroupedByStatus
	kindCounts := make(map[types.QueryKind]int)
	for _, item := range items {
		kindCounts[item.Kind]++
		assert.Equal(t, types.TaskTypeMetrics, item.TaskType)
	}
	assert.Greater(t, kindCounts[types.QueryHttpDurationMetrics], 0, "should have duration items")
	assert.Greater(t, kindCounts[types.QueryHttpMetricsGroupedByStatus], 0, "should have status items")
}

func TestHttpMetricsGenerator_Poll_LiveEdge_IncludesEndDate(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Discovery:       env.Targets,
		Store:           env.Store,
		Clock:           env.Clock,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		ChunkSize:       6 * time.Hour,
		MaxItemsPerPoll: 10,
		StepSeconds:     60,
		Logger:          slog.Default(),
	})

	items := gen.Poll(env.Now)
	require.NotEmpty(t, items)

	// All items should include endDate (HTTP metrics endpoints require it)
	for _, item := range items {
		endDate, hasEndDate := item.Params["endDate"]
		assert.True(t, hasEndDate, "live-edge HTTP metric items should include endDate")
		assert.NotEmpty(t, endDate, "endDate should not be empty")
	}
}

func TestHttpMetricsGenerator_Poll_GapChunking(t *testing.T) {
	// HTTP metrics don't use runGapChunkTests because their chunking behavior
	// differs: all items always include endDate (even live-edge), each chunk
	// position emits 2 items (one per QueryKind), and there's no open-ended
	// vs chunked distinction.
	tests := []struct {
		name            string
		retention       time.Duration
		chunkSize       time.Duration
		maxItems        int
		wantMinItems    int // minimum total items (each gap position emits 2: duration + status)
		wantTotalCapped bool
	}{
		{
			name:         "7d live-edge gap with 6h chunks",
			retention:    7 * 24 * time.Hour,
			chunkSize:    6 * time.Hour,
			maxItems:     400,
			wantMinItems: 4, // at least 2 chunk positions * 2 kinds
		},
		{
			name:         "90d live-edge gap with 6h chunks",
			retention:    90 * 24 * time.Hour,
			chunkSize:    6 * time.Hour,
			maxItems:     1000,
			wantMinItems: 20, // many chunk positions * 2 kinds
		},
		{
			name:         "gap smaller than chunk size stays single pair",
			retention:    1 * time.Hour,
			chunkSize:    6 * time.Hour,
			maxItems:     50,
			wantMinItems: 2, // 1 position * 2 kinds
		},
		{
			name:            "maxItems caps output",
			retention:       7 * 24 * time.Hour,
			chunkSize:       6 * time.Hour,
			maxItems:        6,
			wantMinItems:    6,
			wantTotalCapped: true,
		},
		{
			name:         "1h chunks on 2d gap produces many items",
			retention:    2 * 24 * time.Hour,
			chunkSize:    1 * time.Hour,
			maxItems:     400,
			wantMinItems: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := setupGenTest(t)

			env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
				{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"},
			})
			env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

			gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
				Discovery:       env.Targets,
				Store:           env.Store,
				Clock:           env.Clock,
				Interval:        30 * time.Second,
				MetricRetention: tt.retention,
				ChunkSize:       tt.chunkSize,
				MaxItemsPerPoll: tt.maxItems,
				StepSeconds:     60,
				Logger:          slog.Default(),
			})

			items := gen.Poll(env.Now)
			require.GreaterOrEqual(t, len(items), tt.wantMinItems, "minimum item count")

			// All HTTP metric items must have endDate
			for _, item := range items {
				_, hasEnd := item.Params["endDate"]
				assert.True(t, hasEnd, "all HTTP metric items must have endDate, got item %s", item.ID)
			}

			// Verify we get both kinds
			kindCounts := make(map[types.QueryKind]int)
			for _, item := range items {
				kindCounts[item.Kind]++
			}
			assert.Greater(t, kindCounts[types.QueryHttpDurationMetrics], 0, "should have duration items")
			assert.Greater(t, kindCounts[types.QueryHttpMetricsGroupedByStatus], 0, "should have status items")

			if tt.wantTotalCapped {
				assert.LessOrEqual(t, len(items), tt.maxItems, "output should be capped at maxItems")
			}
		})
	}
}

func TestHttpMetricsGenerator_Deliver_Duration(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}}).AnyTimes()

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
		Discovery:       env.Targets,
		Store:           env.Store,
		Sinks:           []sink.Sink{fakeSink},
		Clock:           env.Clock,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		StepSeconds:     60,
		Logger:          slog.Default(),
	})

	// HTTP duration response shape: {"samples": [{ts, p50, p90, p95, p99}]}
	rawData := map[string]any{
		"samples": []map[string]any{
			{
				"ts":  env.Now.Add(-2 * time.Minute).Unix(),
				"p50": 0.1,
				"p90": 0.2,
				"p95": 0.3,
				"p99": 0.5,
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "http-duration:svc-1:env-1",
		Kind:     types.QueryHttpDurationMetrics,
		TaskType: types.TaskTypeMetrics,
		AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"startDate": env.Now.Add(-5 * time.Minute).Format(time.RFC3339),
			"endDate":   env.Now.Format(time.RFC3339),
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
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
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
		Discovery:   env.Targets,
		Sinks:       []sink.Sink{fakeSink},
		Clock:       env.Clock,
		Interval:    30 * time.Second,
		StepSeconds: 60,
		Logger:      slog.Default(),
	})

	// HTTP status response shape: [{"statusCode": 200, "samples": [{ts, value}]}]
	rawData := []map[string]any{
		{
			"statusCode": 200,
			"samples": []map[string]any{
				{"ts": env.Now.Add(-2 * time.Minute).Unix(), "value": 42.0},
			},
		},
		{
			"statusCode": 500,
			"samples": []map[string]any{
				{"ts": env.Now.Add(-2 * time.Minute).Unix(), "value": 3.0},
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "http-status:svc-1:env-1",
		Kind:     types.QueryHttpMetricsGroupedByStatus,
		TaskType: types.TaskTypeMetrics,
		AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"startDate": env.Now.Add(-5 * time.Minute).Format(time.RFC3339),
			"endDate":   env.Now.Format(time.RFC3339),
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

func TestHttpMetricsGenerator_Deliver_HandlesError(t *testing.T) {
	testDeliverHandlesError(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Interval: 30 * time.Second, Logger: slog.Default(),
			})
		},
		types.WorkItem{ID: "http-duration:svc-1:env-1", Kind: types.QueryHttpDurationMetrics, AliasKey: "svc-1:env-1"},
	)
}

func TestHttpMetricsGenerator_Deliver_EmptyDurationResults(t *testing.T) {
	emptyJSON, _ := json.Marshal(map[string]any{"samples": []map[string]any{}})
	testDeliverEmptyResults(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"}},
		func(env *genTestEnv) types.TaskGenerator {
			return collector.NewHttpMetricsGenerator(collector.HttpMetricsGeneratorConfig{
				Discovery: env.Targets, Store: env.Store,
				Sinks: []sink.Sink{&recordingSink{}},
				Clock: env.Clock, Interval: 30 * time.Second,
				StepSeconds: 60, Logger: slog.Default(),
			})
		},
		types.WorkItem{
			ID: "http-duration:svc-1:env-1", Kind: types.QueryHttpDurationMetrics, AliasKey: "svc-1:env-1",
			Params: map[string]any{
				"startDate": time.Date(2026, 3, 9, 11, 55, 0, 0, time.UTC).Format(time.RFC3339),
				"endDate":   time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC).Format(time.RFC3339),
			},
		},
		emptyJSON,
	)
}
