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
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
	"go.uber.org/mock/gomock"
)

func TestProjectMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		BaseMetricsConfig: collector.BaseMetricsConfig{
			Logger: slog.Default(),
		},
	})
	assert.Equal(t, types.TaskTypeMetrics, gen.Type())
}

func TestProjectMetricsGenerator_Poll_EmitsItemsPerProject(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", EnvironmentID: "env-1"},
		{ProjectID: "proj-2", ProjectName: "two", ServiceID: "svc-2", EnvironmentID: "env-2"},
	})

	// No coverage -- gaps exist for both projects
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		BaseMetricsConfig: collector.BaseMetricsConfig{
			Discovery:       env.Targets,
			Store:           env.Store,
			Clock:           env.Clock,
			Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			SampleRate:      30,
			AvgWindow:       30,
			Interval:        30 * time.Second,
			MetricRetention: 1 * time.Hour,
			ChunkSize:       6 * time.Hour,
			MaxItemsPerPoll: 10,
			Logger:          slog.Default(),
		},
	})

	items := gen.Poll(env.Now)
	require.NotEmpty(t, items)

	// Should have items for both projects
	projectIDs := make(map[string]bool)
	for _, item := range items {
		assert.Equal(t, types.QueryMetrics, item.Kind)
		assert.Equal(t, types.TaskTypeMetrics, item.TaskType)
		projectIDs[item.AliasKey] = true
	}
	assert.True(t, projectIDs["proj-1"], "should have items for proj-1")
	assert.True(t, projectIDs["proj-2"], "should have items for proj-2")
}

func TestProjectMetricsGenerator_Poll_RespectsInterval(t *testing.T) {
	testRespectsInterval(t, 30*time.Second, func(env *genTestEnv) types.TaskGenerator {
		return collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
			BaseMetricsConfig: collector.BaseMetricsConfig{
				Discovery:       env.Targets,
				Store:           env.Store,
				Clock:           env.Clock,
				Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
				SampleRate:      30,
				AvgWindow:       30,
				Interval:        30 * time.Second,
				MetricRetention: 1 * time.Hour,
				MaxItemsPerPoll: 10,
				Logger:          slog.Default(),
			},
		})
	})
}

func TestProjectMetricsGenerator_Poll_NoCoverage_ScansFullRetention(t *testing.T) {
	env := setupGenTest(t)
	retention := 1 * time.Hour

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	})

	// No coverage at all
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		BaseMetricsConfig: collector.BaseMetricsConfig{
			Discovery:       env.Targets,
			Store:           env.Store,
			Clock:           env.Clock,
			Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			SampleRate:      30,
			AvgWindow:       30,
			Interval:        30 * time.Second,
			MetricRetention: retention,
			MaxItemsPerPoll: 10,
			Logger:          slog.Default(),
		},
	})

	items := gen.Poll(env.Now)
	require.NotEmpty(t, items)

	// The live edge gap should be prioritized first (most recent)
	// All items should be for proj-1
	for _, item := range items {
		assert.Equal(t, "proj-1", item.AliasKey)
	}
}

func TestProjectMetricsGenerator_Poll_NoTargets_ReturnsNil(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{})

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		BaseMetricsConfig: collector.BaseMetricsConfig{
			Discovery: env.Targets,
			Clock:     env.Clock,
			Interval:  30 * time.Second,
			Logger:    slog.Default(),
		},
	})

	items := gen.Poll(env.Now)
	assert.Nil(t, items)
}

func TestProjectMetricsGenerator_Deliver_ProcessesResults(t *testing.T) {
	env := setupGenTest(t)
	projectID := "proj-1"

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:       projectID,
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
	}})

	// Expect coverage recording (no cursor update)
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
		BaseMetricsConfig: collector.BaseMetricsConfig{
			Discovery:       env.Targets,
			Store:           env.Store,
			Sinks:           []sink.Sink{fakeSink},
			Clock:           env.Clock,
			Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementMemoryUsageGb},
			SampleRate:      30,
			AvgWindow:       30,
			Interval:        30 * time.Second,
			MetricRetention: 1 * time.Hour,
			Logger:          slog.Default(),
		},
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
				{"ts": env.Now.Add(-2 * time.Minute).Unix(), "value": 0.5},
				{"ts": env.Now.Add(-1 * time.Minute).Unix(), "value": 0.6},
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "metrics:" + projectID,
		Kind:     types.QueryMetrics,
		TaskType: types.TaskTypeMetrics,
		AliasKey: projectID,
		Params: map[string]any{
			"startDate": env.Now.Add(-5 * time.Minute).Format(time.RFC3339),
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
	testDeliverHandlesError(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ProjectName: "test"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
				BaseMetricsConfig: collector.BaseMetricsConfig{
					Discovery: env.Targets, Sinks: []sink.Sink{s},
					Clock: env.Clock, Interval: 30 * time.Second, Logger: slog.Default(),
				},
			})
		},
		types.WorkItem{ID: "metrics:proj-1", AliasKey: "proj-1"},
	)
}

func TestProjectMetricsGenerator_Deliver_EmptyResults(t *testing.T) {
	emptyJSON, _ := json.Marshal([]map[string]any{})
	testDeliverEmptyResults(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ProjectName: "test"}},
		func(env *genTestEnv) types.TaskGenerator {
			return collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
				BaseMetricsConfig: collector.BaseMetricsConfig{
					Discovery: env.Targets, Store: env.Store,
					Clock: env.Clock, Interval: 30 * time.Second, Logger: slog.Default(),
				},
			})
		},
		types.WorkItem{
			ID: "metrics:proj-1", AliasKey: "proj-1",
			Params: map[string]any{"startDate": time.Date(2026, 3, 9, 11, 55, 0, 0, time.UTC).Format(time.RFC3339)},
		},
		emptyJSON,
	)
}

func TestProjectMetricsGenerator_Poll_GapChunking(t *testing.T) {
	// Project metrics has one extra case not shared with other generators.
	cases := append(commonGapChunkCases(), gapChunkCase{
		name:           "gap exactly one chunk size",
		retention:      6 * time.Hour,
		chunkSize:      6 * time.Hour,
		maxItems:       50,
		wantMinItems:   1,
		wantMaxOpenEnd: 1,
		wantMinChunked: 0,
	})

	runGapChunkTests(t, cases, "endDate", func(env *genTestEnv, retention, chunkSize time.Duration, maxItems int) types.TaskGenerator {
		return collector.NewProjectMetricsGenerator(collector.ProjectMetricsGeneratorConfig{
			BaseMetricsConfig: collector.BaseMetricsConfig{
				Discovery:       env.Targets,
				Store:           env.Store,
				Clock:           env.Clock,
				Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
				SampleRate:      30,
				AvgWindow:       30,
				Interval:        30 * time.Second,
				MetricRetention: retention,
				ChunkSize:       chunkSize,
				MaxItemsPerPoll: maxItems,
				Logger:          slog.Default(),
			},
		})
	})
}
