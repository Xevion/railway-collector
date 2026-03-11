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
)

func TestUsageGenerator_Type(t *testing.T) {
	gen := collector.NewUsageGenerator(collector.UsageGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, types.TaskTypeUsage, gen.Type())
}

func TestUsageGenerator_Poll_EmitsTwoItemsPerProject(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", EnvironmentID: "env-1"},
		{ProjectID: "proj-2", ProjectName: "two", ServiceID: "svc-2", EnvironmentID: "env-2"},
	})

	gen := collector.NewUsageGenerator(collector.UsageGeneratorConfig{
		Discovery:    env.Targets,
		Clock:        env.Clock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		Interval:     1 * time.Hour,
		Logger:       slog.Default(),
	})

	items := gen.Poll(env.Now)
	require.NotEmpty(t, items)

	// Should have both QueryUsage and QueryEstimatedUsage for each project
	kindCounts := make(map[types.QueryKind]int)
	projectUsage := make(map[string]map[types.QueryKind]bool)
	for _, item := range items {
		assert.Equal(t, types.TaskTypeUsage, item.TaskType)
		kindCounts[item.Kind]++
		if projectUsage[item.AliasKey] == nil {
			projectUsage[item.AliasKey] = make(map[types.QueryKind]bool)
		}
		projectUsage[item.AliasKey][item.Kind] = true
	}

	// Two projects, two items each = 4 total
	assert.Len(t, items, 4)
	assert.Equal(t, 2, kindCounts[types.QueryUsage])
	assert.Equal(t, 2, kindCounts[types.QueryEstimatedUsage])

	// Each project should have both kinds
	assert.True(t, projectUsage["proj-1"][types.QueryUsage])
	assert.True(t, projectUsage["proj-1"][types.QueryEstimatedUsage])
	assert.True(t, projectUsage["proj-2"][types.QueryUsage])
	assert.True(t, projectUsage["proj-2"][types.QueryEstimatedUsage])
}

func TestUsageGenerator_Poll_DefaultInterval(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	}).AnyTimes()

	// Use default interval (should be 1 hour)
	gen := collector.NewUsageGenerator(collector.UsageGeneratorConfig{
		Discovery:    env.Targets,
		Clock:        env.Clock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		Logger:       slog.Default(),
	})

	// First poll returns items
	items := gen.Poll(env.Now)
	require.NotEmpty(t, items)

	// Poll 30 minutes later should return nil (default 1-hour interval not elapsed)
	items = gen.Poll(env.Now.Add(30 * time.Minute))
	assert.Nil(t, items)

	// Poll 1 hour later should return items
	items = gen.Poll(env.Now.Add(1 * time.Hour))
	require.NotEmpty(t, items)
}

func TestUsageGenerator_Deliver_Usage(t *testing.T) {
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

	gen := collector.NewUsageGenerator(collector.UsageGeneratorConfig{
		Discovery:    env.Targets,
		Sinks:        []sink.Sink{fakeSink},
		Clock:        env.Clock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		Logger:       slog.Default(),
	})

	rawData := []map[string]any{
		{
			"measurement": "CPU_USAGE",
			"value":       123.45,
			"tags": map[string]any{
				"projectId": "proj-1",
				"serviceId": "svc-1",
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "usage:proj-1",
		Kind:     types.QueryUsage,
		TaskType: types.TaskTypeUsage,
		AliasKey: "proj-1",
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "railway_usage_cpu", collected[0].Name)
	assert.Equal(t, 123.45, collected[0].Value)
	assert.Equal(t, "proj-1", collected[0].Labels["project_id"])
	assert.Equal(t, "svc-1", collected[0].Labels["service_id"])
}

func TestUsageGenerator_Deliver_EstimatedUsage(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:   "proj-1",
		ProjectName: "test-project",
		ServiceID:   "svc-1",
	}}).AnyTimes()

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewUsageGenerator(collector.UsageGeneratorConfig{
		Discovery:    env.Targets,
		Sinks:        []sink.Sink{fakeSink},
		Clock:        env.Clock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		Logger:       slog.Default(),
	})

	rawData := []map[string]any{
		{
			"estimatedValue": 99.5,
			"measurement":    "CPU_USAGE",
			"projectId":      "proj-1",
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "estimated-usage:proj-1",
		Kind:     types.QueryEstimatedUsage,
		TaskType: types.TaskTypeUsage,
		AliasKey: "proj-1",
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "railway_estimated_usage_cpu", collected[0].Name)
	assert.Equal(t, 99.5, collected[0].Value)
	assert.Equal(t, "proj-1", collected[0].Labels["project_id"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
}

func TestUsageGenerator_Deliver_Usage_ProjectOnlyFallback(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:   "proj-1",
		ProjectName: "test-project",
		ServiceID:   "svc-1",
		ServiceName: "test-service",
	}}).AnyTimes()

	var collected []sink.MetricPoint
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
			collected = pts
			return nil
		},
	}

	gen := collector.NewUsageGenerator(collector.UsageGeneratorConfig{
		Discovery:    env.Targets,
		Sinks:        []sink.Sink{fakeSink},
		Clock:        env.Clock,
		Measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		Logger:       slog.Default(),
	})

	rawData := []map[string]any{
		{
			"measurement": "CPU_USAGE",
			"value":       50.0,
			"tags": map[string]any{
				"projectId": "proj-1",
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		Kind:     types.QueryUsage,
		TaskType: types.TaskTypeUsage,
		AliasKey: "proj-1",
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "proj-1", collected[0].Labels["project_id"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
	assert.NotContains(t, collected[0].Labels, "service_id")
}

func TestUsageGenerator_Deliver_Usage_InvalidJSON(t *testing.T) {
	testDeliverInvalidJSON(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ProjectName: "test", ServiceID: "svc-1", EnvironmentID: "env-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewUsageGenerator(collector.UsageGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Logger: slog.Default(),
			})
		},
		types.WorkItem{
			ID: "usage:proj-1", Kind: types.QueryUsage,
			TaskType: types.TaskTypeUsage, AliasKey: "proj-1",
		},
	)
}

func TestUsageGenerator_Deliver_EstimatedUsage_InvalidJSON(t *testing.T) {
	testDeliverInvalidJSON(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ProjectName: "test", ServiceID: "svc-1", EnvironmentID: "env-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewUsageGenerator(collector.UsageGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Logger: slog.Default(),
			})
		},
		types.WorkItem{
			ID: "estimated-usage:proj-1", Kind: types.QueryEstimatedUsage,
			TaskType: types.TaskTypeUsage, AliasKey: "proj-1",
		},
	)
}

func TestUsageGenerator_Deliver_HandlesError(t *testing.T) {
	testDeliverHandlesError(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ProjectName: "test", ServiceID: "svc-1", EnvironmentID: "env-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewUsageGenerator(collector.UsageGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Logger: slog.Default(),
			})
		},
		types.WorkItem{ID: "usage:proj-1", Kind: types.QueryUsage, AliasKey: "proj-1"},
	)
}

func TestUsageGenerator_Deliver_EmptyResults(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "test", ServiceID: "svc-1", EnvironmentID: "env-1"},
	}).AnyTimes()

	gen := collector.NewUsageGenerator(collector.UsageGeneratorConfig{
		Discovery: env.Targets,
		Sinks:     []sink.Sink{&recordingSink{}},
		Clock:     env.Clock,
		Logger:    slog.Default(),
	})

	data, _ := json.Marshal([]map[string]any{})
	item := types.WorkItem{
		ID:       "usage:proj-1",
		Kind:     types.QueryUsage,
		AliasKey: "proj-1",
	}

	gen.Deliver(context.Background(), item, data, nil)
}
