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

func TestReplicaMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
		BaseMetricsConfig: collector.BaseMetricsConfig{
			Logger: slog.Default(),
		},
	})
	assert.Equal(t, types.TaskTypeMetrics, gen.Type())
}

func TestReplicaMetricsGenerator_Poll_EmitsItems(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"},
	})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
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

	for _, item := range items {
		assert.Equal(t, types.QueryReplicaMetrics, item.Kind)
		assert.Equal(t, types.TaskTypeMetrics, item.TaskType)

		// Replica metrics should not have groupBy in params
		_, hasGroupBy := item.Params["groupBy"]
		assert.False(t, hasGroupBy, "replica metrics should not have groupBy param")
	}
}

func TestReplicaMetricsGenerator_Poll_GapChunking(t *testing.T) {
	runGapChunkTests(t, commonGapChunkCases(), "endDate", func(env *genTestEnv, retention, chunkSize time.Duration, maxItems int) types.TaskGenerator {
		return collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
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

func TestReplicaMetricsGenerator_Deliver_ProcessesResults(t *testing.T) {
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

	gen := collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
		BaseMetricsConfig: collector.BaseMetricsConfig{
			Discovery:       env.Targets,
			Store:           env.Store,
			Sinks:           []sink.Sink{fakeSink},
			Clock:           env.Clock,
			Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			SampleRate:      30,
			AvgWindow:       30,
			Interval:        30 * time.Second,
			MetricRetention: 1 * time.Hour,
			Logger:          slog.Default(),
		},
	})

	// Replica metrics have replicaName instead of tags
	rawData := []map[string]any{
		{
			"measurement": "CPU_USAGE",
			"replicaName": "replica-1",
			"values": []map[string]any{
				{"ts": env.Now.Add(-2 * time.Minute).Unix(), "value": 0.5},
				{"ts": env.Now.Add(-1 * time.Minute).Unix(), "value": 0.6},
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "replica-metrics:svc-1:env-1",
		Kind:     types.QueryReplicaMetrics,
		TaskType: types.TaskTypeMetrics,
		AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"startDate": env.Now.Add(-5 * time.Minute).Format(time.RFC3339),
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 2)
	assert.Equal(t, "railway_cpu_usage_cores", collected[0].Name)
	assert.Equal(t, 0.5, collected[0].Value)

	// Verify replica_name label is present
	assert.Equal(t, "replica-1", collected[0].Labels["replica_name"])
	assert.Equal(t, "svc-1", collected[0].Labels["service_id"])
	assert.Equal(t, "env-1", collected[0].Labels["environment_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
}

func TestReplicaMetricsGenerator_Deliver_HandlesError(t *testing.T) {
	testDeliverHandlesError(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
				BaseMetricsConfig: collector.BaseMetricsConfig{
					Discovery: env.Targets, Sinks: []sink.Sink{s},
					Clock: env.Clock, Interval: 30 * time.Second, Logger: slog.Default(),
				},
			})
		},
		types.WorkItem{ID: "replica-metrics:svc-1:env-1", AliasKey: "svc-1:env-1"},
	)
}

func TestReplicaMetricsGenerator_Deliver_EmptyResults(t *testing.T) {
	emptyJSON, _ := json.Marshal([]map[string]any{})
	testDeliverEmptyResults(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"}},
		func(env *genTestEnv) types.TaskGenerator {
			return collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
				BaseMetricsConfig: collector.BaseMetricsConfig{
					Discovery: env.Targets, Store: env.Store,
					Clock: env.Clock, Interval: 30 * time.Second, Logger: slog.Default(),
				},
			})
		},
		types.WorkItem{
			ID: "replica-metrics:svc-1:env-1", AliasKey: "svc-1:env-1",
			Params: map[string]any{"startDate": time.Date(2026, 3, 9, 11, 55, 0, 0, time.UTC).Format(time.RFC3339)},
		},
		emptyJSON,
	)
}
