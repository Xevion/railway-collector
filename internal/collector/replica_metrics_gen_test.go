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

func TestReplicaMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, collector.TaskTypeMetrics, gen.Type())
}

func TestReplicaMetricsGenerator_Poll_EmitsItems(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"},
	})

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
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

	for _, item := range items {
		assert.Equal(t, collector.QueryReplicaMetrics, item.Kind)
		assert.Equal(t, collector.TaskTypeMetrics, item.TaskType)

		// Replica metrics should not have groupBy in params
		_, hasGroupBy := item.Params["groupBy"]
		assert.False(t, hasGroupBy, "replica metrics should not have groupBy param")
	}
}

func TestReplicaMetricsGenerator_Deliver_ProcessesResults(t *testing.T) {
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

	gen := collector.NewReplicaMetricsGenerator(collector.ReplicaMetricsGeneratorConfig{
		Discovery:       targets,
		Store:           store,
		Sinks:           []sink.Sink{fakeSink},
		Clock:           fakeClock,
		Measurements:    []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		SampleRate:      30,
		AvgWindow:       30,
		Interval:        30 * time.Second,
		MetricRetention: 1 * time.Hour,
		Logger:          slog.Default(),
	})

	// Replica metrics have replicaName instead of tags
	rawData := []map[string]any{
		{
			"measurement": "CPU_USAGE",
			"replicaName": "replica-1",
			"values": []map[string]any{
				{"ts": now.Add(-2 * time.Minute).Unix(), "value": 0.5},
				{"ts": now.Add(-1 * time.Minute).Unix(), "value": 0.6},
			},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID:       "replica-metrics:svc-1:env-1",
		Kind:     collector.QueryReplicaMetrics,
		TaskType: collector.TaskTypeMetrics,
		AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"startDate": now.Add(-5 * time.Minute).Format(time.RFC3339),
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
