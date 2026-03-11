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

func TestServiceMetricsGenerator_Type(t *testing.T) {
	gen := collector.NewServiceMetricsGenerator(collector.ServiceMetricsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, collector.TaskTypeMetrics, gen.Type())
}

func TestServiceMetricsGenerator_Poll_EmitsItemsPerServiceEnv(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"},
		{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-2", ServiceName: "api", EnvironmentID: "env-1", EnvironmentName: "production"},
	})

	// No coverage -- gaps exist for both service+env pairs
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewServiceMetricsGenerator(collector.ServiceMetricsGeneratorConfig{
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

	// Should have items for both service+env pairs
	aliasKeys := make(map[string]bool)
	for _, item := range items {
		assert.Equal(t, collector.QueryServiceMetrics, item.Kind)
		assert.Equal(t, collector.TaskTypeMetrics, item.TaskType)
		aliasKeys[item.AliasKey] = true

		// Verify serviceId and environmentId are in params
		assert.Contains(t, item.Params, "serviceId")
		assert.Contains(t, item.Params, "environmentId")
	}
	assert.True(t, aliasKeys["svc-1:env-1"], "should have items for svc-1:env-1")
	assert.True(t, aliasKeys["svc-2:env-1"], "should have items for svc-2:env-1")
}

func TestServiceMetricsGenerator_Poll_RespectsInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
	}).AnyTimes()
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewServiceMetricsGenerator(collector.ServiceMetricsGeneratorConfig{
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

func TestServiceMetricsGenerator_Deliver_ProcessesResults(t *testing.T) {
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

	gen := collector.NewServiceMetricsGenerator(collector.ServiceMetricsGeneratorConfig{
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

	rawData := []map[string]any{
		{
			"measurement": "CPU_USAGE",
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
		ID:       "svc-metrics:svc-1:env-1",
		Kind:     collector.QueryServiceMetrics,
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
	assert.Equal(t, "railway_cpu_usage_cores", collected[1].Name)
	assert.Equal(t, 0.6, collected[1].Value)

	// Label assertions
	assert.Equal(t, "svc-1", collected[0].Labels["service_id"])
	assert.Equal(t, "env-1", collected[0].Labels["environment_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
}
