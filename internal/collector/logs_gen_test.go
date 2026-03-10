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

func TestLogsGenerator_Type(t *testing.T) {
	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, collector.TaskTypeLogs, gen.Type())
}

func TestLogsGenerator_Poll_EmitsAllLogTypes(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()
	envCursor := now.Add(-5 * time.Minute)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	store.EXPECT().GetLogCursor("env-1", "environment").Return(envCursor)
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Clock:     fakeClock,
		Types:     []string{"deployment", "build", "http"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	items := gen.Poll(now)
	require.Len(t, items, 3)

	// Environment logs
	assert.Equal(t, "envlogs:env-1", items[0].ID)
	assert.Equal(t, collector.QueryEnvironmentLogs, items[0].Kind)
	assert.Equal(t, "env-1", items[0].AliasKey)
	assert.Equal(t, envCursor.Format(time.RFC3339Nano), items[0].Params["afterDate"])

	// Build logs
	assert.Equal(t, "buildlogs:dep-1", items[1].ID)
	assert.Equal(t, collector.QueryBuildLogs, items[1].Kind)
	assert.Equal(t, "dep-1", items[1].AliasKey)
	_, hasStartDate := items[1].Params["startDate"]
	assert.False(t, hasStartDate, "build logs with no cursor should not have startDate")

	// HTTP logs
	assert.Equal(t, "httplogs:dep-1", items[2].ID)
	assert.Equal(t, collector.QueryHttpLogs, items[2].Kind)
	assert.Equal(t, "dep-1", items[2].AliasKey)
}

func TestLogsGenerator_Poll_RespectsInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	now := fakeClock.Now()

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}}).AnyTimes()
	store.EXPECT().GetLogCursor(gomock.Any(), gomock.Any()).Return(time.Time{}).AnyTimes()

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Clock:     fakeClock,
		Types:     []string{"deployment"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	items := gen.Poll(now)
	require.Len(t, items, 1)

	// Immediate second poll returns nil
	items = gen.Poll(now.Add(1 * time.Second))
	assert.Nil(t, items)

	// After interval, returns items
	items = gen.Poll(now.Add(30 * time.Second))
	require.Len(t, items, 1)
}

func TestLogsGenerator_Poll_DeduplicatesEnvironments(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	// Two services in the same environment
	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"},
		{ProjectID: "proj-1", ServiceID: "svc-2", EnvironmentID: "env-1", DeploymentID: "dep-2"},
	})

	// Only one env cursor call (deduplicated)
	store.EXPECT().GetLogCursor("env-1", "environment").Return(time.Time{}).Times(1)

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Clock:     fakeClock,
		Types:     []string{"deployment"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	items := gen.Poll(fakeClock.Now())
	// Only 1 env log item (deduplicated), not 2
	require.Len(t, items, 1)
	assert.Equal(t, "envlogs:env-1", items[0].ID)
}

func TestLogsGenerator_Poll_SkipsNoDeployment(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	// Target without deployment ID
	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: ""},
	})

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Clock:     fakeClock,
		Types:     []string{"build", "http"}, // no "deployment" type
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	items := gen.Poll(fakeClock.Now())
	// No build/HTTP items because deployment ID is empty
	assert.Nil(t, items)
}

func TestLogsGenerator_Deliver_EnvironmentLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	expectedTS, _ := time.Parse(time.RFC3339Nano, ts)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	store.EXPECT().SetLogCursor("env-1", "environment", gomock.Eq(expectedTS)).Return(nil)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     fakeClock,
		Types:     []string{"deployment"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp": ts,
			"message":   "test log message",
			"severity":  "info",
			"tags": map[string]any{
				"serviceId": "svc-1",
			},
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID:       "envlogs:env-1",
		Kind:     collector.QueryEnvironmentLogs,
		TaskType: collector.TaskTypeLogs,
		AliasKey: "env-1",
		Params:   map[string]any{},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "test log message", collected[0].Message)
	assert.Equal(t, "deployment", collected[0].Labels["log_type"])
	assert.Equal(t, "svc-1", collected[0].Labels["service_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
}

func TestLogsGenerator_Deliver_BuildLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	expectedTS, _ := time.Parse(time.RFC3339Nano, ts)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID: "proj-1", ProjectName: "test-project",
		ServiceID: "svc-1", ServiceName: "test-service",
		EnvironmentID: "env-1", EnvironmentName: "production",
		DeploymentID: "dep-1",
	}})

	store.EXPECT().SetLogCursor("dep-1", "build", gomock.Eq(expectedTS)).Return(nil)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     fakeClock,
		Types:     []string{"build"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp":  ts,
			"message":    "build started",
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID: "buildlogs:dep-1", Kind: collector.QueryBuildLogs,
		TaskType: collector.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "build started", collected[0].Message)
	assert.Equal(t, "build", collected[0].Labels["log_type"])
	assert.Equal(t, "dep-1", collected[0].Labels["deployment_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
}

func TestLogsGenerator_Deliver_HttpLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	expectedTS, _ := time.Parse(time.RFC3339Nano, ts)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID: "proj-1", ProjectName: "test-project",
		ServiceID: "svc-1", ServiceName: "test-service",
		EnvironmentID: "env-1", EnvironmentName: "production",
		DeploymentID: "dep-1",
	}})

	store.EXPECT().SetLogCursor("dep-1", "http", gomock.Eq(expectedTS)).Return(nil)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Store:     store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     fakeClock,
		Types:     []string{"http"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp":          ts,
			"method":             "GET",
			"path":               "/api",
			"host":               "example.com",
			"httpStatus":         200,
			"totalDuration":      50,
			"upstreamRqDuration": 40,
			"srcIp":              "1.2.3.4",
			"clientUa":           "test-agent",
			"rxBytes":            100,
			"txBytes":            200,
			"edgeRegion":         "us-east-1",
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := collector.WorkItem{
		ID: "httplogs:dep-1", Kind: collector.QueryHttpLogs,
		TaskType: collector.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "GET /api 200", collected[0].Message)
	assert.Equal(t, "http", collected[0].Labels["log_type"])
	assert.Equal(t, "GET", collected[0].Labels["method"])
	assert.Equal(t, "200", collected[0].Labels["status"])
	assert.Equal(t, "us-east-1", collected[0].Labels["edge_region"])
	assert.Equal(t, "50", collected[0].Attributes["total_duration_ms"])
	assert.Equal(t, "info", collected[0].Severity)
}

func TestLogsGenerator_Deliver_HandlesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	fakeSink := &recordingSink{}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: targets,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     fakeClock,
		Types:     []string{"deployment"},
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	item := collector.WorkItem{
		ID: "envlogs:env-1", Kind: collector.QueryEnvironmentLogs,
		AliasKey: "env-1",
	}

	// Should not panic
	gen.Deliver(context.Background(), item, nil, assert.AnError)
}
