package collector_test

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestLogsGenerator_Type(t *testing.T) {
	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, types.TaskTypeLogs, gen.Type())
}

func TestLogsGenerator_Poll_EmitsAllLogTypes(t *testing.T) {
	env := setupGenTest(t)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	// All log types use coverage-driven gap filling
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery:       env.Targets,
		Store:           env.Store,
		Clock:           env.Clock,
		Types:           []string{"deployment", "build", "http"},
		Limit:           500,
		Interval:        30 * time.Second,
		LogRetention:    1 * time.Hour,
		MaxItemsPerPoll: 10,
		Logger:          slog.Default(),
	})

	items := gen.Poll(env.Now)
	require.NotEmpty(t, items)

	// Should have env log items, build log items, and HTTP log items
	hasEnvLogs := false
	hasBuildLogs := false
	hasHTTPLogs := false
	for _, item := range items {
		switch item.Kind {
		case types.QueryEnvironmentLogs:
			hasEnvLogs = true
			assert.Equal(t, "env-1", item.AliasKey)
		case types.QueryBuildLogs:
			hasBuildLogs = true
			assert.Equal(t, "dep-1", item.AliasKey)
		case types.QueryHttpLogs:
			hasHTTPLogs = true
			assert.Equal(t, "dep-1", item.AliasKey)
		}
	}
	assert.True(t, hasEnvLogs, "should have env log items")
	assert.True(t, hasBuildLogs, "should have build log items")
	assert.True(t, hasHTTPLogs, "should have HTTP log items")
}

func TestLogsGenerator_Poll_RespectsInterval(t *testing.T) {
	testRespectsInterval(t, 30*time.Second, func(env *genTestEnv) types.TaskGenerator {
		return collector.NewLogsGenerator(collector.LogsGeneratorConfig{
			Discovery:       env.Targets,
			Store:           env.Store,
			Clock:           env.Clock,
			Types:           []string{"deployment"},
			Limit:           500,
			Interval:        30 * time.Second,
			LogRetention:    1 * time.Hour,
			MaxItemsPerPoll: 10,
			Logger:          slog.Default(),
		})
	})
}

func TestLogsGenerator_Poll_DeduplicatesEnvironments(t *testing.T) {
	env := setupGenTest(t)

	// Two services in the same environment
	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"},
		{ProjectID: "proj-1", ServiceID: "svc-2", EnvironmentID: "env-1", DeploymentID: "dep-2"},
	})

	// Coverage-driven: only one coverage check per environment (deduplicated)
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery:       env.Targets,
		Store:           env.Store,
		Clock:           env.Clock,
		Types:           []string{"deployment"},
		Limit:           500,
		Interval:        30 * time.Second,
		LogRetention:    1 * time.Hour,
		MaxItemsPerPoll: 10,
		Logger:          slog.Default(),
	})

	items := gen.Poll(env.Now)
	// Should have env log items for env-1 only (deduplicated)
	envLogItems := 0
	for _, item := range items {
		if item.Kind == types.QueryEnvironmentLogs {
			envLogItems++
			assert.Equal(t, "env-1", item.AliasKey)
		}
	}
	assert.Greater(t, envLogItems, 0, "should have env log items")
}

func TestLogsGenerator_Poll_SkipsNoDeployment(t *testing.T) {
	env := setupGenTest(t)

	// Target without deployment ID
	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: ""},
	})

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets,
		Store:     env.Store,
		Clock:     env.Clock,
		Types:     []string{"build", "http"}, // no "deployment" type
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	items := gen.Poll(env.Now)
	// No build/HTTP items because deployment ID is empty
	assert.Nil(t, items)
}

func TestLogsGenerator_Poll_GapChunking(t *testing.T) {
	runGapChunkTests(t, commonGapChunkCases(), "beforeDate", func(env *genTestEnv, retention, chunkSize time.Duration, maxItems int) types.TaskGenerator {
		return collector.NewLogsGenerator(collector.LogsGeneratorConfig{
			Discovery:       env.Targets,
			Store:           env.Store,
			Clock:           env.Clock,
			Types:           []string{"deployment"},
			Limit:           500,
			Interval:        30 * time.Second,
			LogRetention:    retention,
			ChunkSize:       chunkSize,
			MaxItemsPerPoll: maxItems,
			Logger:          slog.Default(),
		})
	})
}

func TestLogsGenerator_Deliver_EnvironmentLogs(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	// Coverage recording (no cursor update)
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets,
		Store:     env.Store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     env.Clock,
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

	item := types.WorkItem{
		ID:       "envlogs:env-1",
		Kind:     types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs,
		AliasKey: "env-1",
		Params: map[string]any{
			"afterDate": afterDate,
		},
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
	env := setupGenTest(t)

	ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ProjectName: "test-project",
		ServiceID: "svc-1", ServiceName: "test-service",
		EnvironmentID: "env-1", EnvironmentName: "production",
		DeploymentID: "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	startDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets,
		Store:     env.Store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     env.Clock,
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

	item := types.WorkItem{
		ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{
			"startDate": startDate,
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "build started", collected[0].Message)
	assert.Equal(t, "build", collected[0].Labels["log_type"])
	assert.Equal(t, "dep-1", collected[0].Labels["deployment_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
}

func TestLogsGenerator_Deliver_HttpLogs(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	startDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ProjectName: "test-project",
		ServiceID: "svc-1", ServiceName: "test-service",
		EnvironmentID: "env-1", EnvironmentName: "production",
		DeploymentID: "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets,
		Store:     env.Store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     env.Clock,
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

	item := types.WorkItem{
		ID: "httplogs:dep-1", Kind: types.QueryHttpLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{
			"startDate": startDate,
		},
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

func TestLogsGenerator_Deliver_EnvironmentLogs_InvalidJSON(t *testing.T) {
	testDeliverInvalidJSON(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewLogsGenerator(collector.LogsGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Types: []string{"deployment"},
				Limit: 500, Interval: 30 * time.Second, Logger: slog.Default(),
			})
		},
		types.WorkItem{
			ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
			TaskType: types.TaskTypeLogs, AliasKey: "env-1",
		},
	)
}

func TestLogsGenerator_Deliver_HandlesError(t *testing.T) {
	testDeliverHandlesError(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewLogsGenerator(collector.LogsGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Types: []string{"deployment"},
				Interval: 30 * time.Second, Logger: slog.Default(),
			})
		},
		types.WorkItem{ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs, AliasKey: "env-1"},
	)
}

func TestLogsGenerator_Deliver_EnvironmentLogs_UnknownServiceID(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets,
		Store:     env.Store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     env.Clock,
		Types:     []string{"deployment"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp": ts,
			"message":   "unknown service log",
			"tags": map[string]any{
				"serviceId": "svc-unknown",
			},
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "envlogs:env-1",
		Kind:     types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs,
		AliasKey: "env-1",
		Params: map[string]any{
			"afterDate": afterDate,
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "svc-unknown", collected[0].Labels["service_id"])
	assert.NotContains(t, collected[0].Labels, "service_name")
}

func TestLogsGenerator_Deliver_EnvironmentLogs_ProjectIDFallback(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets,
		Store:     env.Store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     env.Clock,
		Types:     []string{"deployment"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp": ts,
			"message":   "project only log",
			"tags": map[string]any{
				"projectId": "proj-1",
			},
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "envlogs:env-1",
		Kind:     types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs,
		AliasKey: "env-1",
		Params: map[string]any{
			"afterDate": afterDate,
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "proj-1", collected[0].Labels["project_id"])
	assert.NotContains(t, collected[0].Labels, "service_name")
}

func TestLogsGenerator_Deliver_EnvironmentLogs_UnparseableTimestamp(t *testing.T) {
	env := setupGenTest(t)

	goodTS := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID:     "proj-1",
		ServiceID:     "svc-1",
		EnvironmentID: "env-1",
		DeploymentID:  "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets,
		Store:     env.Store,
		Sinks:     []sink.Sink{fakeSink},
		Clock:     env.Clock,
		Types:     []string{"deployment"},
		Limit:     500,
		Interval:  30 * time.Second,
		Logger:    slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp":  "not-a-timestamp",
			"message":    "bad ts",
			"attributes": []map[string]string{},
		},
		{
			"timestamp":  goodTS,
			"message":    "good ts",
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID:       "envlogs:env-1",
		Kind:     types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs,
		AliasKey: "env-1",
		Params: map[string]any{
			"afterDate": afterDate,
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "good ts", collected[0].Message)
}

func TestLogsGenerator_Deliver_HttpLogs_SeverityMapping(t *testing.T) {
	tests := []struct {
		httpStatus int
		wantSev    string
	}{
		{200, "info"},
		{301, "info"},
		{399, "info"},
		{400, "warn"},
		{404, "warn"},
		{499, "warn"},
		{500, "error"},
		{503, "error"},
		{599, "error"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.httpStatus), func(t *testing.T) {
			env := setupGenTest(t)

			ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
			startDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

			env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
				ProjectID:       "proj-1",
				ProjectName:     "test-project",
				ServiceID:       "svc-1",
				ServiceName:     "test-service",
				EnvironmentID:   "env-1",
				EnvironmentName: "production",
				DeploymentID:    "dep-1",
			}})

			env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
			env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

			var collected []sink.LogEntry
			fakeSink := &recordingSink{
				writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
					collected = logs
					return nil
				},
			}

			gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
				Discovery: env.Targets,
				Store:     env.Store,
				Sinks:     []sink.Sink{fakeSink},
				Clock:     env.Clock,
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
					"httpStatus":         tt.httpStatus,
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

			item := types.WorkItem{
				ID:       "httplogs:dep-1",
				Kind:     types.QueryHttpLogs,
				TaskType: types.TaskTypeLogs,
				AliasKey: "dep-1",
				Params: map[string]any{
					"startDate": startDate,
				},
			}

			gen.Deliver(context.Background(), item, data, nil)

			require.Len(t, collected, 1)
			assert.Equal(t, tt.wantSev, collected[0].Severity)
			assert.Equal(t, fmt.Sprintf("%d", tt.httpStatus), collected[0].Labels["status"])
		})
	}
}
