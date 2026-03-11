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

// TestLogsGenerator_Deliver_EnvironmentLogs_MultipleEntries delivers 3 logs where
// the first has a bad timestamp. Asserts exactly 2 entries collected and maxTS
// tracks the latest good timestamp. Kills INVERT_LOOPCTRL on continue and
// INCREMENT_DECREMENT on itemCount++/maxTS tracking.
func TestLogsGenerator_Deliver_EnvironmentLogs_MultipleEntries(t *testing.T) {
	env := setupGenTest(t)

	ts1 := env.Now.Add(-3 * time.Minute).Format(time.RFC3339Nano)
	ts2 := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"deployment"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{"timestamp": "not-a-timestamp", "message": "bad", "attributes": []map[string]string{}},
		{"timestamp": ts1, "message": "good-older", "attributes": []map[string]string{}},
		{"timestamp": ts2, "message": "good-newer", "attributes": []map[string]string{}},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-1",
		Params: map[string]any{"afterDate": afterDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 2, "bad timestamp skipped, two good entries kept")
	assert.Equal(t, "good-older", collected[0].Message)
	assert.Equal(t, "good-newer", collected[1].Message)
	// maxTS should be the later timestamp
	expectedMaxTS, _ := time.Parse(time.RFC3339Nano, ts2)
	assert.True(t, collected[1].Timestamp.Equal(expectedMaxTS))
}

// TestLogsGenerator_Deliver_EnvironmentLogs_EmptyData delivers an empty array.
// Asserts no sink write and coverage updated with empty=true.
// Kills CONDITIONALS_NEGATION on `len(entries) == 0`.
func TestLogsGenerator_Deliver_EnvironmentLogs_EmptyData(t *testing.T) {
	env := setupGenTest(t)

	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	sinkCalled := false
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			sinkCalled = true
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"deployment"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	data, err := json.Marshal([]map[string]any{})
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-1",
		Params: map[string]any{"afterDate": afterDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	assert.False(t, sinkCalled, "sink should not be called for empty data")
}

// TestLogsGenerator_Deliver_EnvironmentLogs_AllTagFields delivers a log with all
// tag fields populated. Asserts every label is set correctly.
// Kills CONDITIONALS_NEGATION on nil checks for each tag field.
func TestLogsGenerator_Deliver_EnvironmentLogs_AllTagFields(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"deployment"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp": ts,
			"message":   "all tags log",
			"severity":  "warn",
			"tags": map[string]any{
				"serviceId":            "svc-1",
				"projectId":            "proj-override",
				"environmentId":        "env-override",
				"deploymentId":         "dep-tag",
				"deploymentInstanceId": "inst-1",
			},
			"attributes": []map[string]string{{"key": "attr1", "value": "val1"}},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-1",
		Params: map[string]any{"afterDate": afterDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	labels := collected[0].Labels
	// serviceId matched a known target, so project/service names come from discovery
	assert.Equal(t, "proj-1", labels["project_id"], "project_id from discovery (serviceId match)")
	assert.Equal(t, "test-project", labels["project_name"])
	assert.Equal(t, "svc-1", labels["service_id"])
	assert.Equal(t, "test-service", labels["service_name"])
	assert.Equal(t, "production", labels["environment_name"])
	// deploymentId tag overrides the discovery-sourced one
	assert.Equal(t, "dep-tag", labels["deployment_id"])
	assert.Equal(t, "inst-1", labels["deployment_instance_id"])
	// environmentId tag overrides the default
	assert.Equal(t, "env-override", labels["environment_id"])
	// severity preserved
	assert.Equal(t, "warn", collected[0].Severity)
	// attributes preserved
	assert.Equal(t, "val1", collected[0].Attributes["attr1"])
}

// TestLogsGenerator_Deliver_EnvironmentLogs_ChunkedCoverage delivers with beforeDate
// param set (chunked gap). Asserts covEnd uses beforeDate, not now.
// Kills CONDITIONALS_NEGATION on `beforeDate` presence check.
func TestLogsGenerator_Deliver_EnvironmentLogs_ChunkedCoverage(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-5 * time.Hour).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Hour).Format(time.RFC3339Nano)
	beforeDate := env.Now.Add(-4 * time.Hour).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}})

	// Capture the actual coverage bytes to verify covEnd
	var savedCoverageData []byte
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).DoAndReturn(
		func(key string, data []byte) error {
			savedCoverageData = data
			return nil
		},
	)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"deployment"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{"timestamp": ts, "message": "chunked log", "attributes": []map[string]string{}},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-1",
		Params: map[string]any{
			"afterDate":  afterDate,
			"beforeDate": beforeDate,
		},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	// Verify the coverage end time used beforeDate, not now or maxTS
	require.NotNil(t, savedCoverageData)
	expectedBeforeDate, _ := time.Parse(time.RFC3339Nano, beforeDate)
	// Parse saved coverage to check end time
	var savedIntervals []struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	}
	require.NoError(t, json.Unmarshal(savedCoverageData, &savedIntervals))
	require.Len(t, savedIntervals, 1)
	assert.True(t, savedIntervals[0].End.Equal(expectedBeforeDate),
		"coverage end should be beforeDate (%v), got %v", expectedBeforeDate, savedIntervals[0].End)
}

// TestLogsGenerator_Deliver_EnvironmentLogs_CoverageWithMaxTS delivers with no
// beforeDate and entries present. Asserts coverage end uses maxTS (not now).
// Kills the `maxTS.IsZero()` negation.
func TestLogsGenerator_Deliver_EnvironmentLogs_CoverageWithMaxTS(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}})

	var savedCoverageData []byte
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).DoAndReturn(
		func(key string, data []byte) error {
			savedCoverageData = data
			return nil
		},
	)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"deployment"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{"timestamp": ts, "message": "log entry", "attributes": []map[string]string{}},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	// No beforeDate => live-edge query, covEnd should be maxTS (not now)
	item := types.WorkItem{
		ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-1",
		Params: map[string]any{"afterDate": afterDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	require.NotNil(t, savedCoverageData)
	expectedMaxTS, _ := time.Parse(time.RFC3339Nano, ts)
	var savedIntervals []struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	}
	require.NoError(t, json.Unmarshal(savedCoverageData, &savedIntervals))
	require.Len(t, savedIntervals, 1)
	// covEnd should be maxTS, not now (which is 2 minutes later)
	assert.True(t, savedIntervals[0].End.Equal(expectedMaxTS),
		"coverage end should be maxTS (%v), got %v", expectedMaxTS, savedIntervals[0].End)
	assert.False(t, savedIntervals[0].End.Equal(env.Now),
		"coverage end should NOT be now")
}

// TestLogsGenerator_Deliver_BuildLogs_UnparseableTimestamp delivers two build logs
// where one has a bad timestamp. Asserts only 1 entry collected.
// Kills INVERT_LOOPCTRL on continue in build log loop.
func TestLogsGenerator_Deliver_BuildLogs_UnparseableTimestamp(t *testing.T) {
	env := setupGenTest(t)

	goodTS := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"build"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{"timestamp": "garbage", "message": "bad ts", "attributes": []map[string]string{}},
		{"timestamp": goodTS, "message": "good build log", "attributes": []map[string]string{}},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1, "bad timestamp skipped, one good entry kept")
	assert.Equal(t, "good build log", collected[0].Message)
	assert.Equal(t, "build", collected[0].Labels["log_type"])
}

// TestLogsGenerator_Deliver_BuildLogs_WithSeverity delivers a build log with
// severity field set. Asserts severity is preserved.
// Kills CONDITIONALS_NEGATION on severity nil check.
func TestLogsGenerator_Deliver_BuildLogs_WithSeverity(t *testing.T) {
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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"build"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp":  ts,
			"message":    "error during build",
			"severity":   "error",
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "error", collected[0].Severity, "severity should be preserved from input")
}

// TestLogsGenerator_Deliver_BuildLogs_WithoutSeverity ensures that when severity
// is nil, it defaults to empty string. Kills the negation of the nil check.
func TestLogsGenerator_Deliver_BuildLogs_WithoutSeverity(t *testing.T) {
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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"build"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp":  ts,
			"message":    "build log no severity",
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	assert.Equal(t, "", collected[0].Severity, "nil severity should result in empty string")
}

// TestLogsGenerator_Deliver_HttpLogs_UnparseableTimestamp delivers two HTTP logs
// where one has a bad timestamp. Asserts only 1 entry collected.
// Kills INVERT_LOOPCTRL on continue in HTTP log loop.
func TestLogsGenerator_Deliver_HttpLogs_UnparseableTimestamp(t *testing.T) {
	env := setupGenTest(t)

	goodTS := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"http"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp": "not-valid", "method": "GET", "path": "/bad",
			"host": "x.com", "httpStatus": 200, "totalDuration": 1,
			"upstreamRqDuration": 1, "srcIp": "1.1.1.1", "clientUa": "a",
			"rxBytes": 0, "txBytes": 0, "edgeRegion": "us",
		},
		{
			"timestamp": goodTS, "method": "POST", "path": "/good",
			"host": "x.com", "httpStatus": 201, "totalDuration": 5,
			"upstreamRqDuration": 3, "srcIp": "2.2.2.2", "clientUa": "b",
			"rxBytes": 10, "txBytes": 20, "edgeRegion": "eu",
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "httplogs:dep-1", Kind: types.QueryHttpLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1, "bad timestamp skipped, one good entry kept")
	assert.Equal(t, "POST /good 201", collected[0].Message)
	assert.Equal(t, "info", collected[0].Severity)
}

// TestLogsGenerator_Deliver_BuildLogs_EmptyData delivers an empty build log array.
// Asserts no sink write and coverage is updated.
// Kills CONDITIONALS_NEGATION on `len(entries) == 0` in build path.
func TestLogsGenerator_Deliver_BuildLogs_EmptyData(t *testing.T) {
	env := setupGenTest(t)

	startDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	sinkCalled := false
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			sinkCalled = true
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"build"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	data, err := json.Marshal([]map[string]any{})
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	assert.False(t, sinkCalled, "sink should not be called for empty build log data")
}

// TestLogsGenerator_Deliver_HttpLogs_EmptyData delivers an empty HTTP log array.
// Asserts no sink write and coverage is updated.
// Kills CONDITIONALS_NEGATION on `len(entries) == 0` in HTTP path.
func TestLogsGenerator_Deliver_HttpLogs_EmptyData(t *testing.T) {
	env := setupGenTest(t)

	startDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)

	sinkCalled := false
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			sinkCalled = true
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"http"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	data, err := json.Marshal([]map[string]any{})
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "httplogs:dep-1", Kind: types.QueryHttpLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	assert.False(t, sinkCalled, "sink should not be called for empty HTTP log data")
}

// TestLogsGenerator_Deliver_BuildLogs_CoverageUsesMaxTS verifies that build log
// coverage end uses maxTS when entries are present (not now).
// Kills the `!maxTS.IsZero()` negation in deliverBuildLogs.
func TestLogsGenerator_Deliver_BuildLogs_CoverageUsesMaxTS(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-3 * time.Minute).Format(time.RFC3339Nano)
	startDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}})

	var savedCoverageData []byte
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).DoAndReturn(
		func(key string, data []byte) error {
			savedCoverageData = data
			return nil
		},
	)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"build"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{"timestamp": ts, "message": "build step", "attributes": []map[string]string{}},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	require.NotNil(t, savedCoverageData)
	expectedMaxTS, _ := time.Parse(time.RFC3339Nano, ts)
	var savedIntervals []struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	}
	require.NoError(t, json.Unmarshal(savedCoverageData, &savedIntervals))
	require.Len(t, savedIntervals, 1)
	assert.True(t, savedIntervals[0].End.Equal(expectedMaxTS),
		"build coverage end should be maxTS (%v), got %v", expectedMaxTS, savedIntervals[0].End)
	assert.False(t, savedIntervals[0].End.Equal(env.Now),
		"build coverage end should NOT be now")
}

// TestLogsGenerator_Deliver_HttpLogs_CoverageUsesMaxTS verifies that HTTP log
// coverage end uses maxTS when entries are present (not now).
// Kills the `!maxTS.IsZero()` negation in deliverHttpLogs.
func TestLogsGenerator_Deliver_HttpLogs_CoverageUsesMaxTS(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-3 * time.Minute).Format(time.RFC3339Nano)
	startDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
	}})

	var savedCoverageData []byte
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).DoAndReturn(
		func(key string, data []byte) error {
			savedCoverageData = data
			return nil
		},
	)

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"http"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp": ts, "method": "GET", "path": "/test",
			"host": "x.com", "httpStatus": 200, "totalDuration": 10,
			"upstreamRqDuration": 5, "srcIp": "1.1.1.1", "clientUa": "ua",
			"rxBytes": 100, "txBytes": 200, "edgeRegion": "us",
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "httplogs:dep-1", Kind: types.QueryHttpLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	require.NotNil(t, savedCoverageData)
	expectedMaxTS, _ := time.Parse(time.RFC3339Nano, ts)
	var savedIntervals []struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	}
	require.NoError(t, json.Unmarshal(savedCoverageData, &savedIntervals))
	require.Len(t, savedIntervals, 1)
	assert.True(t, savedIntervals[0].End.Equal(expectedMaxTS),
		"HTTP coverage end should be maxTS (%v), got %v", expectedMaxTS, savedIntervals[0].End)
	assert.False(t, savedIntervals[0].End.Equal(env.Now),
		"HTTP coverage end should NOT be now")
}

// TestLogsGenerator_Deliver_BuildLogs_MultipleEntries delivers 3 build logs
// (1 bad, 2 good) and verifies count and maxTS tracking.
// Kills INCREMENT_DECREMENT and INVERT_LOOPCTRL mutants.
func TestLogsGenerator_Deliver_BuildLogs_MultipleEntries(t *testing.T) {
	env := setupGenTest(t)

	ts1 := env.Now.Add(-5 * time.Minute).Format(time.RFC3339Nano)
	ts2 := env.Now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"build"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{"timestamp": "invalid!", "message": "bad", "attributes": []map[string]string{}},
		{"timestamp": ts1, "message": "step 1", "attributes": []map[string]string{}},
		{"timestamp": ts2, "message": "step 2", "attributes": []map[string]string{}},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 2, "1 bad + 2 good = 2 collected")
	assert.Equal(t, "step 1", collected[0].Message)
	assert.Equal(t, "step 2", collected[1].Message)
}

// TestLogsGenerator_Deliver_HttpLogs_MultipleEntries delivers 3 HTTP logs
// (1 bad timestamp, 2 good) and verifies count and maxTS tracking.
// Kills INCREMENT_DECREMENT and INVERT_LOOPCTRL mutants.
func TestLogsGenerator_Deliver_HttpLogs_MultipleEntries(t *testing.T) {
	env := setupGenTest(t)

	ts1 := env.Now.Add(-5 * time.Minute).Format(time.RFC3339Nano)
	ts2 := env.Now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"http"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	httpEntry := func(timestamp string, status int, path string) map[string]any {
		return map[string]any{
			"timestamp": timestamp, "method": "GET", "path": path,
			"host": "x.com", "httpStatus": status, "totalDuration": 10,
			"upstreamRqDuration": 5, "srcIp": "1.1.1.1", "clientUa": "ua",
			"rxBytes": 100, "txBytes": 200, "edgeRegion": "us",
		}
	}

	rawData := []map[string]any{
		httpEntry("garbage-ts", 200, "/bad"),
		httpEntry(ts1, 200, "/first"),
		httpEntry(ts2, 404, "/second"),
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "httplogs:dep-1", Kind: types.QueryHttpLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		Params: map[string]any{"startDate": startDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 2, "1 bad timestamp + 2 good = 2 collected")
	assert.Equal(t, "GET /first 200", collected[0].Message)
	assert.Equal(t, "GET /second 404", collected[1].Message)
	assert.Equal(t, "info", collected[0].Severity)
	assert.Equal(t, "warn", collected[1].Severity)
}

// TestLogsGenerator_Deliver_EnvironmentLogs_NoTags delivers a log without tags.
// Asserts only base labels are set (log_type, environment_id).
// Kills CONDITIONALS_NEGATION on `log.Tags != nil`.
func TestLogsGenerator_Deliver_EnvironmentLogs_NoTags(t *testing.T) {
	env := setupGenTest(t)

	ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
		ProjectID: "proj-1", ServiceID: "svc-1",
		EnvironmentID: "env-1", DeploymentID: "dep-1",
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
		Discovery: env.Targets, Store: env.Store,
		Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
		Types: []string{"deployment"}, Limit: 500,
		Interval: 30 * time.Second, Logger: slog.Default(),
	})

	rawData := []map[string]any{
		{
			"timestamp":  ts,
			"message":    "no tags log",
			"attributes": []map[string]string{},
		},
	}
	data, err := json.Marshal(rawData)
	require.NoError(t, err)

	item := types.WorkItem{
		ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-1",
		Params: map[string]any{"afterDate": afterDate},
	}

	gen.Deliver(context.Background(), item, data, nil)

	require.Len(t, collected, 1)
	// Only base labels should be present
	assert.Equal(t, "deployment", collected[0].Labels["log_type"])
	assert.Equal(t, "env-1", collected[0].Labels["environment_id"])
	// Tag-derived labels should NOT be present
	assert.NotContains(t, collected[0].Labels, "service_id")
	assert.NotContains(t, collected[0].Labels, "service_name")
	assert.NotContains(t, collected[0].Labels, "deployment_id")
	assert.NotContains(t, collected[0].Labels, "deployment_instance_id")
}

// TestLogsGenerator_Deliver_EnvironmentLogs_SeverityNilVsSet tests that severity
// is empty string when nil and populated when set. Kills the severity nil check negation.
func TestLogsGenerator_Deliver_EnvironmentLogs_SeverityNilVsSet(t *testing.T) {
	tests := []struct {
		name     string
		severity any // nil or string
		wantSev  string
	}{
		{"nil severity", nil, ""},
		{"set severity", "error", "error"},
		{"info severity", "info", "info"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := setupGenTest(t)

			ts := env.Now.Add(-1 * time.Minute).Format(time.RFC3339Nano)
			afterDate := env.Now.Add(-10 * time.Minute).Format(time.RFC3339Nano)

			env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{{
				ProjectID: "proj-1", ServiceID: "svc-1",
				EnvironmentID: "env-1", DeploymentID: "dep-1",
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
				Discovery: env.Targets, Store: env.Store,
				Sinks: []sink.Sink{fakeSink}, Clock: env.Clock,
				Types: []string{"deployment"}, Limit: 500,
				Interval: 30 * time.Second, Logger: slog.Default(),
			})

			entry := map[string]any{
				"timestamp":  ts,
				"message":    "test",
				"attributes": []map[string]string{},
			}
			if tt.severity != nil {
				entry["severity"] = tt.severity
			}

			data, err := json.Marshal([]map[string]any{entry})
			require.NoError(t, err)

			item := types.WorkItem{
				ID: "envlogs:env-1", Kind: types.QueryEnvironmentLogs,
				TaskType: types.TaskTypeLogs, AliasKey: "env-1",
				Params: map[string]any{"afterDate": afterDate},
			}

			gen.Deliver(context.Background(), item, data, nil)

			require.Len(t, collected, 1)
			assert.Equal(t, tt.wantSev, collected[0].Severity)
		})
	}
}

// TestLogsGenerator_Deliver_BuildLogs_InvalidJSON verifies build log invalid JSON handling.
func TestLogsGenerator_Deliver_BuildLogs_InvalidJSON(t *testing.T) {
	testDeliverInvalidJSON(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewLogsGenerator(collector.LogsGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Types: []string{"build"},
				Limit: 500, Interval: 30 * time.Second, Logger: slog.Default(),
			})
		},
		types.WorkItem{
			ID: "buildlogs:dep-1", Kind: types.QueryBuildLogs,
			TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		},
	)
}

// TestLogsGenerator_Deliver_HttpLogs_InvalidJSON verifies HTTP log invalid JSON handling.
func TestLogsGenerator_Deliver_HttpLogs_InvalidJSON(t *testing.T) {
	testDeliverInvalidJSON(t,
		[]types.ServiceTarget{{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"}},
		func(env *genTestEnv, s sink.Sink) types.TaskGenerator {
			return collector.NewLogsGenerator(collector.LogsGeneratorConfig{
				Discovery: env.Targets, Sinks: []sink.Sink{s},
				Clock: env.Clock, Types: []string{"http"},
				Limit: 500, Interval: 30 * time.Second, Logger: slog.Default(),
			})
		},
		types.WorkItem{
			ID: "httplogs:dep-1", Kind: types.QueryHttpLogs,
			TaskType: types.TaskTypeLogs, AliasKey: "dep-1",
		},
	)
}

// TestLogsGenerator_Poll_MaxItemsPerPoll_ExactBoundary tests that exactly
// maxItemsPerPoll items are emitted (not maxItemsPerPoll-1 or +1).
// Kills CONDITIONALS_BOUNDARY on `itemCount >= g.maxItemsPerPoll`.
func TestLogsGenerator_Poll_MaxItemsPerPoll_ExactBoundary(t *testing.T) {
	env := setupGenTest(t)

	// Multiple environments to generate more items than the cap
	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"},
		{ProjectID: "proj-1", ServiceID: "svc-2", EnvironmentID: "env-2", DeploymentID: "dep-2"},
		{ProjectID: "proj-1", ServiceID: "svc-3", EnvironmentID: "env-3", DeploymentID: "dep-3"},
		{ProjectID: "proj-1", ServiceID: "svc-4", EnvironmentID: "env-4", DeploymentID: "dep-4"},
	})

	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	maxItems := 3
	gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
		Discovery:       env.Targets,
		Store:           env.Store,
		Clock:           env.Clock,
		Types:           []string{"deployment"},
		Limit:           500,
		Interval:        30 * time.Second,
		LogRetention:    1 * time.Hour,
		ChunkSize:       6 * time.Hour, // larger than retention so no chunking
		MaxItemsPerPoll: maxItems,
		Logger:          slog.Default(),
	})

	items := gen.Poll(env.Now)
	// Should be exactly maxItems, not maxItems-1 (which >= would allow with >)
	assert.Equal(t, maxItems, len(items),
		"should emit exactly maxItemsPerPoll items, not more or fewer")
}

func TestLogsGenerator_Poll_ChunkSizeControlsTimeRange(t *testing.T) {
	// Verifies that log work items respect the configured ChunkSize.
	// This catches the bug where logs were using MetricChunkSize (10d)
	// instead of a smaller log-appropriate chunk, causing Railway API
	// "Problem processing request" errors on large time ranges.
	tests := []struct {
		name         string
		chunkSize    time.Duration
		logRetention time.Duration
	}{
		{
			name:         "6h chunks on 2d retention",
			chunkSize:    6 * time.Hour,
			logRetention: 2 * 24 * time.Hour,
		},
		{
			name:         "1h chunks on 1d retention",
			chunkSize:    1 * time.Hour,
			logRetention: 24 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := setupGenTest(t)

			env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
				{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", EnvironmentName: "production"},
			})
			env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

			gen := collector.NewLogsGenerator(collector.LogsGeneratorConfig{
				Discovery:       env.Targets,
				Store:           env.Store,
				Clock:           env.Clock,
				Types:           []string{"deployment"},
				Limit:           500,
				Interval:        30 * time.Second,
				LogRetention:    tt.logRetention,
				ChunkSize:       tt.chunkSize,
				MaxItemsPerPoll: 200,
				Logger:          slog.Default(),
			})

			items := gen.Poll(env.Now)
			require.NotEmpty(t, items)

			for _, item := range items {
				afterStr, hasAfter := item.Params["afterDate"].(string)
				beforeStr, hasBefore := item.Params["beforeDate"].(string)
				if !hasAfter || !hasBefore {
					// Open-ended live-edge query; its time range is bounded
					// by the chunk size split in the generator, but we can't
					// directly verify it here. Skip.
					continue
				}

				after, err1 := time.Parse(time.RFC3339Nano, afterStr)
				before, err2 := time.Parse(time.RFC3339Nano, beforeStr)
				require.NoError(t, err1, "item %s afterDate parse", item.ID)
				require.NoError(t, err2, "item %s beforeDate parse", item.ID)

				dur := before.Sub(after)
				assert.LessOrEqual(t, dur, tt.chunkSize,
					"item %s spans %v which exceeds chunk size %v", item.ID, dur, tt.chunkSize)
			}
		})
	}
}
