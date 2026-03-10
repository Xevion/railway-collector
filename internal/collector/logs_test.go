package collector_test

import (
	"context"
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

func TestLogsCollector_Collect_EnvironmentLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	envID := "env-1"
	cursorTime := fakeClock.Now().Add(-5 * time.Minute)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   envID,
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	// Environment log cursor
	store.EXPECT().GetLogCursor(envID, "environment").Return(cursorTime)
	// Build and HTTP log cursors
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDateStr := cursorTime.Format(time.RFC3339Nano)

	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(), envID, gomock.Nil(),
		gomock.Eq(&afterDateStr), gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{
		EnvironmentLogs: []railway.EnvironmentLogsQueryEnvironmentLogsLog{{
			Timestamp: ts,
			Message:   "test log message",
			Tags: &railway.EnvironmentLogsQueryEnvironmentLogsLogTags{
				ServiceId: strPtr("svc-1"),
			},
		}},
	}, nil)

	// Build logs (empty)
	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{}, nil)
	// HTTP logs (empty)
	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{}, nil)

	// Only env logs produced data, so only env cursor advances
	expectedTS, _ := time.Parse(time.RFC3339Nano, ts)
	store.EXPECT().SetLogCursor(envID, "environment", gomock.Eq(expectedTS)).Return(nil)

	// Coverage recording for env logs (has entries)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"deployment", "build", "http"},
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 1)
	assert.Equal(t, "test log message", collected[0].Message)
	assert.Equal(t, "deployment", collected[0].Labels["log_type"])
	assert.Equal(t, "env-1", collected[0].Labels["environment_id"])
	assert.Equal(t, "svc-1", collected[0].Labels["service_id"])
	assert.Equal(t, "test-service", collected[0].Labels["service_name"])
	assert.Equal(t, "test-project", collected[0].Labels["project_name"])
}

func TestLogsCollector_Collect_FirstRunNoCursor(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	envID := "env-1"

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   envID,
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	// No cursors exist
	store.EXPECT().GetLogCursor(envID, "environment").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	// No afterDate sent when cursor is zero
	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(), envID, gomock.Nil(),
		gomock.Nil(), gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{}, nil)

	// Build + HTTP logs (no startDate)
	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{}, nil)
	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{}, nil)

	fakeSink := &recordingSink{}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"deployment", "build", "http"},
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
}

func TestLogsCollector_Collect_RecordsCoverage(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	envID := "env-1"
	cursorTime := fakeClock.Now().Add(-5 * time.Minute)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   envID,
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	// Environment log cursor
	store.EXPECT().GetLogCursor(envID, "environment").Return(cursorTime)
	// Build and HTTP log cursors (no cursor)
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDateStr := cursorTime.Format(time.RFC3339Nano)

	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(), envID, gomock.Nil(),
		gomock.Eq(&afterDateStr), gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{
		EnvironmentLogs: []railway.EnvironmentLogsQueryEnvironmentLogsLog{{
			Timestamp: ts,
			Message:   "test log message",
			Tags: &railway.EnvironmentLogsQueryEnvironmentLogsLogTags{
				ServiceId: strPtr("svc-1"),
			},
		}},
	}, nil)

	// Build logs (empty)
	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{}, nil)
	// HTTP logs (empty)
	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{}, nil)

	// Env log cursor advances
	store.EXPECT().SetLogCursor(envID, "environment", gomock.Any()).Return(nil)

	// Expect coverage recording for environment logs (has entries, so coverage is recorded)
	envCoverageKey := collector.CoverageKey(envID, "log", "environment")
	store.EXPECT().GetCoverage(envCoverageKey).Return(nil, nil)
	store.EXPECT().SetCoverage(envCoverageKey, gomock.Any()).Return(nil)

	// Build and HTTP logs were empty (maxTS is zero), so no coverage recording for them

	fakeSink := &recordingSink{}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"deployment", "build", "http"},
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
}

func TestLogsCollector_Collect_BuildAndHttpPerDeployment(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	buildCursor := fakeClock.Now().Add(-10 * time.Minute)
	httpCursor := fakeClock.Now().Add(-3 * time.Minute)
	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   "env-1",
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	// No env log type enabled for this test
	store.EXPECT().GetLogCursor("dep-1", "build").Return(buildCursor)
	store.EXPECT().GetLogCursor("dep-1", "http").Return(httpCursor)

	buildStartStr := buildCursor.Format(time.RFC3339Nano)
	httpStartStr := httpCursor.Format(time.RFC3339Nano)

	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Eq(&buildStartStr), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{
			BuildLogs: []railway.BuildLogsQueryBuildLogsLog{{
				Timestamp: ts,
				Message:   "build log",
			}},
		}, nil)

	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Eq(&httpStartStr), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{
			HttpLogs: []railway.HttpLogsQueryHttpLogsHttpLog{{
				Timestamp:          ts,
				Method:             "GET",
				Path:               "/api",
				HttpStatus:         200,
				Host:               "example.com",
				EdgeRegion:         "us-east-1",
				TotalDuration:      50,
				UpstreamRqDuration: 40,
			}},
		}, nil)

	// Both cursors should advance
	expectedTS, _ := time.Parse(time.RFC3339Nano, ts)
	store.EXPECT().SetLogCursor("dep-1", "build", gomock.Eq(expectedTS)).Return(nil)
	store.EXPECT().SetLogCursor("dep-1", "http", gomock.Eq(expectedTS)).Return(nil)

	// Coverage recording for build and HTTP logs (both have entries)
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = append(collected, logs...)
			return nil
		},
	}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"build", "http"}, // no "deployment" type
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 2)

	// Verify both log types present
	types := map[string]bool{}
	for _, l := range collected {
		types[l.Labels["log_type"]] = true
	}
	assert.True(t, types["build"])
	assert.True(t, types["http"])

	// Content assertions for each log type
	for _, l := range collected {
		switch l.Labels["log_type"] {
		case "build":
			assert.Equal(t, "build log", l.Message)
		case "http":
			assert.Equal(t, "GET /api 200", l.Message)
			assert.Equal(t, "GET", l.Labels["method"])
			assert.Equal(t, "200", l.Labels["status"])
			assert.Equal(t, "us-east-1", l.Labels["edge_region"])
			assert.Equal(t, "50", l.Attributes["total_duration_ms"])
		}
	}
}

func TestLogsCollector_Collect_EnvironmentLogAPIFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	envID := "env-1"
	cursorTime := fakeClock.Now().Add(-5 * time.Minute)
	buildTS := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   envID,
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	// All cursors returned
	store.EXPECT().GetLogCursor(envID, "environment").Return(cursorTime)
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	// Environment logs API fails
	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(), envID, gomock.Any(),
		gomock.Any(), gomock.Any(),
		gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, assert.AnError)

	// Build logs succeed with 1 entry
	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{
			BuildLogs: []railway.BuildLogsQueryBuildLogsLog{{
				Timestamp: buildTS,
				Message:   "build started",
			}},
		}, nil)

	// HTTP logs empty
	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{}, nil)

	// Build cursor should advance; environment cursor should NOT be called
	expectedBuildTS, _ := time.Parse(time.RFC3339Nano, buildTS)
	store.EXPECT().SetLogCursor("dep-1", "build", gomock.Eq(expectedBuildTS)).Return(nil)

	// Coverage expectations
	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = append(collected, logs...)
			return nil
		},
	}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"deployment", "build", "http"},
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 1)
	assert.Equal(t, "build started", collected[0].Message)
	assert.Equal(t, "build", collected[0].Labels["log_type"])
}

func TestLogsCollector_Collect_UnparseableTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	envID := "env-1"
	cursorTime := fakeClock.Now().Add(-5 * time.Minute)
	validTS := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDateStr := cursorTime.Format(time.RFC3339Nano)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   envID,
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	store.EXPECT().GetLogCursor(envID, "environment").Return(cursorTime)
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	// Two logs: one valid, one with bad timestamp
	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(), envID, gomock.Nil(),
		gomock.Eq(&afterDateStr), gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{
		EnvironmentLogs: []railway.EnvironmentLogsQueryEnvironmentLogsLog{
			{
				Timestamp: validTS,
				Message:   "valid log",
				Tags: &railway.EnvironmentLogsQueryEnvironmentLogsLogTags{
					ServiceId: strPtr("svc-1"),
				},
			},
			{
				Timestamp: "not-a-timestamp",
				Message:   "bad log",
				Tags: &railway.EnvironmentLogsQueryEnvironmentLogsLogTags{
					ServiceId: strPtr("svc-1"),
				},
			},
		},
	}, nil)

	// Build + HTTP empty
	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{}, nil)
	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{}, nil)

	// Cursor advances to the valid timestamp
	expectedTS, _ := time.Parse(time.RFC3339Nano, validTS)
	store.EXPECT().SetLogCursor(envID, "environment", gomock.Eq(expectedTS)).Return(nil)

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"deployment", "build", "http"},
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 1)
	assert.Equal(t, "valid log", collected[0].Message)
}

func TestLogsCollector_Collect_NilTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	envID := "env-1"
	cursorTime := fakeClock.Now().Add(-5 * time.Minute)
	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDateStr := cursorTime.Format(time.RFC3339Nano)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   envID,
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	store.EXPECT().GetLogCursor(envID, "environment").Return(cursorTime)
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	// Log entry with nil Tags
	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(), envID, gomock.Nil(),
		gomock.Eq(&afterDateStr), gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{
		EnvironmentLogs: []railway.EnvironmentLogsQueryEnvironmentLogsLog{{
			Timestamp: ts,
			Message:   "no tags log",
			Tags:      nil,
		}},
	}, nil)

	// Build + HTTP empty
	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{}, nil)
	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{}, nil)

	// Cursor advances
	expectedTS, _ := time.Parse(time.RFC3339Nano, ts)
	store.EXPECT().SetLogCursor(envID, "environment", gomock.Eq(expectedTS)).Return(nil)

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"deployment", "build", "http"},
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 1)
	assert.Equal(t, "no tags log", collected[0].Message)
	assert.Equal(t, "deployment", collected[0].Labels["log_type"])
	assert.Equal(t, "env-1", collected[0].Labels["environment_id"])
	_, hasServiceID := collected[0].Labels["service_id"]
	assert.False(t, hasServiceID, "service_id should not be present when Tags is nil")
}

func TestLogsCollector_Collect_UnknownServiceInTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))

	envID := "env-1"
	cursorTime := fakeClock.Now().Add(-5 * time.Minute)
	ts := fakeClock.Now().Add(-1 * time.Minute).Format(time.RFC3339Nano)
	afterDateStr := cursorTime.Format(time.RFC3339Nano)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{{
		ProjectID:       "proj-1",
		ProjectName:     "test-project",
		ServiceID:       "svc-1",
		ServiceName:     "test-service",
		EnvironmentID:   envID,
		EnvironmentName: "production",
		DeploymentID:    "dep-1",
	}})

	store.EXPECT().GetLogCursor(envID, "environment").Return(cursorTime)
	store.EXPECT().GetLogCursor("dep-1", "build").Return(time.Time{})
	store.EXPECT().GetLogCursor("dep-1", "http").Return(time.Time{})

	// Log entry with unknown service ID
	api.EXPECT().GetEnvironmentLogs(
		gomock.Any(), envID, gomock.Nil(),
		gomock.Eq(&afterDateStr), gomock.Nil(),
		gomock.Any(), gomock.Nil(), gomock.Nil(),
	).Return(&railway.EnvironmentLogsQueryResponse{
		EnvironmentLogs: []railway.EnvironmentLogsQueryEnvironmentLogsLog{{
			Timestamp: ts,
			Message:   "unknown service log",
			Tags: &railway.EnvironmentLogsQueryEnvironmentLogsLogTags{
				ServiceId: strPtr("svc-unknown"),
			},
		}},
	}, nil)

	// Build + HTTP empty
	api.EXPECT().GetBuildLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.BuildLogsQueryResponse{}, nil)
	api.EXPECT().GetHttpLogs(gomock.Any(), "dep-1", gomock.Any(), gomock.Nil(), gomock.Nil(), gomock.Nil()).
		Return(&railway.HttpLogsQueryResponse{}, nil)

	// Cursor advances
	expectedTS, _ := time.Parse(time.RFC3339Nano, ts)
	store.EXPECT().SetLogCursor(envID, "environment", gomock.Eq(expectedTS)).Return(nil)

	store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()
	store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var collected []sink.LogEntry
	fakeSink := &recordingSink{
		writeLogs: func(_ context.Context, logs []sink.LogEntry) error {
			collected = logs
			return nil
		},
	}

	lc := collector.NewLogsCollector(
		api, targets, []sink.Sink{fakeSink},
		[]string{"deployment", "build", "http"},
		500,
		2*time.Minute,
		store,
		fakeClock,
		slog.Default(),
	)

	err := lc.Collect(context.Background())
	require.NoError(t, err)
	assert.Len(t, collected, 1)
	assert.Equal(t, "unknown service log", collected[0].Message)
	assert.Equal(t, "svc-unknown", collected[0].Labels["service_id"])
	_, hasServiceName := collected[0].Labels["service_name"]
	assert.False(t, hasServiceName, "service_name should not be present for unknown service")
}
