package railway_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xevion/railway-collector/internal/railway"
)

func TestBuildBatchMetricsQuery(t *testing.T) {
	sampleRate := 30
	query, vars := railway.BuildBatchMetricsQuery(
		[]railway.MetricsBatchItem{
			{ProjectID: "abc-123", StartDate: "2024-01-01T00:00:00Z"},
			{ProjectID: "def-456", StartDate: "2024-01-01T00:05:00Z"},
		},
		[]railway.MetricMeasurement{railway.MetricMeasurementCpuUsage, railway.MetricMeasurementMemoryUsageGb},
		[]railway.MetricTag{railway.MetricTagServiceId},
		&sampleRate,
		nil,
	)

	// Should contain both aliases
	assert.Contains(t, query, "p_abc_123: metrics(")
	assert.Contains(t, query, "p_def_456: metrics(")

	// Should have operation name
	assert.True(t, strings.HasPrefix(query, "query BatchMetrics("))

	// Should contain field selections
	assert.Contains(t, query, "measurement")
	assert.Contains(t, query, "tags {")
	assert.Contains(t, query, "values {")

	// Per-alias startDate variables should be set
	assert.Equal(t, "2024-01-01T00:00:00Z", vars["startDate_p_abc_123"])
	assert.Equal(t, "2024-01-01T00:05:00Z", vars["startDate_p_def_456"])
	assert.Equal(t, 30, vars["sampleRateSeconds"])
	require.Len(t, vars["measurements"], 2)

	// Each alias should reference its own startDate variable
	assert.Contains(t, query, "startDate: $startDate_p_abc_123")
	assert.Contains(t, query, "startDate: $startDate_p_def_456")
}

func TestBuildBatchMetricsQuery_SingleProject(t *testing.T) {
	query, _ := railway.BuildBatchMetricsQuery(
		[]railway.MetricsBatchItem{
			{ProjectID: "single-id", StartDate: "2024-01-01T00:00:00Z"},
		},
		[]railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		nil,
		nil,
		nil,
	)

	assert.Contains(t, query, "p_single_id: metrics(")
	// Should not contain groupBy since it's nil
	assert.NotContains(t, query, "groupBy")
}

func TestBuildBatchMetricsQuery_PerAliasEndDate(t *testing.T) {
	end1 := "2024-01-01T06:00:00Z"
	end2 := "2024-01-01T12:00:00Z"
	query, vars := railway.BuildBatchMetricsQuery(
		[]railway.MetricsBatchItem{
			{ProjectID: "proj-1", StartDate: "2024-01-01T00:00:00Z", EndDate: &end1},
			{ProjectID: "proj-2", StartDate: "2024-01-01T06:00:00Z", EndDate: &end2},
		},
		[]railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		nil,
		nil,
		nil,
	)

	// Each alias should have its own endDate variable
	assert.Contains(t, query, "endDate: $endDate_p_proj_1")
	assert.Contains(t, query, "endDate: $endDate_p_proj_2")
	assert.Equal(t, "2024-01-01T06:00:00Z", vars["endDate_p_proj_1"])
	assert.Equal(t, "2024-01-01T12:00:00Z", vars["endDate_p_proj_2"])
}

func TestBuildBatchDeploymentLogsQuery(t *testing.T) {
	limit := 100
	start := "2024-01-01T00:00:00Z"
	query, vars := railway.BuildBatchDeploymentLogsQuery(
		[]railway.DeploymentLogRequest{
			{DeploymentID: "dep-1", IncludeBuildLogs: true, IncludeHttpLogs: true},
			{DeploymentID: "dep-2", IncludeDeploymentLogs: true},
		},
		&limit,
		&start,
		nil,
	)

	// dep-1 should have build and http aliases
	assert.Contains(t, query, "p_dep_1_build: buildLogs(")
	assert.Contains(t, query, "p_dep_1_http: httpLogs(")
	// dep-1 should NOT have deployment logs
	assert.NotContains(t, query, "p_dep_1_deploy:")

	// dep-2 should have deployment logs
	assert.Contains(t, query, "p_dep_2_deploy: deploymentLogs(")

	assert.Equal(t, 100, vars["limit"])
	assert.Equal(t, "2024-01-01T00:00:00Z", vars["startDate"])
}

func TestBuildBatchEnvironmentLogsQuery(t *testing.T) {
	afterDate := "2024-01-01T00:00:00Z"
	afterLimit := 500
	query, vars := railway.BuildBatchEnvironmentLogsQuery(
		[]string{"env-aaa", "env-bbb"},
		&afterDate,
		nil,
		&afterLimit,
		nil,
	)

	assert.Contains(t, query, "p_env_aaa: environmentLogs(")
	assert.Contains(t, query, "p_env_bbb: environmentLogs(")
	assert.Equal(t, "2024-01-01T00:00:00Z", vars["afterDate"])
	assert.Equal(t, 500, vars["afterLimit"])
}
