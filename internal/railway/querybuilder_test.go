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
		[]string{"abc-123", "def-456"},
		[]railway.MetricMeasurement{railway.MetricMeasurementCpuUsage, railway.MetricMeasurementMemoryUsageGb},
		[]railway.MetricTag{railway.MetricTagServiceId},
		"2024-01-01T00:00:00Z",
		nil,
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

	// Variables should be set
	assert.Equal(t, "2024-01-01T00:00:00Z", vars["startDate"])
	assert.Equal(t, 30, vars["sampleRateSeconds"])
	require.Len(t, vars["measurements"], 2)
}

func TestBuildBatchMetricsQuery_SingleProject(t *testing.T) {
	query, _ := railway.BuildBatchMetricsQuery(
		[]string{"single-id"},
		[]railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		nil,
		"2024-01-01T00:00:00Z",
		nil,
		nil,
		nil,
	)

	assert.Contains(t, query, "p_single_id: metrics(")
	// Should not contain groupBy since it's nil
	assert.NotContains(t, query, "groupBy")
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

func TestSanitizeAlias_RoundTrip(t *testing.T) {
	// Standard UUID
	id := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	alias := "p_" + strings.ReplaceAll(id, "-", "_")
	recovered := railway.AliasToID(alias)
	assert.Equal(t, id, recovered)
}

func TestAliasToID_NonUUID(t *testing.T) {
	// Non-UUID ID (not 32 hex chars) falls back to simple replacement
	recovered := railway.AliasToID("p_short_id")
	assert.Equal(t, "short-id", recovered)
}
