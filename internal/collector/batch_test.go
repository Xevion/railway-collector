package collector_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/railway"
)

func TestGroupByCompatibility_GroupsByKindAndBatchKey(t *testing.T) {
	items := []collector.WorkItem{
		{ID: "m1", Kind: collector.QueryMetrics, TaskType: collector.TaskTypeMetrics, AliasKey: "proj-a", BatchKey: "sr=30"},
		{ID: "m2", Kind: collector.QueryMetrics, TaskType: collector.TaskTypeMetrics, AliasKey: "proj-b", BatchKey: "sr=30"},
		{ID: "l1", Kind: collector.QueryEnvironmentLogs, TaskType: collector.TaskTypeLogs, AliasKey: "env-a", BatchKey: "limit=100"},
		{ID: "l2", Kind: collector.QueryBuildLogs, TaskType: collector.TaskTypeLogs, AliasKey: "dep-a", BatchKey: "limit=100"},
	}

	batches := collector.GroupByCompatibility(items)

	// Should produce 3 batches: metrics (2 items), env logs (1), build logs (1)
	require.Len(t, batches, 3)

	// Sorted by priority: Metrics first, then Logs batches
	assert.Equal(t, collector.QueryMetrics, batches[0].Kind)
	assert.Len(t, batches[0].Items, 2)
	assert.Equal(t, collector.TaskTypeMetrics, batches[0].TaskType)

	// The two logs batches (different Kind, same TaskType)
	assert.Equal(t, collector.TaskTypeLogs, batches[1].TaskType)
	assert.Equal(t, collector.TaskTypeLogs, batches[2].TaskType)
}

func TestGroupByCompatibility_SortsHighPriorityFirst(t *testing.T) {
	items := []collector.WorkItem{
		{ID: "b1", Kind: collector.QueryMetrics, TaskType: collector.TaskTypeBackfill, AliasKey: "proj-a", BatchKey: "bf"},
		{ID: "d1", Kind: collector.QueryDiscovery, TaskType: collector.TaskTypeDiscovery, AliasKey: "disc", BatchKey: "disc"},
		{ID: "m1", Kind: collector.QueryMetrics, TaskType: collector.TaskTypeMetrics, AliasKey: "proj-b", BatchKey: "sr=30"},
		{ID: "l1", Kind: collector.QueryEnvironmentLogs, TaskType: collector.TaskTypeLogs, AliasKey: "env-a", BatchKey: "limit=100"},
	}

	batches := collector.GroupByCompatibility(items)
	require.Len(t, batches, 4)

	// Priority order: Metrics(0) < Logs(1) < Discovery(2) < Backfill(3)
	assert.Equal(t, collector.TaskTypeMetrics, batches[0].TaskType)
	assert.Equal(t, collector.TaskTypeLogs, batches[1].TaskType)
	assert.Equal(t, collector.TaskTypeDiscovery, batches[2].TaskType)
	assert.Equal(t, collector.TaskTypeBackfill, batches[3].TaskType)
}

func TestGroupByCompatibility_Empty(t *testing.T) {
	batches := collector.GroupByCompatibility(nil)
	assert.Empty(t, batches)
}

func TestBuildQueryFromBatch_Metrics(t *testing.T) {
	batch := collector.Batch{
		Kind:     collector.QueryMetrics,
		TaskType: collector.TaskTypeMetrics,
		Items: []collector.WorkItem{
			{
				AliasKey: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				Params: map[string]any{
					"startDate":              "2025-01-01T00:00:00Z",
					"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
					"sampleRateSeconds":      30,
					"averagingWindowSeconds": 60,
				},
			},
			{
				AliasKey: "11111111-2222-3333-4444-555555555555",
				Params: map[string]any{
					"startDate":              "2025-01-02T00:00:00Z",
					"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
					"sampleRateSeconds":      30,
					"averagingWindowSeconds": 60,
				},
			},
		},
	}

	opName, query, vars, err := collector.BuildQueryFromBatch(batch)
	require.NoError(t, err)

	assert.Equal(t, "BatchMetrics", opName)
	assert.Contains(t, query, "query BatchMetrics(")
	assert.Contains(t, query, "p_aaaaaaaa_bbbb_cccc_dddd_eeeeeeeeeeee: metrics(")
	assert.Contains(t, query, "p_11111111_2222_3333_4444_555555555555: metrics(")

	// Should use earliest start date
	assert.Equal(t, "2025-01-01T00:00:00Z", vars["startDate"])
}

func TestBuildQueryFromBatch_MetricsWithEndDate(t *testing.T) {
	batch := collector.Batch{
		Kind:     collector.QueryMetrics,
		TaskType: collector.TaskTypeBackfill,
		Items: []collector.WorkItem{
			{
				AliasKey: "proj-a",
				Params: map[string]any{
					"startDate":              "2025-01-01T00:00:00Z",
					"endDate":                "2025-01-01T06:00:00Z",
					"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
					"sampleRateSeconds":      30,
					"averagingWindowSeconds": 60,
				},
			},
			{
				AliasKey: "proj-b",
				Params: map[string]any{
					"startDate":              "2025-01-01T00:00:00Z",
					"endDate":                "2025-01-01T06:00:00Z",
					"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
					"sampleRateSeconds":      30,
					"averagingWindowSeconds": 60,
				},
			},
		},
	}

	_, _, vars, err := collector.BuildQueryFromBatch(batch)
	require.NoError(t, err)

	// Should pass through endDate
	assert.Equal(t, "2025-01-01T06:00:00Z", vars["endDate"])
}

func TestBuildQueryFromBatch_MetricsWithoutEndDate(t *testing.T) {
	batch := collector.Batch{
		Kind:     collector.QueryMetrics,
		TaskType: collector.TaskTypeMetrics,
		Items: []collector.WorkItem{
			{
				AliasKey: "proj-a",
				Params: map[string]any{
					"startDate":              "2025-01-01T00:00:00Z",
					"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
					"sampleRateSeconds":      30,
					"averagingWindowSeconds": 60,
				},
			},
		},
	}

	_, _, vars, err := collector.BuildQueryFromBatch(batch)
	require.NoError(t, err)

	// Realtime items don't set endDate -- should not appear in vars
	_, hasEndDate := vars["endDate"]
	assert.False(t, hasEndDate, "realtime items should not have endDate in vars")
}

func TestBuildQueryFromBatch_EnvironmentLogsWithBeforeDate(t *testing.T) {
	batch := collector.Batch{
		Kind:     collector.QueryEnvironmentLogs,
		TaskType: collector.TaskTypeBackfill,
		Items: []collector.WorkItem{
			{
				AliasKey: "env-a",
				Params:   map[string]any{"afterDate": "2025-01-01T00:00:00Z", "beforeDate": "2025-01-01T06:00:00Z", "afterLimit": 5000},
			},
		},
	}

	_, _, vars, err := collector.BuildQueryFromBatch(batch)
	require.NoError(t, err)

	assert.Equal(t, "2025-01-01T06:00:00Z", vars["beforeDate"])
}

func TestBuildQueryFromBatch_EnvironmentLogs(t *testing.T) {
	batch := collector.Batch{
		Kind:     collector.QueryEnvironmentLogs,
		TaskType: collector.TaskTypeLogs,
		Items: []collector.WorkItem{
			{
				AliasKey: "env-aaa",
				Params:   map[string]any{"afterDate": "2025-01-02T00:00:00Z", "afterLimit": 500},
			},
			{
				AliasKey: "env-bbb",
				Params:   map[string]any{"afterDate": "2025-01-01T00:00:00Z", "afterLimit": 500},
			},
		},
	}

	opName, query, vars, err := collector.BuildQueryFromBatch(batch)
	require.NoError(t, err)

	assert.Equal(t, "BatchEnvironmentLogs", opName)
	assert.Contains(t, query, "query BatchEnvironmentLogs(")
	assert.Contains(t, query, "p_env_aaa: environmentLogs(")
	assert.Contains(t, query, "p_env_bbb: environmentLogs(")
	assert.Equal(t, "2025-01-01T00:00:00Z", vars["afterDate"])
}

func TestBuildQueryFromBatch_Discovery(t *testing.T) {
	batch := collector.Batch{
		Kind:     collector.QueryDiscovery,
		TaskType: collector.TaskTypeDiscovery,
		Items:    []collector.WorkItem{{ID: "discovery"}},
	}

	opName, query, vars, err := collector.BuildQueryFromBatch(batch)
	require.NoError(t, err)

	assert.Equal(t, "Discovery", opName)
	assert.Empty(t, query)
	assert.Nil(t, vars)
}

// fakeGenerator implements TaskGenerator for testing result dispatch.
type fakeGenerator struct {
	deliveries []fakeDelivery
	taskType   collector.TaskType
}

type fakeDelivery struct {
	item collector.WorkItem
	data json.RawMessage
	err  error
}

func (g *fakeGenerator) Poll(_ time.Time) []collector.WorkItem { return nil }
func (g *fakeGenerator) Type() collector.TaskType              { return g.taskType }
func (g *fakeGenerator) NextPoll() time.Time                   { return time.Time{} }
func (g *fakeGenerator) Deliver(_ context.Context, item collector.WorkItem, data json.RawMessage, err error) {
	g.deliveries = append(g.deliveries, fakeDelivery{item: item, data: data, err: err})
}

func TestDispatchResults_Success(t *testing.T) {
	gen := &fakeGenerator{taskType: collector.TaskTypeMetrics}
	items := []collector.WorkItem{
		{ID: "m1", Kind: collector.QueryMetrics, AliasKey: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
	}
	batch := collector.Batch{Kind: collector.QueryMetrics, Items: items}

	alias := railway.SanitizeAlias("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
	resp := &railway.RawQueryResponse{
		Data: map[string]json.RawMessage{
			alias: json.RawMessage(`[{"measurement":"CPU_USAGE"}]`),
		},
	}

	generatorMap := map[string]collector.TaskGenerator{"m1": gen}
	collector.DispatchResults(context.Background(), batch, resp, nil, generatorMap)

	require.Len(t, gen.deliveries, 1)
	assert.Nil(t, gen.deliveries[0].err)
	assert.Contains(t, string(gen.deliveries[0].data), "CPU_USAGE")
}

func TestDispatchResults_QueryError(t *testing.T) {
	gen := &fakeGenerator{taskType: collector.TaskTypeMetrics}
	items := []collector.WorkItem{
		{ID: "m1", Kind: collector.QueryMetrics, AliasKey: "proj-a"},
		{ID: "m2", Kind: collector.QueryMetrics, AliasKey: "proj-b"},
	}
	batch := collector.Batch{Kind: collector.QueryMetrics, Items: items}

	generatorMap := map[string]collector.TaskGenerator{"m1": gen, "m2": gen}
	collector.DispatchResults(context.Background(), batch, nil, assert.AnError, generatorMap)

	// Both items should get the error
	require.Len(t, gen.deliveries, 2)
	assert.ErrorIs(t, gen.deliveries[0].err, assert.AnError)
	assert.ErrorIs(t, gen.deliveries[1].err, assert.AnError)
}

func TestDispatchResults_PartialGraphQLError(t *testing.T) {
	gen := &fakeGenerator{taskType: collector.TaskTypeMetrics}
	alias1 := railway.SanitizeAlias("proj-a")
	alias2 := railway.SanitizeAlias("proj-b")

	items := []collector.WorkItem{
		{ID: "m1", Kind: collector.QueryMetrics, AliasKey: "proj-a"},
		{ID: "m2", Kind: collector.QueryMetrics, AliasKey: "proj-b"},
	}
	batch := collector.Batch{Kind: collector.QueryMetrics, Items: items}

	resp := &railway.RawQueryResponse{
		Data: map[string]json.RawMessage{
			alias1: json.RawMessage(`[{"measurement":"CPU_USAGE"}]`),
			alias2: json.RawMessage(`null`),
		},
		Errors: []railway.GraphQLError{
			{Message: "project not found", Path: []string{alias2}},
		},
	}

	generatorMap := map[string]collector.TaskGenerator{"m1": gen, "m2": gen}
	collector.DispatchResults(context.Background(), batch, resp, nil, generatorMap)

	require.Len(t, gen.deliveries, 2)
	// First alias succeeded
	assert.Nil(t, gen.deliveries[0].err)
	assert.Contains(t, string(gen.deliveries[0].data), "CPU_USAGE")
	// Second alias got the GraphQL error
	assert.Error(t, gen.deliveries[1].err)
	assert.Contains(t, gen.deliveries[1].err.Error(), "project not found")
}

func TestDispatchResults_BuildLogsSuffix(t *testing.T) {
	gen := &fakeGenerator{taskType: collector.TaskTypeLogs}
	depAlias := railway.SanitizeAlias("dep-123")

	items := []collector.WorkItem{
		{ID: "b1", Kind: collector.QueryBuildLogs, AliasKey: "dep-123"},
	}
	batch := collector.Batch{Kind: collector.QueryBuildLogs, Items: items}

	resp := &railway.RawQueryResponse{
		Data: map[string]json.RawMessage{
			depAlias + "_build": json.RawMessage(`[{"timestamp":"2025-01-01T00:00:00Z"}]`),
		},
	}

	generatorMap := map[string]collector.TaskGenerator{"b1": gen}
	collector.DispatchResults(context.Background(), batch, resp, nil, generatorMap)

	require.Len(t, gen.deliveries, 1)
	assert.Nil(t, gen.deliveries[0].err)
}

func TestOperationNameForBatch(t *testing.T) {
	tests := []struct {
		kind     collector.QueryKind
		expected string
	}{
		{collector.QueryMetrics, "BatchMetrics"},
		{collector.QueryEnvironmentLogs, "BatchEnvironmentLogs"},
		{collector.QueryBuildLogs, "BatchDeploymentLogs"},
		{collector.QueryHttpLogs, "BatchDeploymentLogs"},
		{collector.QueryDiscovery, "Discovery"},
	}

	for _, tt := range tests {
		t.Run(string(tt.kind), func(t *testing.T) {
			assert.Equal(t, tt.expected, collector.OperationNameForBatch(tt.kind))
		})
	}
}
