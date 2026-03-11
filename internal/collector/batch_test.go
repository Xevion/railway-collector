package collector_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/loader"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/railway"
)

func TestFragmentFromWorkItem_Metrics(t *testing.T) {
	item := types.WorkItem{
		ID: "metrics:proj-a", Kind: types.QueryMetrics,
		TaskType: types.TaskTypeMetrics, AliasKey: "proj-a",
		Params: map[string]any{
			"startDate":              "2025-01-01T00:00:00Z",
			"endDate":                "2025-01-01T06:00:00Z",
			"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
			"sampleRateSeconds":      30,
			"averagingWindowSeconds": 60,
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())
	alias := railway.SanitizeAlias("proj-a")

	assert.Equal(t, alias, f.Alias)
	assert.Equal(t, "metrics", f.Field)
	assert.Equal(t, loader.BreadthMetrics, f.Breadth)
	assert.Contains(t, f.Args, `projectId: "proj-a"`)
	assert.Contains(t, f.Args, "startDate: $startDate_"+alias)
	assert.Contains(t, f.Args, "endDate: $endDate_"+alias)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["startDate_"+alias])
	assert.Equal(t, "2025-01-01T06:00:00Z", f.VarValues["endDate_"+alias])
}

func TestFragmentFromWorkItem_MetricsNoEndDate(t *testing.T) {
	item := types.WorkItem{
		ID: "metrics:proj-a", Kind: types.QueryMetrics,
		TaskType: types.TaskTypeMetrics, AliasKey: "proj-a",
		Params: map[string]any{
			"startDate":    "2025-01-01T00:00:00Z",
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())
	alias := railway.SanitizeAlias("proj-a")

	_, hasEndDate := f.VarValues["endDate_"+alias]
	assert.False(t, hasEndDate, "realtime items should not have endDate")
}

func TestFragmentFromWorkItem_EnvironmentLogs(t *testing.T) {
	item := types.WorkItem{
		ID: "envlogs:env-a", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-a",
		Params: map[string]any{
			"afterDate":  "2025-01-01T00:00:00Z",
			"beforeDate": "2025-01-01T06:00:00Z",
			"afterLimit": 500,
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())
	alias := railway.SanitizeAlias("env-a")

	assert.Equal(t, alias, f.Alias)
	assert.Equal(t, "environmentLogs", f.Field)
	assert.Equal(t, loader.BreadthEnvironmentLogs, f.Breadth)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["afterDate_"+alias])
	assert.Equal(t, "2025-01-01T06:00:00Z", f.VarValues["beforeDate_"+alias])
	assert.Equal(t, 500, f.VarValues["afterLimit_"+alias])
}

func TestFragmentFromWorkItem_BuildLogs(t *testing.T) {
	item := types.WorkItem{
		ID: "buildlogs:dep-a", Kind: types.QueryBuildLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-a",
		Params: map[string]any{
			"limit":     500,
			"startDate": "2025-01-01T00:00:00Z",
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())
	depAlias := railway.SanitizeAlias("dep-a") + "_build"

	assert.Equal(t, depAlias, f.Alias)
	assert.Equal(t, "buildLogs", f.Field)
	assert.Equal(t, loader.BreadthBuildLogs, f.Breadth)
}

func TestFragmentFromWorkItem_HttpLogs(t *testing.T) {
	item := types.WorkItem{
		ID: "httplogs:dep-a", Kind: types.QueryHttpLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "dep-a",
		Params: map[string]any{
			"limit":     500,
			"startDate": "2025-01-01T00:00:00Z",
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())
	depAlias := railway.SanitizeAlias("dep-a") + "_http"

	assert.Equal(t, depAlias, f.Alias)
	assert.Equal(t, "httpLogs", f.Field)
	assert.Equal(t, loader.BreadthHttpLogs, f.Breadth)
}

func TestPack_FitsWithinBreadthBudget(t *testing.T) {
	// 15 metrics fragments × 12 breadth = 180, fits in one request (under 20 alias cap)
	var fragments []loader.AliasFragment
	for i := 0; i < 15; i++ {
		fragments = append(fragments, loader.AliasFragment{
			Alias:   fmt.Sprintf("alias_%d", i),
			Breadth: loader.BreadthMetrics, // 12
		})
	}

	requests := loader.Pack(fragments)
	require.Len(t, requests, 1)
	assert.Equal(t, 180, requests[0].Breadth)
	assert.Len(t, requests[0].Fragments, 15)
}

func TestPack_SplitsAtAliasLimit(t *testing.T) {
	// 25 metrics fragments: first 20 in request 1, remaining 5 in request 2
	var fragments []loader.AliasFragment
	for i := 0; i < 25; i++ {
		fragments = append(fragments, loader.AliasFragment{
			Alias:   fmt.Sprintf("alias_%d", i),
			Breadth: loader.BreadthMetrics,
		})
	}

	requests := loader.Pack(fragments)
	require.Len(t, requests, 2)
	assert.Len(t, requests[0].Fragments, loader.MaxAliasesPerRequest)
	assert.Len(t, requests[1].Fragments, 5)
}

func TestPack_SplitsAtBreadthLimit(t *testing.T) {
	// 5 fragments with breadth 120 each = 600 > 500, splits at 4 (480)
	var fragments []loader.AliasFragment
	for i := 0; i < 5; i++ {
		fragments = append(fragments, loader.AliasFragment{
			Alias:   fmt.Sprintf("alias_%d", i),
			Breadth: 120,
		})
	}

	requests := loader.Pack(fragments)
	require.Len(t, requests, 2)
	assert.Equal(t, 480, requests[0].Breadth)
	assert.Equal(t, 120, requests[1].Breadth)
}

func TestPack_MixedTypes(t *testing.T) {
	// Mix metrics (12) and env logs (13): both fit in one request
	fragments := []loader.AliasFragment{
		{Alias: "m1", Breadth: loader.BreadthMetrics},
		{Alias: "l1", Breadth: loader.BreadthEnvironmentLogs},
	}

	requests := loader.Pack(fragments)
	require.Len(t, requests, 1)
	assert.Equal(t, 25, requests[0].Breadth)
}

func TestPack_Empty(t *testing.T) {
	requests := loader.Pack(nil)
	assert.Empty(t, requests)
}

func TestAssembleQuery_ProducesValidGraphQL(t *testing.T) {
	item1 := types.WorkItem{
		ID: "m1", Kind: types.QueryMetrics,
		TaskType: types.TaskTypeMetrics, AliasKey: "proj-a",
		Params: map[string]any{
			"startDate":    "2025-01-01T00:00:00Z",
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		},
	}
	item2 := types.WorkItem{
		ID: "l1", Kind: types.QueryEnvironmentLogs,
		TaskType: types.TaskTypeLogs, AliasKey: "env-a",
		Params: map[string]any{
			"afterDate":  "2025-01-01T00:00:00Z",
			"afterLimit": 500,
		},
	}

	f1 := collector.FragmentFromWorkItem(item1, time.Now())
	f2 := collector.FragmentFromWorkItem(item2, time.Now())
	req := loader.Request{Fragments: []loader.AliasFragment{f1, f2}, Breadth: f1.Breadth + f2.Breadth}

	query, vars := req.AssembleQuery()

	assert.Contains(t, query, "query Batch(")
	assert.Contains(t, query, "p_proj_a: metrics(")
	assert.Contains(t, query, "p_env_a: environmentLogs(")

	// Variables are fully namespaced per alias
	assert.Equal(t, "2025-01-01T00:00:00Z", vars["startDate_p_proj_a"])
	assert.Equal(t, "2025-01-01T00:00:00Z", vars["afterDate_p_env_a"])
	assert.Equal(t, 500, vars["afterLimit_p_env_a"])
}

func TestSortByPriority(t *testing.T) {
	fragments := []loader.AliasFragment{
		{Alias: "l1", Item: types.WorkItem{TaskType: types.TaskTypeLogs}},
		{Alias: "m1", Item: types.WorkItem{TaskType: types.TaskTypeMetrics}},
		{Alias: "d1", Item: types.WorkItem{TaskType: types.TaskTypeDiscovery}},
	}

	loader.SortByPriority(fragments)

	assert.Equal(t, types.TaskTypeMetrics, fragments[0].Item.TaskType)
	assert.Equal(t, types.TaskTypeLogs, fragments[1].Item.TaskType)
	assert.Equal(t, types.TaskTypeDiscovery, fragments[2].Item.TaskType)
}

// fakeGenerator implements TaskGenerator for testing result dispatch.
type fakeGenerator struct {
	deliveries []fakeDelivery
	taskType   types.TaskType
}

type fakeDelivery struct {
	item types.WorkItem
	data json.RawMessage
	err  error
}

func (g *fakeGenerator) Poll(_ time.Time) []types.WorkItem { return nil }
func (g *fakeGenerator) Type() types.TaskType              { return g.taskType }
func (g *fakeGenerator) NextPoll() time.Time               { return time.Time{} }
func (g *fakeGenerator) Deliver(_ context.Context, item types.WorkItem, data json.RawMessage, err error) {
	g.deliveries = append(g.deliveries, fakeDelivery{item: item, data: data, err: err})
}

func TestDispatchRequestResults_Success(t *testing.T) {
	gen := &fakeGenerator{taskType: types.TaskTypeMetrics}
	alias := railway.SanitizeAlias("proj-a")

	item := types.WorkItem{ID: "m1", Kind: types.QueryMetrics, AliasKey: "proj-a"}
	req := loader.Request{
		Fragments: []loader.AliasFragment{
			{Alias: alias, Item: item},
		},
	}

	resp := &railway.RawQueryResponse{
		Data: map[string]json.RawMessage{
			alias: json.RawMessage(`[{"measurement":"CPU_USAGE"}]`),
		},
	}

	generatorMap := map[string]types.TaskGenerator{"m1": gen}
	loader.DispatchRequestResults(context.Background(), req, resp, nil, generatorMap)

	require.Len(t, gen.deliveries, 1)
	assert.Nil(t, gen.deliveries[0].err)
	assert.Contains(t, string(gen.deliveries[0].data), "CPU_USAGE")
}

func TestDispatchRequestResults_QueryError(t *testing.T) {
	gen := &fakeGenerator{taskType: types.TaskTypeMetrics}
	item1 := types.WorkItem{ID: "m1", Kind: types.QueryMetrics, AliasKey: "proj-a"}
	item2 := types.WorkItem{ID: "m2", Kind: types.QueryMetrics, AliasKey: "proj-b"}

	req := loader.Request{
		Fragments: []loader.AliasFragment{
			{Alias: "p_proj_a", Item: item1},
			{Alias: "p_proj_b", Item: item2},
		},
	}

	generatorMap := map[string]types.TaskGenerator{"m1": gen, "m2": gen}
	loader.DispatchRequestResults(context.Background(), req, nil, assert.AnError, generatorMap)

	require.Len(t, gen.deliveries, 2)
	assert.ErrorIs(t, gen.deliveries[0].err, assert.AnError)
	assert.ErrorIs(t, gen.deliveries[1].err, assert.AnError)
}

func TestDispatchRequestResults_BuildLogsSuffix(t *testing.T) {
	gen := &fakeGenerator{taskType: types.TaskTypeLogs}
	depAlias := railway.SanitizeAlias("dep-123") + "_build"

	item := types.WorkItem{ID: "b1", Kind: types.QueryBuildLogs, AliasKey: "dep-123"}
	req := loader.Request{
		Fragments: []loader.AliasFragment{
			{Alias: depAlias, Item: item},
		},
	}

	resp := &railway.RawQueryResponse{
		Data: map[string]json.RawMessage{
			depAlias: json.RawMessage(`[{"timestamp":"2025-01-01T00:00:00Z"}]`),
		},
	}

	generatorMap := map[string]types.TaskGenerator{"b1": gen}
	loader.DispatchRequestResults(context.Background(), req, resp, nil, generatorMap)

	require.Len(t, gen.deliveries, 1)
	assert.Nil(t, gen.deliveries[0].err)
}

func TestFragmentFromWorkItem_ServiceMetrics(t *testing.T) {
	item := types.WorkItem{
		ID: "svc-metrics:svc-1:env-1", Kind: types.QueryServiceMetrics,
		TaskType: types.TaskTypeMetrics, AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"serviceId":              "svc-1",
			"environmentId":          "env-1",
			"startDate":              "2025-01-01T00:00:00Z",
			"endDate":                "2025-01-01T06:00:00Z",
			"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
			"sampleRateSeconds":      30,
			"averagingWindowSeconds": 60,
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())

	assert.True(t, strings.HasSuffix(f.Alias, "_svcmetrics"), "alias should end with _svcmetrics, got %s", f.Alias)
	assert.Equal(t, "metrics", f.Field)
	assert.Equal(t, loader.BreadthMetrics, f.Breadth)

	// serviceId and environmentId are literals (not variables)
	assert.Contains(t, f.Args, `serviceId: "svc-1"`)
	assert.Contains(t, f.Args, `environmentId: "env-1"`)

	// Variable namespacing: all vars should use the full alias suffix
	assert.Contains(t, f.Args, "startDate: $startDate_"+f.Alias)
	assert.Contains(t, f.Args, "endDate: $endDate_"+f.Alias)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["startDate_"+f.Alias])
	assert.Equal(t, "2025-01-01T06:00:00Z", f.VarValues["endDate_"+f.Alias])
}

func TestFragmentFromWorkItem_ReplicaMetrics(t *testing.T) {
	item := types.WorkItem{
		ID: "replica-metrics:svc-1:env-1", Kind: types.QueryReplicaMetrics,
		TaskType: types.TaskTypeMetrics, AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"serviceId":              "svc-1",
			"environmentId":          "env-1",
			"startDate":              "2025-01-01T00:00:00Z",
			"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"sampleRateSeconds":      30,
			"averagingWindowSeconds": 60,
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())

	assert.True(t, strings.HasSuffix(f.Alias, "_replica"), "alias should end with _replica, got %s", f.Alias)
	assert.Equal(t, "replicaMetrics", f.Field)
	assert.Equal(t, loader.BreadthReplicaMetrics, f.Breadth)

	assert.Contains(t, f.Args, `serviceId: "svc-1"`)
	assert.Contains(t, f.Args, `environmentId: "env-1"`)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["startDate_"+f.Alias])
}

func TestFragmentFromWorkItem_HttpDurationMetrics(t *testing.T) {
	item := types.WorkItem{
		ID: "http-duration:svc-1:env-1", Kind: types.QueryHttpDurationMetrics,
		TaskType: types.TaskTypeMetrics, AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"serviceId":     "svc-1",
			"environmentId": "env-1",
			"startDate":     "2025-01-01T00:00:00Z",
			"endDate":       "2025-01-01T06:00:00Z",
			"stepSeconds":   60,
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())

	assert.True(t, strings.HasSuffix(f.Alias, "_httpdur"), "alias should end with _httpdur, got %s", f.Alias)
	assert.Equal(t, "httpDurationMetrics", f.Field)
	assert.Equal(t, loader.BreadthHttpDurationMetrics, f.Breadth)

	assert.Contains(t, f.Args, `serviceId: "svc-1"`)
	assert.Contains(t, f.Args, `environmentId: "env-1"`)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["startDate_"+f.Alias])
	assert.Equal(t, "2025-01-01T06:00:00Z", f.VarValues["endDate_"+f.Alias])
	assert.Equal(t, 60, f.VarValues["stepSeconds_"+f.Alias])
}

func TestFragmentFromWorkItem_HttpMetricsGroupedByStatus(t *testing.T) {
	item := types.WorkItem{
		ID: "http-status:svc-1:env-1", Kind: types.QueryHttpMetricsGroupedByStatus,
		TaskType: types.TaskTypeMetrics, AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"serviceId":     "svc-1",
			"environmentId": "env-1",
			"startDate":     "2025-01-01T00:00:00Z",
			"endDate":       "2025-01-01T06:00:00Z",
			"stepSeconds":   60,
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())

	assert.True(t, strings.HasSuffix(f.Alias, "_httpstatus"), "alias should end with _httpstatus, got %s", f.Alias)
	assert.Equal(t, "httpMetricsGroupedByStatus", f.Field)
	assert.Equal(t, loader.BreadthHttpMetricsGroupedByStatus, f.Breadth)

	assert.Contains(t, f.Args, `serviceId: "svc-1"`)
	assert.Contains(t, f.Args, `environmentId: "env-1"`)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["startDate_"+f.Alias])
	assert.Equal(t, "2025-01-01T06:00:00Z", f.VarValues["endDate_"+f.Alias])
	assert.Equal(t, 60, f.VarValues["stepSeconds_"+f.Alias])
}

func TestFragmentFromWorkItem_Usage(t *testing.T) {
	item := types.WorkItem{
		ID: "usage:proj-1", Kind: types.QueryUsage,
		TaskType: types.TaskTypeUsage, AliasKey: "proj-1",
		Params: map[string]any{
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"groupBy":      []railway.MetricTag{railway.MetricTagProjectId},
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())

	assert.True(t, strings.HasSuffix(f.Alias, "_usage"), "alias should end with _usage, got %s", f.Alias)
	assert.Equal(t, "usage", f.Field)
	assert.Equal(t, loader.BreadthUsage, f.Breadth)

	assert.Contains(t, f.Args, `projectId: "proj-1"`)
}

func TestFragmentFromWorkItem_EstimatedUsage(t *testing.T) {
	item := types.WorkItem{
		ID: "estimated-usage:proj-1", Kind: types.QueryEstimatedUsage,
		TaskType: types.TaskTypeUsage, AliasKey: "proj-1",
		Params: map[string]any{
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		},
	}

	f := collector.FragmentFromWorkItem(item, time.Now())

	assert.True(t, strings.HasSuffix(f.Alias, "_estusage"), "alias should end with _estusage, got %s", f.Alias)
	assert.Equal(t, "estimatedUsage", f.Field)
	assert.Equal(t, loader.BreadthEstimatedUsage, f.Breadth)

	assert.Contains(t, f.Args, `projectId: "proj-1"`)
}
