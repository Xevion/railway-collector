package collector_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/railway"
)

func TestFragmentFromWorkItem_Metrics(t *testing.T) {
	item := collector.WorkItem{
		ID: "metrics:proj-a", Kind: collector.QueryMetrics,
		TaskType: collector.TaskTypeMetrics, AliasKey: "proj-a",
		Params: map[string]any{
			"startDate":              "2025-01-01T00:00:00Z",
			"endDate":                "2025-01-01T06:00:00Z",
			"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"groupBy":                []railway.MetricTag{railway.MetricTagServiceId},
			"sampleRateSeconds":      30,
			"averagingWindowSeconds": 60,
		},
	}

	f := collector.FragmentFromWorkItem(item)
	alias := railway.SanitizeAlias("proj-a")

	assert.Equal(t, alias, f.Alias)
	assert.Equal(t, "metrics", f.Field)
	assert.Equal(t, collector.BreadthMetrics, f.Breadth)
	assert.Contains(t, f.Args, `projectId: "proj-a"`)
	assert.Contains(t, f.Args, "startDate: $startDate_"+alias)
	assert.Contains(t, f.Args, "endDate: $endDate_"+alias)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["startDate_"+alias])
	assert.Equal(t, "2025-01-01T06:00:00Z", f.VarValues["endDate_"+alias])
}

func TestFragmentFromWorkItem_MetricsNoEndDate(t *testing.T) {
	item := collector.WorkItem{
		ID: "metrics:proj-a", Kind: collector.QueryMetrics,
		TaskType: collector.TaskTypeMetrics, AliasKey: "proj-a",
		Params: map[string]any{
			"startDate":    "2025-01-01T00:00:00Z",
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		},
	}

	f := collector.FragmentFromWorkItem(item)
	alias := railway.SanitizeAlias("proj-a")

	_, hasEndDate := f.VarValues["endDate_"+alias]
	assert.False(t, hasEndDate, "realtime items should not have endDate")
}

func TestFragmentFromWorkItem_EnvironmentLogs(t *testing.T) {
	item := collector.WorkItem{
		ID: "envlogs:env-a", Kind: collector.QueryEnvironmentLogs,
		TaskType: collector.TaskTypeLogs, AliasKey: "env-a",
		Params: map[string]any{
			"afterDate":  "2025-01-01T00:00:00Z",
			"beforeDate": "2025-01-01T06:00:00Z",
			"afterLimit": 500,
		},
	}

	f := collector.FragmentFromWorkItem(item)
	alias := railway.SanitizeAlias("env-a")

	assert.Equal(t, alias, f.Alias)
	assert.Equal(t, "environmentLogs", f.Field)
	assert.Equal(t, collector.BreadthEnvironmentLogs, f.Breadth)
	assert.Equal(t, "2025-01-01T00:00:00Z", f.VarValues["afterDate_"+alias])
	assert.Equal(t, "2025-01-01T06:00:00Z", f.VarValues["beforeDate_"+alias])
	assert.Equal(t, 500, f.VarValues["afterLimit_"+alias])
}

func TestFragmentFromWorkItem_BuildLogs(t *testing.T) {
	item := collector.WorkItem{
		ID: "buildlogs:dep-a", Kind: collector.QueryBuildLogs,
		TaskType: collector.TaskTypeLogs, AliasKey: "dep-a",
		Params: map[string]any{
			"limit":     500,
			"startDate": "2025-01-01T00:00:00Z",
		},
	}

	f := collector.FragmentFromWorkItem(item)
	depAlias := railway.SanitizeAlias("dep-a") + "_build"

	assert.Equal(t, depAlias, f.Alias)
	assert.Equal(t, "buildLogs", f.Field)
	assert.Equal(t, collector.BreadthBuildLogs, f.Breadth)
}

func TestFragmentFromWorkItem_HttpLogs(t *testing.T) {
	item := collector.WorkItem{
		ID: "httplogs:dep-a", Kind: collector.QueryHttpLogs,
		TaskType: collector.TaskTypeLogs, AliasKey: "dep-a",
		Params: map[string]any{
			"limit":     500,
			"startDate": "2025-01-01T00:00:00Z",
		},
	}

	f := collector.FragmentFromWorkItem(item)
	depAlias := railway.SanitizeAlias("dep-a") + "_http"

	assert.Equal(t, depAlias, f.Alias)
	assert.Equal(t, "httpLogs", f.Field)
	assert.Equal(t, collector.BreadthHttpLogs, f.Breadth)
}

func TestPack_FitsWithinBreadthBudget(t *testing.T) {
	// 15 metrics fragments × 12 breadth = 180, fits in one request (under 20 alias cap)
	var fragments []collector.AliasFragment
	for i := 0; i < 15; i++ {
		fragments = append(fragments, collector.AliasFragment{
			Alias:   fmt.Sprintf("alias_%d", i),
			Breadth: collector.BreadthMetrics, // 12
		})
	}

	requests := collector.Pack(fragments)
	require.Len(t, requests, 1)
	assert.Equal(t, 180, requests[0].Breadth)
	assert.Len(t, requests[0].Fragments, 15)
}

func TestPack_SplitsAtAliasLimit(t *testing.T) {
	// 25 metrics fragments: first 20 in request 1, remaining 5 in request 2
	var fragments []collector.AliasFragment
	for i := 0; i < 25; i++ {
		fragments = append(fragments, collector.AliasFragment{
			Alias:   fmt.Sprintf("alias_%d", i),
			Breadth: collector.BreadthMetrics,
		})
	}

	requests := collector.Pack(fragments)
	require.Len(t, requests, 2)
	assert.Len(t, requests[0].Fragments, collector.MaxAliasesPerRequest)
	assert.Len(t, requests[1].Fragments, 5)
}

func TestPack_SplitsAtBreadthLimit(t *testing.T) {
	// 5 fragments with breadth 120 each = 600 > 500, splits at 4 (480)
	var fragments []collector.AliasFragment
	for i := 0; i < 5; i++ {
		fragments = append(fragments, collector.AliasFragment{
			Alias:   fmt.Sprintf("alias_%d", i),
			Breadth: 120,
		})
	}

	requests := collector.Pack(fragments)
	require.Len(t, requests, 2)
	assert.Equal(t, 480, requests[0].Breadth)
	assert.Equal(t, 120, requests[1].Breadth)
}

func TestPack_MixedTypes(t *testing.T) {
	// Mix metrics (12) and env logs (13): both fit in one request
	fragments := []collector.AliasFragment{
		{Alias: "m1", Breadth: collector.BreadthMetrics},
		{Alias: "l1", Breadth: collector.BreadthEnvironmentLogs},
	}

	requests := collector.Pack(fragments)
	require.Len(t, requests, 1)
	assert.Equal(t, 25, requests[0].Breadth)
}

func TestPack_Empty(t *testing.T) {
	requests := collector.Pack(nil)
	assert.Empty(t, requests)
}

func TestAssembleQuery_ProducesValidGraphQL(t *testing.T) {
	item1 := collector.WorkItem{
		ID: "m1", Kind: collector.QueryMetrics,
		TaskType: collector.TaskTypeMetrics, AliasKey: "proj-a",
		Params: map[string]any{
			"startDate":    "2025-01-01T00:00:00Z",
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		},
	}
	item2 := collector.WorkItem{
		ID: "l1", Kind: collector.QueryEnvironmentLogs,
		TaskType: collector.TaskTypeLogs, AliasKey: "env-a",
		Params: map[string]any{
			"afterDate":  "2025-01-01T00:00:00Z",
			"afterLimit": 500,
		},
	}

	f1 := collector.FragmentFromWorkItem(item1)
	f2 := collector.FragmentFromWorkItem(item2)
	req := collector.Request{Fragments: []collector.AliasFragment{f1, f2}, Breadth: f1.Breadth + f2.Breadth}

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
	fragments := []collector.AliasFragment{
		{Alias: "l1", Item: collector.WorkItem{TaskType: collector.TaskTypeLogs}},
		{Alias: "m1", Item: collector.WorkItem{TaskType: collector.TaskTypeMetrics}},
		{Alias: "d1", Item: collector.WorkItem{TaskType: collector.TaskTypeDiscovery}},
	}

	collector.SortByPriority(fragments)

	assert.Equal(t, collector.TaskTypeMetrics, fragments[0].Item.TaskType)
	assert.Equal(t, collector.TaskTypeLogs, fragments[1].Item.TaskType)
	assert.Equal(t, collector.TaskTypeDiscovery, fragments[2].Item.TaskType)
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

func TestDispatchRequestResults_Success(t *testing.T) {
	gen := &fakeGenerator{taskType: collector.TaskTypeMetrics}
	alias := railway.SanitizeAlias("proj-a")

	item := collector.WorkItem{ID: "m1", Kind: collector.QueryMetrics, AliasKey: "proj-a"}
	req := collector.Request{
		Fragments: []collector.AliasFragment{
			{Alias: alias, Item: item},
		},
	}

	resp := &railway.RawQueryResponse{
		Data: map[string]json.RawMessage{
			alias: json.RawMessage(`[{"measurement":"CPU_USAGE"}]`),
		},
	}

	generatorMap := map[string]collector.TaskGenerator{"m1": gen}
	collector.DispatchRequestResults(context.Background(), req, resp, nil, generatorMap)

	require.Len(t, gen.deliveries, 1)
	assert.Nil(t, gen.deliveries[0].err)
	assert.Contains(t, string(gen.deliveries[0].data), "CPU_USAGE")
}

func TestDispatchRequestResults_QueryError(t *testing.T) {
	gen := &fakeGenerator{taskType: collector.TaskTypeMetrics}
	item1 := collector.WorkItem{ID: "m1", Kind: collector.QueryMetrics, AliasKey: "proj-a"}
	item2 := collector.WorkItem{ID: "m2", Kind: collector.QueryMetrics, AliasKey: "proj-b"}

	req := collector.Request{
		Fragments: []collector.AliasFragment{
			{Alias: "p_proj_a", Item: item1},
			{Alias: "p_proj_b", Item: item2},
		},
	}

	generatorMap := map[string]collector.TaskGenerator{"m1": gen, "m2": gen}
	collector.DispatchRequestResults(context.Background(), req, nil, assert.AnError, generatorMap)

	require.Len(t, gen.deliveries, 2)
	assert.ErrorIs(t, gen.deliveries[0].err, assert.AnError)
	assert.ErrorIs(t, gen.deliveries[1].err, assert.AnError)
}

func TestDispatchRequestResults_BuildLogsSuffix(t *testing.T) {
	gen := &fakeGenerator{taskType: collector.TaskTypeLogs}
	depAlias := railway.SanitizeAlias("dep-123") + "_build"

	item := collector.WorkItem{ID: "b1", Kind: collector.QueryBuildLogs, AliasKey: "dep-123"}
	req := collector.Request{
		Fragments: []collector.AliasFragment{
			{Alias: depAlias, Item: item},
		},
	}

	resp := &railway.RawQueryResponse{
		Data: map[string]json.RawMessage{
			depAlias: json.RawMessage(`[{"timestamp":"2025-01-01T00:00:00Z"}]`),
		},
	}

	generatorMap := map[string]collector.TaskGenerator{"b1": gen}
	collector.DispatchRequestResults(context.Background(), req, resp, nil, generatorMap)

	require.Len(t, gen.deliveries, 1)
	assert.Nil(t, gen.deliveries[0].err)
}
