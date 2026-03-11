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
	alias := f.Alias

	assert.Contains(t, alias, "p_proj_a")
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
	alias := f.Alias

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
	alias := f.Alias

	assert.Contains(t, alias, "p_env_a")
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

	assert.Contains(t, f.Alias, "p_dep_a_build")
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

	assert.Contains(t, f.Alias, "p_dep_a_http")
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
	assert.Contains(t, query, f1.Alias+": metrics(")
	assert.Contains(t, query, f2.Alias+": environmentLogs(")

	// Variables are fully namespaced per alias
	assert.Equal(t, "2025-01-01T00:00:00Z", vars["startDate_"+f1.Alias])
	assert.Equal(t, "2025-01-01T00:00:00Z", vars["afterDate_"+f2.Alias])
	assert.Equal(t, 500, vars["afterLimit_"+f2.Alias])
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

func TestDispatchRequestResults_Success(t *testing.T) {
	gen := &stubGenerator{taskType: types.TaskTypeMetrics}
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

	d := gen.getDeliveries()
	require.Len(t, d, 1)
	assert.Nil(t, d[0].Err)
	assert.Contains(t, string(d[0].Data), "CPU_USAGE")
}

func TestDispatchRequestResults_QueryError(t *testing.T) {
	gen := &stubGenerator{taskType: types.TaskTypeMetrics}
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

	d := gen.getDeliveries()
	require.Len(t, d, 2)
	assert.ErrorIs(t, d[0].Err, assert.AnError)
	assert.ErrorIs(t, d[1].Err, assert.AnError)
}

func TestDispatchRequestResults_BuildLogsSuffix(t *testing.T) {
	gen := &stubGenerator{taskType: types.TaskTypeLogs}
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

	d := gen.getDeliveries()
	require.Len(t, d, 1)
	assert.Nil(t, d[0].Err)
}

func TestFragmentFromWorkItem_ByKind(t *testing.T) {
	tests := []struct {
		name          string
		item          types.WorkItem
		wantSuffix    string
		wantField     string
		wantBreadth   int
		wantArgs      []string // substrings expected in f.Args
		wantVarValues map[string]any
	}{
		{
			name: "ServiceMetrics",
			item: types.WorkItem{
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
			},
			wantSuffix:  "_svcmetrics",
			wantField:   "metrics",
			wantBreadth: loader.BreadthMetrics,
			wantArgs: []string{
				`serviceId: "svc-1"`,
				`environmentId: "env-1"`,
			},
			wantVarValues: map[string]any{
				"startDate": "2025-01-01T00:00:00Z",
				"endDate":   "2025-01-01T06:00:00Z",
			},
		},
		{
			name: "ReplicaMetrics",
			item: types.WorkItem{
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
			},
			wantSuffix:  "_replica",
			wantField:   "replicaMetrics",
			wantBreadth: loader.BreadthReplicaMetrics,
			wantArgs: []string{
				`serviceId: "svc-1"`,
				`environmentId: "env-1"`,
			},
			wantVarValues: map[string]any{
				"startDate": "2025-01-01T00:00:00Z",
			},
		},
		{
			name: "HttpDurationMetrics",
			item: types.WorkItem{
				ID: "http-duration:svc-1:env-1", Kind: types.QueryHttpDurationMetrics,
				TaskType: types.TaskTypeMetrics, AliasKey: "svc-1:env-1",
				Params: map[string]any{
					"serviceId":     "svc-1",
					"environmentId": "env-1",
					"startDate":     "2025-01-01T00:00:00Z",
					"endDate":       "2025-01-01T06:00:00Z",
					"stepSeconds":   60,
				},
			},
			wantSuffix:  "_httpdur",
			wantField:   "httpDurationMetrics",
			wantBreadth: loader.BreadthHttpDurationMetrics,
			wantArgs: []string{
				`serviceId: "svc-1"`,
				`environmentId: "env-1"`,
			},
			wantVarValues: map[string]any{
				"startDate":   "2025-01-01T00:00:00Z",
				"endDate":     "2025-01-01T06:00:00Z",
				"stepSeconds": 60,
			},
		},
		{
			name: "HttpMetricsGroupedByStatus",
			item: types.WorkItem{
				ID: "http-status:svc-1:env-1", Kind: types.QueryHttpMetricsGroupedByStatus,
				TaskType: types.TaskTypeMetrics, AliasKey: "svc-1:env-1",
				Params: map[string]any{
					"serviceId":     "svc-1",
					"environmentId": "env-1",
					"startDate":     "2025-01-01T00:00:00Z",
					"endDate":       "2025-01-01T06:00:00Z",
					"stepSeconds":   60,
				},
			},
			wantSuffix:  "_httpstatus",
			wantField:   "httpMetricsGroupedByStatus",
			wantBreadth: loader.BreadthHttpMetricsGroupedByStatus,
			wantArgs: []string{
				`serviceId: "svc-1"`,
				`environmentId: "env-1"`,
			},
			wantVarValues: map[string]any{
				"startDate":   "2025-01-01T00:00:00Z",
				"endDate":     "2025-01-01T06:00:00Z",
				"stepSeconds": 60,
			},
		},
		{
			name: "Usage",
			item: types.WorkItem{
				ID: "usage:proj-1", Kind: types.QueryUsage,
				TaskType: types.TaskTypeUsage, AliasKey: "proj-1",
				Params: map[string]any{
					"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"groupBy":      []railway.MetricTag{railway.MetricTagProjectId},
				},
			},
			wantSuffix:  "_usage",
			wantField:   "usage",
			wantBreadth: loader.BreadthUsage,
			wantArgs: []string{
				`projectId: "proj-1"`,
			},
		},
		{
			name: "EstimatedUsage",
			item: types.WorkItem{
				ID: "estimated-usage:proj-1", Kind: types.QueryEstimatedUsage,
				TaskType: types.TaskTypeUsage, AliasKey: "proj-1",
				Params: map[string]any{
					"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
				},
			},
			wantSuffix:  "_estusage",
			wantField:   "estimatedUsage",
			wantBreadth: loader.BreadthEstimatedUsage,
			wantArgs: []string{
				`projectId: "proj-1"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := collector.FragmentFromWorkItem(tt.item, time.Now())
			assert.Contains(t, f.Alias, tt.wantSuffix, "alias %s should contain %s", f.Alias, tt.wantSuffix)
			assert.Equal(t, tt.wantField, f.Field)
			assert.Equal(t, tt.wantBreadth, f.Breadth)
			for _, arg := range tt.wantArgs {
				assert.Contains(t, f.Args, arg)
			}
			for key, want := range tt.wantVarValues {
				fullKey := key + "_" + f.Alias
				assert.Equal(t, want, f.VarValues[fullKey], "VarValues[%s]", fullKey)
			}
		})
	}
}
