package collector_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/loader"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/railway"
)

func TestFragmentFromWorkItem_EstimateMetricPoints(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour) // 3600 seconds before now
	end := now                       // exactly now

	tests := []struct {
		name           string
		item           types.WorkItem
		expectedPoints int
	}{
		{
			name: "valid params single measurement",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-1",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           end.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 60,
				},
			},
			// 3600 / 60 * 1 = 60
			expectedPoints: 60,
		},
		{
			name: "valid params multiple measurements",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-2",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           end.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage, railway.MetricMeasurementMemoryUsageGb, railway.MetricMeasurementNetworkRxGb},
					"sampleRateSeconds": 60,
				},
			},
			// 3600 / 60 * 3 = 180
			expectedPoints: 180,
		},
		{
			name: "invalid startDate",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-3",
				Params: map[string]any{
					"startDate":         "not-a-date",
					"endDate":           end.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 60,
				},
			},
			expectedPoints: 0,
		},
		{
			name: "invalid endDate",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-4",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           "not-a-date",
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 60,
				},
			},
			expectedPoints: 0,
		},
		{
			name: "endDate before startDate (negative duration)",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-5",
				Params: map[string]any{
					"startDate":         end.Format(time.RFC3339),
					"endDate":           start.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 60,
				},
			},
			expectedPoints: 0,
		},
		{
			name: "endDate equals startDate (zero duration)",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-6",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           start.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 60,
				},
			},
			expectedPoints: 0,
		},
		{
			name: "sampleRateSeconds is zero",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-7",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           end.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 0,
				},
			},
			expectedPoints: 0,
		},
		{
			name: "sampleRateSeconds is negative",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-8",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           end.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": -1,
				},
			},
			expectedPoints: 0,
		},
		{
			name: "sampleRateSeconds missing from params",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-9",
				Params: map[string]any{
					"startDate":    start.Format(time.RFC3339),
					"endDate":      end.Format(time.RFC3339),
					"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
				},
			},
			expectedPoints: 0,
		},
		{
			name: "sampleRateSeconds is 1 (boundary positive)",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-10",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           end.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 1,
				},
			},
			// 3600 / 1 * 1 = 3600
			expectedPoints: 3600,
		},
		{
			name: "no endDate uses now",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-11",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 60,
				},
			},
			// now - start = 3600s, 3600/60 * 1 = 60
			expectedPoints: 60,
		},
		{
			name: "slightly positive duration (1 second)",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-12",
				Params: map[string]any{
					"startDate":         start.Format(time.RFC3339),
					"endDate":           start.Add(1 * time.Second).Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 1,
				},
			},
			// 1 / 1 * 1 = 1
			expectedPoints: 1,
		},
		{
			name: "startDate missing (empty string)",
			item: types.WorkItem{
				Kind:     types.QueryMetrics,
				AliasKey: "proj-13",
				Params: map[string]any{
					"endDate":           end.Format(time.RFC3339),
					"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
					"sampleRateSeconds": 60,
				},
			},
			expectedPoints: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frag := collector.FragmentFromWorkItem(tt.item, now)
			assert.Equal(t, tt.expectedPoints, frag.EstimatedPoints, "EstimatedPoints mismatch")
		})
	}
}

func TestFragmentFromWorkItem_EstimateHttpMetricPoints(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour) // 3600 seconds
	end := now

	tests := []struct {
		name           string
		kind           types.QueryKind
		expectedPoints int
		params         map[string]any
	}{
		{
			name: "valid http duration metrics",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       end.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   60,
			},
			// 3600 / 60 = 60
			expectedPoints: 60,
		},
		{
			name: "valid http status metrics",
			kind: types.QueryHttpMetricsGroupedByStatus,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       end.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   120,
			},
			// 3600 / 120 = 30
			expectedPoints: 30,
		},
		{
			name: "invalid startDate",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     "bad",
				"endDate":       end.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   60,
			},
			expectedPoints: 0,
		},
		{
			name: "invalid endDate",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       "bad",
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   60,
			},
			expectedPoints: 0,
		},
		{
			name: "both dates invalid",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     "bad",
				"endDate":       "bad",
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   60,
			},
			expectedPoints: 0,
		},
		{
			name: "end before start (negative duration)",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     end.Format(time.RFC3339),
				"endDate":       start.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   60,
			},
			expectedPoints: 0,
		},
		{
			name: "end equals start (zero duration)",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       start.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   60,
			},
			expectedPoints: 0,
		},
		{
			name: "stepSeconds is zero",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       end.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   0,
			},
			expectedPoints: 0,
		},
		{
			name: "stepSeconds is negative",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       end.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   -1,
			},
			expectedPoints: 0,
		},
		{
			name: "stepSeconds missing",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       end.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
			},
			expectedPoints: 0,
		},
		{
			name: "stepSeconds is 1 (boundary positive)",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       end.Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   1,
			},
			// 3600 / 1 = 3600
			expectedPoints: 3600,
		},
		{
			name: "slightly positive duration (1 second)",
			kind: types.QueryHttpDurationMetrics,
			params: map[string]any{
				"startDate":     start.Format(time.RFC3339),
				"endDate":       start.Add(1 * time.Second).Format(time.RFC3339),
				"serviceId":     "svc-1",
				"environmentId": "env-1",
				"stepSeconds":   1,
			},
			// 1 / 1 = 1
			expectedPoints: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			item := types.WorkItem{
				Kind:     tt.kind,
				AliasKey: "svc-1:env-1",
				Params:   tt.params,
			}
			frag := collector.FragmentFromWorkItem(item, now)
			assert.Equal(t, tt.expectedPoints, frag.EstimatedPoints, "EstimatedPoints mismatch")
		})
	}
}

func TestFragmentFromWorkItem_MetricsFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	item := types.WorkItem{
		Kind:     types.QueryMetrics,
		AliasKey: "proj-abc",
		Params: map[string]any{
			"startDate":    start.Format(time.RFC3339),
			"endDate":      now.Format(time.RFC3339),
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "p_proj_abc")
	assert.Equal(t, "metrics", frag.Field)
	assert.Equal(t, railway.MetricsFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthMetrics, frag.Breadth)
	assert.Equal(t, item, frag.Item)

	// Must have VarDecls for startDate, endDate, measurements
	assert.NotEmpty(t, frag.VarDecls)
	assert.NotEmpty(t, frag.VarValues)
	assert.NotEmpty(t, frag.Args)
}

func TestFragmentFromWorkItem_MetricsOptionalVars(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	t.Run("with optional params present", func(t *testing.T) {
		item := types.WorkItem{
			Kind:     types.QueryMetrics,
			AliasKey: "proj-opt",
			Params: map[string]any{
				"startDate":              start.Format(time.RFC3339),
				"endDate":                now.Format(time.RFC3339),
				"measurements":           []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
				"sampleRateSeconds":      60,
				"groupBy":                []string{"SERVICE"},
				"averagingWindowSeconds": 120,
			},
		}
		frag := collector.FragmentFromWorkItem(item, now)

		// sampleRateSeconds, groupBy, averagingWindowSeconds, endDate should all be in vars
		alias := frag.Alias
		assert.Contains(t, frag.VarValues, "sampleRateSeconds_"+alias)
		assert.Contains(t, frag.VarValues, "groupBy_"+alias)
		assert.Contains(t, frag.VarValues, "averagingWindowSeconds_"+alias)
		assert.Contains(t, frag.VarValues, "endDate_"+alias)
	})

	t.Run("without optional params", func(t *testing.T) {
		item := types.WorkItem{
			Kind:     types.QueryMetrics,
			AliasKey: "proj-noopt",
			Params: map[string]any{
				"startDate":    start.Format(time.RFC3339),
				"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			},
		}
		frag := collector.FragmentFromWorkItem(item, now)

		alias := frag.Alias
		// Optional vars should NOT be present when params are missing
		assert.NotContains(t, frag.VarValues, "sampleRateSeconds_"+alias)
		assert.NotContains(t, frag.VarValues, "groupBy_"+alias)
		assert.NotContains(t, frag.VarValues, "averagingWindowSeconds_"+alias)
		assert.NotContains(t, frag.VarValues, "endDate_"+alias)
	})
}

func TestFragmentFromWorkItem_HttpDurationFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	item := types.WorkItem{
		Kind:     types.QueryHttpDurationMetrics,
		AliasKey: "svc-1:env-1",
		Params: map[string]any{
			"startDate":     start.Format(time.RFC3339),
			"endDate":       now.Format(time.RFC3339),
			"serviceId":     "svc-1",
			"environmentId": "env-1",
			"stepSeconds":   60,
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_httpdur")
	assert.Equal(t, "httpDurationMetrics", frag.Field)
	assert.Equal(t, railway.HttpDurationMetricsFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthHttpDurationMetrics, frag.Breadth)

	// stepSeconds is optional - should be in vars when provided
	found := false
	for k := range frag.VarValues {
		if len(k) > len("stepSeconds_") && k[:len("stepSeconds_")] == "stepSeconds_" {
			found = true
			break
		}
	}
	assert.True(t, found, "stepSeconds variable should be present")
}

func TestFragmentFromWorkItem_HttpStatusFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	item := types.WorkItem{
		Kind:     types.QueryHttpMetricsGroupedByStatus,
		AliasKey: "svc-2:env-2",
		Params: map[string]any{
			"startDate":     start.Format(time.RFC3339),
			"endDate":       now.Format(time.RFC3339),
			"serviceId":     "svc-2",
			"environmentId": "env-2",
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_httpstatus")
	assert.Equal(t, "httpMetricsGroupedByStatus", frag.Field)
	assert.Equal(t, railway.HttpMetricsGroupedByStatusFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthHttpMetricsGroupedByStatus, frag.Breadth)
}

func TestFragmentFromWorkItem_EnvLogsFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("with optional params", func(t *testing.T) {
		item := types.WorkItem{
			Kind:     types.QueryEnvironmentLogs,
			AliasKey: "env-abc",
			Params: map[string]any{
				"afterDate":  "2025-01-01T00:00:00Z",
				"beforeDate": "2025-01-01T12:00:00Z",
				"afterLimit": 100,
			},
		}
		frag := collector.FragmentFromWorkItem(item, now)

		assert.Contains(t, frag.Alias, "p_env_abc")
		assert.Equal(t, "environmentLogs", frag.Field)
		assert.Equal(t, railway.EnvironmentLogsFieldBody, frag.Selection)
		assert.Equal(t, loader.BreadthEnvironmentLogs, frag.Breadth)

		alias := frag.Alias
		assert.Contains(t, frag.VarValues, "afterDate_"+alias)
		assert.Contains(t, frag.VarValues, "beforeDate_"+alias)
		assert.Contains(t, frag.VarValues, "afterLimit_"+alias)
	})

	t.Run("without optional params", func(t *testing.T) {
		item := types.WorkItem{
			Kind:     types.QueryEnvironmentLogs,
			AliasKey: "env-xyz",
			Params:   map[string]any{},
		}
		frag := collector.FragmentFromWorkItem(item, now)

		alias := frag.Alias
		assert.NotContains(t, frag.VarValues, "afterDate_"+alias)
		assert.NotContains(t, frag.VarValues, "beforeDate_"+alias)
		assert.NotContains(t, frag.VarValues, "afterLimit_"+alias)
	})
}

func TestFragmentFromWorkItem_BuildLogsFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	item := types.WorkItem{
		Kind:     types.QueryBuildLogs,
		AliasKey: "deploy-1",
		Params: map[string]any{
			"limit":     500,
			"startDate": "2025-01-01T00:00:00Z",
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_build")
	assert.Equal(t, "buildLogs", frag.Field)
	assert.Equal(t, railway.BuildLogsFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthBuildLogs, frag.Breadth)
}

func TestFragmentFromWorkItem_HttpLogsFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	item := types.WorkItem{
		Kind:     types.QueryHttpLogs,
		AliasKey: "deploy-2",
		Params: map[string]any{
			"limit":     200,
			"startDate": "2025-01-01T00:00:00Z",
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_http")
	assert.Equal(t, "httpLogs", frag.Field)
	assert.Equal(t, railway.HttpLogsFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthHttpLogs, frag.Breadth)
}

func TestFragmentFromWorkItem_UsageFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	item := types.WorkItem{
		Kind:     types.QueryUsage,
		AliasKey: "proj-usage",
		Params: map[string]any{
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"startDate":    "2025-01-01T00:00:00Z",
			"endDate":      "2025-01-01T12:00:00Z",
			"groupBy":      []string{"SERVICE"},
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_usage")
	assert.Equal(t, "usage", frag.Field)
	assert.Equal(t, railway.UsageFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthUsage, frag.Breadth)

	alias := frag.Alias
	assert.Contains(t, frag.VarValues, "startDate_"+alias)
	assert.Contains(t, frag.VarValues, "endDate_"+alias)
	assert.Contains(t, frag.VarValues, "groupBy_"+alias)
}

func TestFragmentFromWorkItem_EstimatedUsageFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	item := types.WorkItem{
		Kind:     types.QueryEstimatedUsage,
		AliasKey: "proj-est",
		Params: map[string]any{
			"measurements": []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_estusage")
	assert.Equal(t, "estimatedUsage", frag.Field)
	assert.Equal(t, railway.EstimatedUsageFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthEstimatedUsage, frag.Breadth)
}

func TestFragmentFromWorkItem_UnknownKind(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	item := types.WorkItem{
		Kind:     types.QueryDiscovery,
		AliasKey: "disc-1",
		Params:   map[string]any{},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Empty(t, frag.Alias)
	assert.Empty(t, frag.Field)
	assert.Equal(t, item, frag.Item)
	assert.Equal(t, 0, frag.EstimatedPoints)
}

func TestFragmentFromWorkItem_ServiceMetricsFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	item := types.WorkItem{
		Kind:     types.QueryServiceMetrics,
		AliasKey: "svc-s1:env-e1",
		Params: map[string]any{
			"startDate":         start.Format(time.RFC3339),
			"endDate":           now.Format(time.RFC3339),
			"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"serviceId":         "svc-s1",
			"environmentId":     "env-e1",
			"sampleRateSeconds": 60,
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_svcmetrics")
	assert.Equal(t, "metrics", frag.Field)
	assert.Equal(t, railway.MetricsFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthMetrics, frag.Breadth)
	// 3600/60 * 1 = 60
	assert.Equal(t, 60, frag.EstimatedPoints)
}

func TestFragmentFromWorkItem_ReplicaMetricsFields(t *testing.T) {
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	item := types.WorkItem{
		Kind:     types.QueryReplicaMetrics,
		AliasKey: "svc-r1:env-r1",
		Params: map[string]any{
			"startDate":         start.Format(time.RFC3339),
			"endDate":           now.Format(time.RFC3339),
			"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementMemoryUsageGb},
			"serviceId":         "svc-r1",
			"environmentId":     "env-r1",
			"sampleRateSeconds": 120,
		},
	}
	frag := collector.FragmentFromWorkItem(item, now)

	assert.Contains(t, frag.Alias, "_replica")
	assert.Equal(t, "replicaMetrics", frag.Field)
	assert.Equal(t, railway.ReplicaMetricsFieldBody, frag.Selection)
	assert.Equal(t, loader.BreadthReplicaMetrics, frag.Breadth)
	// 3600/120 * 1 = 30
	assert.Equal(t, 30, frag.EstimatedPoints)
}
