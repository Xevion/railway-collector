package collector_test

import (
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

// workItemForKind builds a minimal WorkItem for the given kind, entity key, and
// time range params. This is the shared helper for all parameterized alias
// uniqueness tests.
func workItemForKind(kind types.QueryKind, aliasKey string, params map[string]any) types.WorkItem {
	taskType := types.TaskTypeMetrics
	switch kind {
	case types.QueryEnvironmentLogs, types.QueryBuildLogs, types.QueryHttpLogs:
		taskType = types.TaskTypeLogs
	case types.QueryUsage, types.QueryEstimatedUsage:
		taskType = types.TaskTypeUsage
	}

	return types.WorkItem{
		ID:       aliasKey + "_" + string(kind),
		Kind:     kind,
		TaskType: taskType,
		AliasKey: aliasKey,
		Params:   params,
	}
}

// metricsParams returns params for a project metrics work item with the given time range.
func metricsParams(start, end time.Time) map[string]any {
	return map[string]any{
		"startDate":         start.Format(time.RFC3339),
		"endDate":           end.Format(time.RFC3339),
		"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		"sampleRateSeconds": 60,
	}
}

// serviceMetricsParams returns params for a service metrics work item.
func serviceMetricsParams(serviceID, envID string, start, end time.Time) map[string]any {
	return map[string]any{
		"startDate":         start.Format(time.RFC3339),
		"endDate":           end.Format(time.RFC3339),
		"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
		"sampleRateSeconds": 60,
		"serviceId":         serviceID,
		"environmentId":     envID,
	}
}

// httpMetricsParams returns params for HTTP duration or status metrics.
func httpMetricsParams(serviceID, envID string, start, end time.Time) map[string]any {
	return map[string]any{
		"startDate":     start.Format(time.RFC3339),
		"endDate":       end.Format(time.RFC3339),
		"stepSeconds":   60,
		"serviceId":     serviceID,
		"environmentId": envID,
	}
}

// envLogParams returns params for environment logs with the given time range.
func envLogParams(start, end time.Time) map[string]any {
	return map[string]any{
		"afterDate":  start.Format(time.RFC3339),
		"beforeDate": end.Format(time.RFC3339),
		"afterLimit": 500,
	}
}

// replicaMetricsParams returns params for replica metrics.
func replicaMetricsParams(serviceID, envID string, start, end time.Time) map[string]any {
	return map[string]any{
		"startDate":         start.Format(time.RFC3339),
		"endDate":           end.Format(time.RFC3339),
		"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementMemoryUsageGb},
		"sampleRateSeconds": 60,
		"serviceId":         serviceID,
		"environmentId":     envID,
	}
}

// TestFragmentAliasUniqueness_SameEntityDifferentTimeRanges verifies that
// multiple work items targeting the same entity+kind but with different time
// ranges produce fragments with distinct aliases. This is the core collision
// bug: coverage gap chunking creates multiple WorkItems for the same
// entity+kind, and FragmentFromWorkItem currently generates identical aliases
// for all of them.
func TestFragmentAliasUniqueness_SameEntityDifferentTimeRanges(t *testing.T) {
	now := time.Date(2026, 3, 11, 22, 0, 0, 0, time.UTC)

	// Each test case produces multiple work items for the same entity+kind
	// with different time ranges (simulating coverage gap chunking).
	tests := []struct {
		name  string
		items []types.WorkItem
	}{
		{
			name: "project metrics: 3 chunks same project",
			items: []types.WorkItem{
				workItemForKind(types.QueryMetrics, "proj-aaa",
					metricsParams(now.Add(-3*time.Hour), now.Add(-2*time.Hour))),
				workItemForKind(types.QueryMetrics, "proj-aaa",
					metricsParams(now.Add(-2*time.Hour), now.Add(-1*time.Hour))),
				workItemForKind(types.QueryMetrics, "proj-aaa",
					metricsParams(now.Add(-1*time.Hour), now)),
			},
		},
		{
			name: "service metrics: 5 chunks same service:env",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 5 {
					start := now.Add(time.Duration(-5+i) * 24 * time.Hour)
					end := now.Add(time.Duration(-4+i) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryServiceMetrics, "svc-1:env-1",
						serviceMetricsParams("svc-1", "env-1", start, end)))
				}
				return items
			}(),
		},
		{
			name: "replica metrics: 3 chunks same service:env",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 3 {
					start := now.Add(time.Duration(-3+i) * 24 * time.Hour)
					end := now.Add(time.Duration(-2+i) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryReplicaMetrics, "svc-r1:env-r1",
						replicaMetricsParams("svc-r1", "env-r1", start, end)))
				}
				return items
			}(),
		},
		{
			name: "http duration metrics: 5 chunks same service:env",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 5 {
					start := now.Add(time.Duration(-5+i) * 24 * time.Hour)
					end := now.Add(time.Duration(-4+i) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryHttpDurationMetrics, "svc-h1:env-h1",
						httpMetricsParams("svc-h1", "env-h1", start, end)))
				}
				return items
			}(),
		},
		{
			name: "http status metrics: 5 chunks same service:env",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 5 {
					start := now.Add(time.Duration(-5+i) * 24 * time.Hour)
					end := now.Add(time.Duration(-4+i) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryHttpMetricsGroupedByStatus, "svc-h1:env-h1",
						httpMetricsParams("svc-h1", "env-h1", start, end)))
				}
				return items
			}(),
		},
		{
			name: "environment logs: 6 chunks same environment",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 6 {
					start := now.Add(time.Duration(-6+i) * 6 * time.Hour)
					end := now.Add(time.Duration(-5+i) * 6 * time.Hour)
					items = append(items, workItemForKind(types.QueryEnvironmentLogs, "env-abc",
						envLogParams(start, end)))
				}
				return items
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Greater(t, len(tt.items), 1, "test must have multiple items to detect collisions")

			aliases := make(map[string]int) // alias -> count
			for _, item := range tt.items {
				frag := collector.FragmentFromWorkItem(item, now)
				aliases[frag.Alias]++
			}

			// Every fragment must have a unique alias
			for alias, count := range aliases {
				assert.Equal(t, 1, count,
					"alias %q appeared %d times; same entity+kind with different time ranges must produce unique aliases",
					alias, count)
			}
		})
	}
}

// TestPackedRequestAliasUniqueness verifies that after packing fragments into
// requests, no single request contains duplicate aliases. This catches the
// case where Pack doesn't detect or prevent alias collisions.
func TestPackedRequestAliasUniqueness(t *testing.T) {
	now := time.Date(2026, 3, 11, 22, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		items []types.WorkItem
	}{
		{
			name: "mixed kinds same entity produces unique aliases per request",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				// 3 chunks of HTTP duration metrics for the same service:env
				for i := range 3 {
					start := now.Add(time.Duration(-3+i) * 24 * time.Hour)
					end := now.Add(time.Duration(-2+i) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryHttpDurationMetrics, "svc-x:env-x",
						httpMetricsParams("svc-x", "env-x", start, end)))
				}
				// 3 chunks of HTTP status metrics for the same service:env
				for i := range 3 {
					start := now.Add(time.Duration(-3+i) * 24 * time.Hour)
					end := now.Add(time.Duration(-2+i) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryHttpMetricsGroupedByStatus, "svc-x:env-x",
						httpMetricsParams("svc-x", "env-x", start, end)))
				}
				// 4 chunks of env logs for a related environment
				for i := range 4 {
					start := now.Add(time.Duration(-4+i) * 6 * time.Hour)
					end := now.Add(time.Duration(-3+i) * 6 * time.Hour)
					items = append(items, workItemForKind(types.QueryEnvironmentLogs, "env-x",
						envLogParams(start, end)))
				}
				return items
			}(),
		},
		{
			name: "request 18 reproduction: httpDur:5 httpStatus:5 envLogs:9 replica:1",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				svcID := "3ce706f3-d4ad-482d-90ae-ff0d4e970e5b"
				envID := "c6c3228f-f663-4790-845a-cbcc4bddcf79"
				compositeKey := svcID + ":" + envID

				// 5 httpDurationMetrics chunks for the same service:env
				for i := range 5 {
					start := now.Add(time.Duration(-90+i*18) * 24 * time.Hour)
					end := now.Add(time.Duration(-72+i*18) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryHttpDurationMetrics, compositeKey,
						httpMetricsParams(svcID, envID, start, end)))
				}
				// 5 httpMetricsGroupedByStatus chunks for the same service:env
				for i := range 5 {
					start := now.Add(time.Duration(-90+i*18) * 24 * time.Hour)
					end := now.Add(time.Duration(-72+i*18) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryHttpMetricsGroupedByStatus, compositeKey,
						httpMetricsParams(svcID, envID, start, end)))
				}
				// 6 env log chunks for envID
				for i := range 6 {
					start := now.Add(time.Duration(-6+i) * 6 * time.Hour)
					end := now.Add(time.Duration(-5+i) * 6 * time.Hour)
					items = append(items, workItemForKind(types.QueryEnvironmentLogs, envID,
						envLogParams(start, end)))
				}
				// 3 env log chunks for a second environment
				envID2 := "a2db8319-1d70-4fcc-897d-e9bc38872ad6"
				for i := range 3 {
					start := now.Add(time.Duration(-3+i) * 6 * time.Hour)
					end := now.Add(time.Duration(-2+i) * 6 * time.Hour)
					items = append(items, workItemForKind(types.QueryEnvironmentLogs, envID2,
						envLogParams(start, end)))
				}
				// 1 replica metrics
				items = append(items, workItemForKind(types.QueryReplicaMetrics, compositeKey,
					replicaMetricsParams(svcID, envID, now.Add(-24*time.Hour), now)))

				return items
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fragments []loader.AliasFragment
			for _, item := range tt.items {
				frag := collector.FragmentFromWorkItem(item, now)
				fragments = append(fragments, frag)
			}

			requests := loader.Pack(fragments)

			for ri, req := range requests {
				seen := make(map[string]int)
				for _, frag := range req.Fragments {
					seen[frag.Alias]++
				}
				for alias, count := range seen {
					assert.Equal(t, 1, count,
						"request %d: alias %q appears %d times; each alias in a packed request must be unique",
						ri, alias, count)
				}
			}
		})
	}
}

// TestAssembledQueryNoDuplicateVars verifies that when fragments are packed
// into a request and assembled into a GraphQL query, there are no duplicate
// variable declarations. This is the direct symptom of the collision bug:
// "$startDate_p_<id>_httpdur" declared multiple times.
func TestAssembledQueryNoDuplicateVars(t *testing.T) {
	now := time.Date(2026, 3, 11, 22, 0, 0, 0, time.UTC)

	tests := []struct {
		name  string
		items []types.WorkItem
	}{
		{
			name: "5 http duration chunks same entity",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 5 {
					start := now.Add(time.Duration(-90+i*18) * 24 * time.Hour)
					end := now.Add(time.Duration(-72+i*18) * 24 * time.Hour)
					items = append(items, workItemForKind(types.QueryHttpDurationMetrics, "svc-1:env-1",
						httpMetricsParams("svc-1", "env-1", start, end)))
				}
				return items
			}(),
		},
		{
			name: "3 project metrics chunks same project",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 3 {
					start := now.Add(time.Duration(-3+i) * time.Hour)
					end := now.Add(time.Duration(-2+i) * time.Hour)
					items = append(items, workItemForKind(types.QueryMetrics, "proj-dup",
						metricsParams(start, end)))
				}
				return items
			}(),
		},
		{
			name: "6 env log chunks same environment",
			items: func() []types.WorkItem {
				var items []types.WorkItem
				for i := range 6 {
					start := now.Add(time.Duration(-6+i) * 6 * time.Hour)
					end := now.Add(time.Duration(-5+i) * 6 * time.Hour)
					items = append(items, workItemForKind(types.QueryEnvironmentLogs, "env-dup",
						envLogParams(start, end)))
				}
				return items
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var fragments []loader.AliasFragment
			for _, item := range tt.items {
				frag := collector.FragmentFromWorkItem(item, now)
				fragments = append(fragments, frag)
			}

			// Force all fragments into one request for maximum collision potential
			req := &loader.Request{Fragments: fragments}
			query, vars := req.AssembleQuery()

			// Check for duplicate variable declarations in the query string.
			// The query format is: query Batch($var1: Type!, $var2: Type!, ...) { ... }
			// Extract the variable declaration section.
			varSection := query[strings.Index(query, "(")+1 : strings.Index(query, ")")]
			declParts := strings.Split(varSection, ", ")

			declCounts := make(map[string]int)
			for _, decl := range declParts {
				decl = strings.TrimSpace(decl)
				if decl == "" {
					continue
				}
				declCounts[decl]++
			}

			for decl, count := range declCounts {
				assert.Equal(t, 1, count,
					"variable declaration %q appears %d times in query; must be unique",
					decl, count)
			}

			// Also verify no duplicate keys in the variables map.
			// (maps can't have duplicate keys in Go, but duplicate VarValues
			// from different fragments would silently overwrite each other,
			// meaning the last writer wins and earlier fragments get wrong values.)
			//
			// Check that the number of var values equals the total across all fragments.
			expectedVarCount := 0
			for _, frag := range fragments {
				expectedVarCount += len(frag.VarValues)
			}
			assert.Equal(t, expectedVarCount, len(vars),
				"variable count mismatch: %d fragments produced %d total vars but AssembleQuery merged to %d (silent overwrites)",
				len(fragments), expectedVarCount, len(vars))
		})
	}
}

// TestCrossKindSameEntityAliasUniqueness verifies that different query kinds
// targeting the same entity ID produce distinct aliases (this already works
// via kind suffixes, but we test it to prevent regressions).
func TestCrossKindSameEntityAliasUniqueness(t *testing.T) {
	now := time.Date(2026, 3, 11, 22, 0, 0, 0, time.UTC)
	start := now.Add(-1 * time.Hour)

	compositeKey := "svc-1:env-1"
	items := []types.WorkItem{
		workItemForKind(types.QueryServiceMetrics, compositeKey,
			serviceMetricsParams("svc-1", "env-1", start, now)),
		workItemForKind(types.QueryReplicaMetrics, compositeKey,
			replicaMetricsParams("svc-1", "env-1", start, now)),
		workItemForKind(types.QueryHttpDurationMetrics, compositeKey,
			httpMetricsParams("svc-1", "env-1", start, now)),
		workItemForKind(types.QueryHttpMetricsGroupedByStatus, compositeKey,
			httpMetricsParams("svc-1", "env-1", start, now)),
	}

	aliases := make(map[string]int)
	for _, item := range items {
		frag := collector.FragmentFromWorkItem(item, now)
		aliases[frag.Alias]++
	}

	for alias, count := range aliases {
		assert.Equal(t, 1, count,
			"alias %q appeared %d times across different kinds for the same entity",
			alias, count)
	}
}
