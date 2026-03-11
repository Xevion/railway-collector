package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/xevion/railway-collector/internal/railway"
)

// Breadth costs per query type, determined by Railway API error messages.
const (
	BreadthMetrics                    = 12
	BreadthEnvironmentLogs            = 13
	BreadthBuildLogs                  = 13
	BreadthDeploymentLogs             = 13
	BreadthHttpLogs                   = 21
	BreadthHttpDurationMetrics        = 5
	BreadthHttpMetricsGroupedByStatus = 5
	BreadthReplicaMetrics             = 12 // assumed same as BreadthMetrics; same selection set shape
	BreadthNetworkFlowServiceLayer    = 11
	BreadthUsage                      = 12
	BreadthEstimatedUsage             = 5

	// MaxBreadth is the per-request breadth cap imposed by the Railway API.
	MaxBreadth = 500

	// MaxAliasesPerRequest caps aliases per request to avoid "Problem processing
	// request" errors from the API's undocumented computation limits.
	MaxAliasesPerRequest = 20

	// MaxPointsPerRequest caps the estimated total data points in a single
	// request. The Railway API computation limit is around 45-60k points;
	// we target ~30k to stay safely under.
	MaxPointsPerRequest = 30000
)

// AliasFragment is a self-contained GraphQL alias with fully-namespaced
// variables. Fragments from different query types can be packed into a
// single request without variable collisions.
type AliasFragment struct {
	// Alias is the GraphQL alias name (e.g. "p_abc123").
	Alias string
	// Field is the GraphQL root field (e.g. "metrics", "environmentLogs").
	Field string
	// Args is the argument string (e.g. `projectId: "...", startDate: $startDate_p_abc123`).
	Args string
	// Selection is the field body / selection set.
	Selection string
	// VarDecls are the variable declarations (e.g. "$startDate_p_abc123: DateTime!").
	VarDecls []string
	// VarValues maps variable names to their values.
	VarValues map[string]any
	// Breadth is the breadth cost for this alias.
	Breadth int
	// EstimatedPoints is the estimated number of data points this alias will
	// return, based on time range, sample rate, and measurement count.
	EstimatedPoints int
	// Item is the original WorkItem that produced this fragment.
	Item WorkItem
}

// Request is a packed set of alias fragments that fit within the breadth budget.
type Request struct {
	Fragments       []AliasFragment
	Breadth         int
	EstimatedPoints int
}

// AssembleQuery builds a GraphQL query string and variables map from a Request.
func (r *Request) AssembleQuery() (query string, variables map[string]any) {
	variables = make(map[string]any)
	var varDecls []string
	var aliases []string

	for _, f := range r.Fragments {
		for _, decl := range f.VarDecls {
			varDecls = append(varDecls, decl)
		}
		for k, v := range f.VarValues {
			variables[k] = v
		}
		aliases = append(aliases, fmt.Sprintf("    %s: %s(%s) %s", f.Alias, f.Field, f.Args, f.Selection))
	}

	query = fmt.Sprintf("query Batch(%s) {\n%s\n}", strings.Join(varDecls, ", "), strings.Join(aliases, "\n"))
	return query, variables
}

// Items returns all WorkItems in this request.
func (r *Request) Items() []WorkItem {
	items := make([]WorkItem, len(r.Fragments))
	for i, f := range r.Fragments {
		items[i] = f.Item
	}
	return items
}

// Pack groups alias fragments into requests, each fitting within MaxBreadth.
// Fragments are packed in the order given (caller should sort by priority).
func Pack(fragments []AliasFragment) []Request {
	if len(fragments) == 0 {
		return nil
	}

	var requests []Request
	current := Request{}

	for _, f := range fragments {
		if f.Breadth > MaxBreadth {
			requests = append(requests, Request{
				Fragments:       []AliasFragment{f},
				Breadth:         f.Breadth,
				EstimatedPoints: f.EstimatedPoints,
			})
			continue
		}

		wouldExceed := current.Breadth+f.Breadth > MaxBreadth ||
			len(current.Fragments) >= MaxAliasesPerRequest ||
			(current.EstimatedPoints+f.EstimatedPoints > MaxPointsPerRequest && len(current.Fragments) > 0)

		if wouldExceed {
			if len(current.Fragments) > 0 {
				requests = append(requests, current)
			}
			current = Request{}
		}

		current.Fragments = append(current.Fragments, f)
		current.Breadth += f.Breadth
		current.EstimatedPoints += f.EstimatedPoints
	}

	if len(current.Fragments) > 0 {
		requests = append(requests, current)
	}

	return requests
}

// estimateMetricPoints estimates the number of data points a metrics query will
// return based on time range, sample rate, and measurement count.
// For open-ended queries (no endDate), uses nowFunc to determine the end time.
func estimateMetricPoints(params map[string]any, nowFunc func() time.Time) int {
	startStr, _ := params["startDate"].(string)
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return 0
	}

	var end time.Time
	if endStr, ok := params["endDate"].(string); ok {
		end, err = time.Parse(time.RFC3339, endStr)
		if err != nil {
			return 0
		}
	} else if nowFunc != nil {
		end = nowFunc()
	} else {
		end = time.Now()
	}

	durationSec := end.Sub(start).Seconds()
	if durationSec <= 0 {
		return 0
	}

	sampleRate, ok := params["sampleRateSeconds"].(int)
	if !ok || sampleRate <= 0 {
		return 0
	}

	numSamples := int(durationSec) / sampleRate

	numMeasurements := 1
	if meas, ok := params["measurements"].([]railway.MetricMeasurement); ok {
		numMeasurements = len(meas)
	}

	return numSamples * numMeasurements
}

// estimateHttpMetricPoints estimates points for HTTP duration/status queries.
func estimateHttpMetricPoints(params map[string]any) int {
	startStr, _ := params["startDate"].(string)
	endStr, _ := params["endDate"].(string)
	start, err1 := time.Parse(time.RFC3339, startStr)
	end, err2 := time.Parse(time.RFC3339, endStr)
	if err1 != nil || err2 != nil {
		return 0
	}

	durationSec := end.Sub(start).Seconds()
	if durationSec <= 0 {
		return 0
	}

	stepSec, ok := params["stepSeconds"].(int)
	if !ok || stepSec <= 0 {
		return 0
	}

	return int(durationSec) / stepSec
}

// FragmentFromWorkItem converts a WorkItem into a self-contained AliasFragment
// with fully namespaced variables. The now parameter is used for estimating
// data points on open-ended queries (no endDate).
func FragmentFromWorkItem(item WorkItem, now time.Time) AliasFragment {
	nowFunc := func() time.Time { return now }
	switch item.Kind {
	case QueryMetrics:
		return metricsFragment(item, nowFunc)
	case QueryServiceMetrics:
		return serviceMetricsFragment(item, nowFunc)
	case QueryReplicaMetrics:
		return replicaMetricsFragment(item, nowFunc)
	case QueryHttpDurationMetrics:
		return httpDurationMetricsFragment(item)
	case QueryHttpMetricsGroupedByStatus:
		return httpStatusMetricsFragment(item)
	case QueryUsage:
		return usageFragment(item)
	case QueryEstimatedUsage:
		return estimatedUsageFragment(item)
	case QueryEnvironmentLogs:
		return envLogsFragment(item)
	case QueryBuildLogs:
		return buildLogsFragment(item)
	case QueryHttpLogs:
		return httpLogsFragment(item)
	default:
		// Discovery and unknown types produce empty fragments.
		return AliasFragment{Item: item}
	}
}

func metricsFragment(item WorkItem, nowFunc func() time.Time) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey)
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	// projectId is a literal, not a variable
	argParts = append(argParts, fmt.Sprintf("projectId: %q", item.AliasKey))

	// startDate (required)
	startVar := "startDate_" + alias
	decls = append(decls, fmt.Sprintf("$%s: DateTime!", startVar))
	vars[startVar] = item.Params["startDate"]
	argParts = append(argParts, fmt.Sprintf("startDate: $%s", startVar))

	// endDate (optional)
	if ed, ok := item.Params["endDate"]; ok {
		endVar := "endDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: DateTime", endVar))
		vars[endVar] = ed
		argParts = append(argParts, fmt.Sprintf("endDate: $%s", endVar))
	}

	// measurements
	measVar := "measurements_" + alias
	decls = append(decls, fmt.Sprintf("$%s: [MetricMeasurement!]!", measVar))
	vars[measVar] = item.Params["measurements"]
	argParts = append(argParts, fmt.Sprintf("measurements: $%s", measVar))

	// groupBy (optional)
	if gb, ok := item.Params["groupBy"]; ok {
		gbVar := "groupBy_" + alias
		decls = append(decls, fmt.Sprintf("$%s: [MetricTag!]", gbVar))
		vars[gbVar] = gb
		argParts = append(argParts, fmt.Sprintf("groupBy: $%s", gbVar))
	}

	// sampleRateSeconds
	if sr, ok := item.Params["sampleRateSeconds"]; ok {
		srVar := "sampleRateSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", srVar))
		vars[srVar] = sr
		argParts = append(argParts, fmt.Sprintf("sampleRateSeconds: $%s", srVar))
	}

	// averagingWindowSeconds
	if aw, ok := item.Params["averagingWindowSeconds"]; ok {
		awVar := "averagingWindowSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", awVar))
		vars[awVar] = aw
		argParts = append(argParts, fmt.Sprintf("averagingWindowSeconds: $%s", awVar))
	}

	return AliasFragment{
		Alias:           alias,
		Field:           "metrics",
		Args:            strings.Join(argParts, ", "),
		Selection:       railway.MetricsFieldBody,
		VarDecls:        decls,
		VarValues:       vars,
		Breadth:         BreadthMetrics,
		EstimatedPoints: estimateMetricPoints(item.Params, nowFunc),
		Item:            item,
	}
}

func envLogsFragment(item WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey)
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	argParts = append(argParts, fmt.Sprintf("environmentId: %q", item.AliasKey))

	if ad, ok := item.Params["afterDate"]; ok {
		v := "afterDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: String", v))
		vars[v] = ad
		argParts = append(argParts, fmt.Sprintf("afterDate: $%s", v))
	}
	if bd, ok := item.Params["beforeDate"]; ok {
		v := "beforeDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: String", v))
		vars[v] = bd
		argParts = append(argParts, fmt.Sprintf("beforeDate: $%s", v))
	}
	if al, ok := item.Params["afterLimit"]; ok {
		v := "afterLimit_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", v))
		vars[v] = al
		argParts = append(argParts, fmt.Sprintf("afterLimit: $%s", v))
	}

	return AliasFragment{
		Alias:     alias,
		Field:     "environmentLogs",
		Args:      strings.Join(argParts, ", "),
		Selection: railway.EnvironmentLogsFieldBody,
		VarDecls:  decls,
		VarValues: vars,
		Breadth:   BreadthEnvironmentLogs,
		Item:      item,
	}
}

func buildLogsFragment(item WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_build"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	argParts = append(argParts, fmt.Sprintf("deploymentId: %q", item.AliasKey))

	if l, ok := item.Params["limit"]; ok {
		v := "limit_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", v))
		vars[v] = l
		argParts = append(argParts, fmt.Sprintf("limit: $%s", v))
	}
	if sd, ok := item.Params["startDate"]; ok {
		v := "startDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: DateTime", v))
		vars[v] = sd
		argParts = append(argParts, fmt.Sprintf("startDate: $%s", v))
	}

	return AliasFragment{
		Alias:     alias,
		Field:     "buildLogs",
		Args:      strings.Join(argParts, ", "),
		Selection: railway.BuildLogsFieldBody,
		VarDecls:  decls,
		VarValues: vars,
		Breadth:   BreadthBuildLogs,
		Item:      item,
	}
}

func httpLogsFragment(item WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_http"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	argParts = append(argParts, fmt.Sprintf("deploymentId: %q", item.AliasKey))

	if l, ok := item.Params["limit"]; ok {
		v := "limit_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", v))
		vars[v] = l
		argParts = append(argParts, fmt.Sprintf("limit: $%s", v))
	}
	// httpLogs uses String type for dates, not DateTime
	if sd, ok := item.Params["startDate"]; ok {
		v := "startDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: String", v))
		vars[v] = sd
		argParts = append(argParts, fmt.Sprintf("startDate: $%s", v))
	}

	return AliasFragment{
		Alias:     alias,
		Field:     "httpLogs",
		Args:      strings.Join(argParts, ", "),
		Selection: railway.HttpLogsFieldBody,
		VarDecls:  decls,
		VarValues: vars,
		Breadth:   BreadthHttpLogs,
		Item:      item,
	}
}

// sanitizeCompositeAlias wraps SanitizeAlias and additionally replaces colons
// with underscores, for composite keys like "serviceID:envID".
func sanitizeCompositeAlias(key string) string {
	return strings.ReplaceAll(railway.SanitizeAlias(key), ":", "_")
}

func serviceMetricsFragment(item WorkItem, nowFunc func() time.Time) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_svcmetrics"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	// serviceId and environmentId are literals extracted from Params
	argParts = append(argParts, fmt.Sprintf("serviceId: %q", item.Params["serviceId"]))
	argParts = append(argParts, fmt.Sprintf("environmentId: %q", item.Params["environmentId"]))

	// startDate (required)
	startVar := "startDate_" + alias
	decls = append(decls, fmt.Sprintf("$%s: DateTime!", startVar))
	vars[startVar] = item.Params["startDate"]
	argParts = append(argParts, fmt.Sprintf("startDate: $%s", startVar))

	// endDate (optional)
	if ed, ok := item.Params["endDate"]; ok {
		endVar := "endDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: DateTime", endVar))
		vars[endVar] = ed
		argParts = append(argParts, fmt.Sprintf("endDate: $%s", endVar))
	}

	// measurements
	measVar := "measurements_" + alias
	decls = append(decls, fmt.Sprintf("$%s: [MetricMeasurement!]!", measVar))
	vars[measVar] = item.Params["measurements"]
	argParts = append(argParts, fmt.Sprintf("measurements: $%s", measVar))

	// groupBy (optional)
	if gb, ok := item.Params["groupBy"]; ok {
		gbVar := "groupBy_" + alias
		decls = append(decls, fmt.Sprintf("$%s: [MetricTag!]", gbVar))
		vars[gbVar] = gb
		argParts = append(argParts, fmt.Sprintf("groupBy: $%s", gbVar))
	}

	// sampleRateSeconds (optional)
	if sr, ok := item.Params["sampleRateSeconds"]; ok {
		srVar := "sampleRateSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", srVar))
		vars[srVar] = sr
		argParts = append(argParts, fmt.Sprintf("sampleRateSeconds: $%s", srVar))
	}

	// averagingWindowSeconds (optional)
	if aw, ok := item.Params["averagingWindowSeconds"]; ok {
		awVar := "averagingWindowSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", awVar))
		vars[awVar] = aw
		argParts = append(argParts, fmt.Sprintf("averagingWindowSeconds: $%s", awVar))
	}

	return AliasFragment{
		Alias:           alias,
		Field:           "metrics",
		Args:            strings.Join(argParts, ", "),
		Selection:       railway.MetricsFieldBody,
		VarDecls:        decls,
		VarValues:       vars,
		Breadth:         BreadthMetrics,
		EstimatedPoints: estimateMetricPoints(item.Params, nowFunc),
		Item:            item,
	}
}

func replicaMetricsFragment(item WorkItem, nowFunc func() time.Time) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_replica"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	// serviceId and environmentId are literals
	argParts = append(argParts, fmt.Sprintf("serviceId: %q", item.Params["serviceId"]))
	argParts = append(argParts, fmt.Sprintf("environmentId: %q", item.Params["environmentId"]))

	// startDate (required)
	startVar := "startDate_" + alias
	decls = append(decls, fmt.Sprintf("$%s: DateTime!", startVar))
	vars[startVar] = item.Params["startDate"]
	argParts = append(argParts, fmt.Sprintf("startDate: $%s", startVar))

	// endDate (optional)
	if ed, ok := item.Params["endDate"]; ok {
		endVar := "endDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: DateTime", endVar))
		vars[endVar] = ed
		argParts = append(argParts, fmt.Sprintf("endDate: $%s", endVar))
	}

	// measurements
	measVar := "measurements_" + alias
	decls = append(decls, fmt.Sprintf("$%s: [MetricMeasurement!]!", measVar))
	vars[measVar] = item.Params["measurements"]
	argParts = append(argParts, fmt.Sprintf("measurements: $%s", measVar))

	// sampleRateSeconds (optional)
	if sr, ok := item.Params["sampleRateSeconds"]; ok {
		srVar := "sampleRateSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", srVar))
		vars[srVar] = sr
		argParts = append(argParts, fmt.Sprintf("sampleRateSeconds: $%s", srVar))
	}

	// averagingWindowSeconds (optional)
	if aw, ok := item.Params["averagingWindowSeconds"]; ok {
		awVar := "averagingWindowSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", awVar))
		vars[awVar] = aw
		argParts = append(argParts, fmt.Sprintf("averagingWindowSeconds: $%s", awVar))
	}

	return AliasFragment{
		Alias:           alias,
		Field:           "replicaMetrics",
		Args:            strings.Join(argParts, ", "),
		Selection:       railway.ReplicaMetricsFieldBody,
		VarDecls:        decls,
		VarValues:       vars,
		Breadth:         BreadthReplicaMetrics,
		EstimatedPoints: estimateMetricPoints(item.Params, nowFunc),
		Item:            item,
	}
}

func httpDurationMetricsFragment(item WorkItem) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_httpdur"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	// serviceId and environmentId are literals
	argParts = append(argParts, fmt.Sprintf("serviceId: %q", item.Params["serviceId"]))
	argParts = append(argParts, fmt.Sprintf("environmentId: %q", item.Params["environmentId"]))

	// startDate (required)
	startVar := "startDate_" + alias
	decls = append(decls, fmt.Sprintf("$%s: DateTime!", startVar))
	vars[startVar] = item.Params["startDate"]
	argParts = append(argParts, fmt.Sprintf("startDate: $%s", startVar))

	// endDate (required)
	endVar := "endDate_" + alias
	decls = append(decls, fmt.Sprintf("$%s: DateTime!", endVar))
	vars[endVar] = item.Params["endDate"]
	argParts = append(argParts, fmt.Sprintf("endDate: $%s", endVar))

	// stepSeconds (optional)
	if ss, ok := item.Params["stepSeconds"]; ok {
		ssVar := "stepSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", ssVar))
		vars[ssVar] = ss
		argParts = append(argParts, fmt.Sprintf("stepSeconds: $%s", ssVar))
	}

	return AliasFragment{
		Alias:           alias,
		Field:           "httpDurationMetrics",
		Args:            strings.Join(argParts, ", "),
		Selection:       railway.HttpDurationMetricsFieldBody,
		VarDecls:        decls,
		VarValues:       vars,
		Breadth:         BreadthHttpDurationMetrics,
		EstimatedPoints: estimateHttpMetricPoints(item.Params),
		Item:            item,
	}
}

func httpStatusMetricsFragment(item WorkItem) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_httpstatus"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	// serviceId and environmentId are literals
	argParts = append(argParts, fmt.Sprintf("serviceId: %q", item.Params["serviceId"]))
	argParts = append(argParts, fmt.Sprintf("environmentId: %q", item.Params["environmentId"]))

	// startDate (required)
	startVar := "startDate_" + alias
	decls = append(decls, fmt.Sprintf("$%s: DateTime!", startVar))
	vars[startVar] = item.Params["startDate"]
	argParts = append(argParts, fmt.Sprintf("startDate: $%s", startVar))

	// endDate (required)
	endVar := "endDate_" + alias
	decls = append(decls, fmt.Sprintf("$%s: DateTime!", endVar))
	vars[endVar] = item.Params["endDate"]
	argParts = append(argParts, fmt.Sprintf("endDate: $%s", endVar))

	// stepSeconds (optional)
	if ss, ok := item.Params["stepSeconds"]; ok {
		ssVar := "stepSeconds_" + alias
		decls = append(decls, fmt.Sprintf("$%s: Int", ssVar))
		vars[ssVar] = ss
		argParts = append(argParts, fmt.Sprintf("stepSeconds: $%s", ssVar))
	}

	return AliasFragment{
		Alias:           alias,
		Field:           "httpMetricsGroupedByStatus",
		Args:            strings.Join(argParts, ", "),
		Selection:       railway.HttpMetricsGroupedByStatusFieldBody,
		VarDecls:        decls,
		VarValues:       vars,
		Breadth:         BreadthHttpMetricsGroupedByStatus,
		EstimatedPoints: estimateHttpMetricPoints(item.Params),
		Item:            item,
	}
}

func usageFragment(item WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_usage"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	// projectId is a literal
	argParts = append(argParts, fmt.Sprintf("projectId: %q", item.AliasKey))

	// measurements (required)
	measVar := "measurements_" + alias
	decls = append(decls, fmt.Sprintf("$%s: [MetricMeasurement!]!", measVar))
	vars[measVar] = item.Params["measurements"]
	argParts = append(argParts, fmt.Sprintf("measurements: $%s", measVar))

	// startDate (optional)
	if sd, ok := item.Params["startDate"]; ok {
		sdVar := "startDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: DateTime", sdVar))
		vars[sdVar] = sd
		argParts = append(argParts, fmt.Sprintf("startDate: $%s", sdVar))
	}

	// endDate (optional)
	if ed, ok := item.Params["endDate"]; ok {
		edVar := "endDate_" + alias
		decls = append(decls, fmt.Sprintf("$%s: DateTime", edVar))
		vars[edVar] = ed
		argParts = append(argParts, fmt.Sprintf("endDate: $%s", edVar))
	}

	// groupBy (optional)
	if gb, ok := item.Params["groupBy"]; ok {
		gbVar := "groupBy_" + alias
		decls = append(decls, fmt.Sprintf("$%s: [MetricTag!]", gbVar))
		vars[gbVar] = gb
		argParts = append(argParts, fmt.Sprintf("groupBy: $%s", gbVar))
	}

	return AliasFragment{
		Alias:     alias,
		Field:     "usage",
		Args:      strings.Join(argParts, ", "),
		Selection: railway.UsageFieldBody,
		VarDecls:  decls,
		VarValues: vars,
		Breadth:   BreadthUsage,
		Item:      item,
	}
}

func estimatedUsageFragment(item WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_estusage"
	vars := make(map[string]any)
	var decls []string
	var argParts []string

	// projectId is a literal
	argParts = append(argParts, fmt.Sprintf("projectId: %q", item.AliasKey))

	// measurements (required)
	measVar := "measurements_" + alias
	decls = append(decls, fmt.Sprintf("$%s: [MetricMeasurement!]!", measVar))
	vars[measVar] = item.Params["measurements"]
	argParts = append(argParts, fmt.Sprintf("measurements: $%s", measVar))

	return AliasFragment{
		Alias:     alias,
		Field:     "estimatedUsage",
		Args:      strings.Join(argParts, ", "),
		Selection: railway.EstimatedUsageFieldBody,
		VarDecls:  decls,
		VarValues: vars,
		Breadth:   BreadthEstimatedUsage,
		Item:      item,
	}
}

// SortByPriority sorts fragments by TaskType priority (Metrics first, then Logs,
// then Discovery). Within the same type, order is preserved.
func SortByPriority(fragments []AliasFragment) {
	sort.SliceStable(fragments, func(i, j int) bool {
		return fragments[i].Item.TaskType < fragments[j].Item.TaskType
	})
}

// DispatchRequestResults delivers per-alias results from a RawQueryResponse
// back to the originating generators. Uses fragment aliases directly.
func DispatchRequestResults(
	ctx context.Context,
	req Request,
	resp *railway.RawQueryResponse,
	queryErr error,
	generatorMap map[string]TaskGenerator,
) {
	// Check for request-level errors (empty data with errors = total failure).
	if queryErr == nil && resp != nil && len(resp.Data) == 0 && len(resp.Errors) > 0 {
		msgs := make([]string, len(resp.Errors))
		for i, e := range resp.Errors {
			msgs[i] = e.Message
		}
		queryErr = fmt.Errorf("GraphQL request failed: %s", strings.Join(msgs, "; "))
	}

	// Check for breadth-exceeded errors (indicates a packer bug)
	if resp != nil {
		for _, gqlErr := range resp.Errors {
			if strings.Contains(strings.ToLower(gqlErr.Message), "breadth") {
				// Log at ERROR -- this means our breadth calculation is wrong
				slog.Error("breadth limit exceeded (packer bug)",
					"message", gqlErr.Message,
					"computed_breadth", req.Breadth,
					"max_breadth", MaxBreadth,
					"alias_count", len(req.Fragments),
				)
			}
		}
	}

	for _, frag := range req.Fragments {
		gen, ok := generatorMap[frag.Item.ID]
		if !ok {
			continue
		}

		if queryErr != nil {
			gen.Deliver(ctx, frag.Item, nil, queryErr)
			continue
		}

		alias := frag.Alias

		data, exists := resp.Data[alias]
		if !exists {
			var aliasErr error
			for _, gqlErr := range resp.Errors {
				if len(gqlErr.Path) > 0 && gqlErr.Path[0] == alias {
					aliasErr = fmt.Errorf("GraphQL error: %s", gqlErr.Message)
					break
				}
			}
			if aliasErr != nil {
				gen.Deliver(ctx, frag.Item, nil, aliasErr)
			} else {
				gen.Deliver(ctx, frag.Item, nil, fmt.Errorf("alias %q not found in response", alias))
			}
			continue
		}

		if string(data) == "null" {
			var aliasErr error
			for _, gqlErr := range resp.Errors {
				if len(gqlErr.Path) > 0 && gqlErr.Path[0] == alias {
					aliasErr = fmt.Errorf("GraphQL error: %s", gqlErr.Message)
					break
				}
			}
			if aliasErr != nil {
				gen.Deliver(ctx, frag.Item, nil, aliasErr)
				continue
			}
		}

		gen.Deliver(ctx, frag.Item, json.RawMessage(data), nil)
	}
}
