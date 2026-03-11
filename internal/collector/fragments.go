package collector

import (
	"fmt"
	"strings"
	"time"

	"github.com/xevion/railway-collector/internal/railway"
)

// sanitizeCompositeAlias wraps SanitizeAlias and additionally replaces colons
// with underscores, for composite keys like "serviceID:envID".
func sanitizeCompositeAlias(key string) string {
	return strings.ReplaceAll(railway.SanitizeAlias(key), ":", "_")
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
