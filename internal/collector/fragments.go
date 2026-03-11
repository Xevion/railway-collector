package collector

import (
	"fmt"
	"strings"
	"time"

	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/railway"
)

// sanitizeCompositeAlias wraps SanitizeAlias and additionally replaces colons
// with underscores, for composite keys like "serviceID:envID".
func sanitizeCompositeAlias(key string) string {
	return strings.ReplaceAll(railway.SanitizeAlias(key), ":", "_")
}

// fragmentBuilder accumulates namespaced GraphQL variable declarations, values,
// and argument parts for a single aliased fragment. It eliminates the repetitive
// 4-line "create var name, append decl, set value, append arg" pattern.
type fragmentBuilder struct {
	alias    string
	params   map[string]any
	vars     map[string]any
	decls    []string
	argParts []string
}

func newFragmentBuilder(alias string, params map[string]any) *fragmentBuilder {
	return &fragmentBuilder{
		alias:  alias,
		params: params,
		vars:   make(map[string]any),
	}
}

// addLiteral adds an inline literal argument (not a variable), e.g. projectId: "abc".
func (b *fragmentBuilder) addLiteral(name, value string) {
	b.argParts = append(b.argParts, fmt.Sprintf("%s: %q", name, value))
}

// addVar adds a required namespaced variable with an explicit value.
func (b *fragmentBuilder) addVar(name, gqlType string, value any) {
	v := name + "_" + b.alias
	b.decls = append(b.decls, fmt.Sprintf("$%s: %s", v, gqlType))
	b.vars[v] = value
	b.argParts = append(b.argParts, fmt.Sprintf("%s: $%s", name, v))
}

// addOptionalVar adds a namespaced variable only if the param exists in b.params.
func (b *fragmentBuilder) addOptionalVar(name, gqlType string) {
	if val, ok := b.params[name]; ok {
		b.addVar(name, gqlType, val)
	}
}

// args joins all accumulated argument parts with ", ".
func (b *fragmentBuilder) args() string {
	return strings.Join(b.argParts, ", ")
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

// FragmentFromWorkItem converts a types.WorkItem into a self-contained AliasFragment
// with fully namespaced variables. The now parameter is used for estimating
// data points on open-ended queries (no endDate).
func FragmentFromWorkItem(item types.WorkItem, now time.Time) AliasFragment {
	nowFunc := func() time.Time { return now }
	switch item.Kind {
	case types.QueryMetrics:
		return metricsFragment(item, nowFunc)
	case types.QueryServiceMetrics:
		return serviceMetricsFragment(item, nowFunc)
	case types.QueryReplicaMetrics:
		return replicaMetricsFragment(item, nowFunc)
	case types.QueryHttpDurationMetrics:
		return httpDurationMetricsFragment(item)
	case types.QueryHttpMetricsGroupedByStatus:
		return httpStatusMetricsFragment(item)
	case types.QueryUsage:
		return usageFragment(item)
	case types.QueryEstimatedUsage:
		return estimatedUsageFragment(item)
	case types.QueryEnvironmentLogs:
		return envLogsFragment(item)
	case types.QueryBuildLogs:
		return buildLogsFragment(item)
	case types.QueryHttpLogs:
		return httpLogsFragment(item)
	default:
		// Discovery and unknown types produce empty fragments.
		return AliasFragment{Item: item}
	}
}

func metricsFragment(item types.WorkItem, nowFunc func() time.Time) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey)
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("projectId", item.AliasKey)
	b.addVar("startDate", "DateTime!", item.Params["startDate"])
	b.addOptionalVar("endDate", "DateTime")
	b.addVar("measurements", "[MetricMeasurement!]!", item.Params["measurements"])
	b.addOptionalVar("groupBy", "[MetricTag!]")
	b.addOptionalVar("sampleRateSeconds", "Int")
	b.addOptionalVar("averagingWindowSeconds", "Int")

	return AliasFragment{
		Alias:           alias,
		Field:           "metrics",
		Args:            b.args(),
		Selection:       railway.MetricsFieldBody,
		VarDecls:        b.decls,
		VarValues:       b.vars,
		Breadth:         BreadthMetrics,
		EstimatedPoints: estimateMetricPoints(item.Params, nowFunc),
		Item:            item,
	}
}

func envLogsFragment(item types.WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey)
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("environmentId", item.AliasKey)
	b.addOptionalVar("afterDate", "String")
	b.addOptionalVar("beforeDate", "String")
	b.addOptionalVar("afterLimit", "Int")

	return AliasFragment{
		Alias:     alias,
		Field:     "environmentLogs",
		Args:      b.args(),
		Selection: railway.EnvironmentLogsFieldBody,
		VarDecls:  b.decls,
		VarValues: b.vars,
		Breadth:   BreadthEnvironmentLogs,
		Item:      item,
	}
}

func buildLogsFragment(item types.WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_build"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("deploymentId", item.AliasKey)
	b.addOptionalVar("limit", "Int")
	b.addOptionalVar("startDate", "DateTime")

	return AliasFragment{
		Alias:     alias,
		Field:     "buildLogs",
		Args:      b.args(),
		Selection: railway.BuildLogsFieldBody,
		VarDecls:  b.decls,
		VarValues: b.vars,
		Breadth:   BreadthBuildLogs,
		Item:      item,
	}
}

func httpLogsFragment(item types.WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_http"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("deploymentId", item.AliasKey)
	b.addOptionalVar("limit", "Int")
	// httpLogs uses String type for dates, not DateTime
	b.addOptionalVar("startDate", "String")

	return AliasFragment{
		Alias:     alias,
		Field:     "httpLogs",
		Args:      b.args(),
		Selection: railway.HttpLogsFieldBody,
		VarDecls:  b.decls,
		VarValues: b.vars,
		Breadth:   BreadthHttpLogs,
		Item:      item,
	}
}

func serviceMetricsFragment(item types.WorkItem, nowFunc func() time.Time) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_svcmetrics"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("serviceId", item.Params["serviceId"].(string))
	b.addLiteral("environmentId", item.Params["environmentId"].(string))
	b.addVar("startDate", "DateTime!", item.Params["startDate"])
	b.addOptionalVar("endDate", "DateTime")
	b.addVar("measurements", "[MetricMeasurement!]!", item.Params["measurements"])
	b.addOptionalVar("groupBy", "[MetricTag!]")
	b.addOptionalVar("sampleRateSeconds", "Int")
	b.addOptionalVar("averagingWindowSeconds", "Int")

	return AliasFragment{
		Alias:           alias,
		Field:           "metrics",
		Args:            b.args(),
		Selection:       railway.MetricsFieldBody,
		VarDecls:        b.decls,
		VarValues:       b.vars,
		Breadth:         BreadthMetrics,
		EstimatedPoints: estimateMetricPoints(item.Params, nowFunc),
		Item:            item,
	}
}

func replicaMetricsFragment(item types.WorkItem, nowFunc func() time.Time) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_replica"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("serviceId", item.Params["serviceId"].(string))
	b.addLiteral("environmentId", item.Params["environmentId"].(string))
	b.addVar("startDate", "DateTime!", item.Params["startDate"])
	b.addOptionalVar("endDate", "DateTime")
	b.addVar("measurements", "[MetricMeasurement!]!", item.Params["measurements"])
	b.addOptionalVar("sampleRateSeconds", "Int")
	b.addOptionalVar("averagingWindowSeconds", "Int")

	return AliasFragment{
		Alias:           alias,
		Field:           "replicaMetrics",
		Args:            b.args(),
		Selection:       railway.ReplicaMetricsFieldBody,
		VarDecls:        b.decls,
		VarValues:       b.vars,
		Breadth:         BreadthReplicaMetrics,
		EstimatedPoints: estimateMetricPoints(item.Params, nowFunc),
		Item:            item,
	}
}

func httpDurationMetricsFragment(item types.WorkItem) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_httpdur"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("serviceId", item.Params["serviceId"].(string))
	b.addLiteral("environmentId", item.Params["environmentId"].(string))
	b.addVar("startDate", "DateTime!", item.Params["startDate"])
	b.addVar("endDate", "DateTime!", item.Params["endDate"])
	b.addOptionalVar("stepSeconds", "Int")

	return AliasFragment{
		Alias:           alias,
		Field:           "httpDurationMetrics",
		Args:            b.args(),
		Selection:       railway.HttpDurationMetricsFieldBody,
		VarDecls:        b.decls,
		VarValues:       b.vars,
		Breadth:         BreadthHttpDurationMetrics,
		EstimatedPoints: estimateHttpMetricPoints(item.Params),
		Item:            item,
	}
}

func httpStatusMetricsFragment(item types.WorkItem) AliasFragment {
	alias := sanitizeCompositeAlias(item.AliasKey) + "_httpstatus"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("serviceId", item.Params["serviceId"].(string))
	b.addLiteral("environmentId", item.Params["environmentId"].(string))
	b.addVar("startDate", "DateTime!", item.Params["startDate"])
	b.addVar("endDate", "DateTime!", item.Params["endDate"])
	b.addOptionalVar("stepSeconds", "Int")

	return AliasFragment{
		Alias:           alias,
		Field:           "httpMetricsGroupedByStatus",
		Args:            b.args(),
		Selection:       railway.HttpMetricsGroupedByStatusFieldBody,
		VarDecls:        b.decls,
		VarValues:       b.vars,
		Breadth:         BreadthHttpMetricsGroupedByStatus,
		EstimatedPoints: estimateHttpMetricPoints(item.Params),
		Item:            item,
	}
}

func usageFragment(item types.WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_usage"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("projectId", item.AliasKey)
	b.addVar("measurements", "[MetricMeasurement!]!", item.Params["measurements"])
	b.addOptionalVar("startDate", "DateTime")
	b.addOptionalVar("endDate", "DateTime")
	b.addOptionalVar("groupBy", "[MetricTag!]")

	return AliasFragment{
		Alias:     alias,
		Field:     "usage",
		Args:      b.args(),
		Selection: railway.UsageFieldBody,
		VarDecls:  b.decls,
		VarValues: b.vars,
		Breadth:   BreadthUsage,
		Item:      item,
	}
}

func estimatedUsageFragment(item types.WorkItem) AliasFragment {
	alias := railway.SanitizeAlias(item.AliasKey) + "_estusage"
	b := newFragmentBuilder(alias, item.Params)
	b.addLiteral("projectId", item.AliasKey)
	b.addVar("measurements", "[MetricMeasurement!]!", item.Params["measurements"])

	return AliasFragment{
		Alias:     alias,
		Field:     "estimatedUsage",
		Args:      b.args(),
		Selection: railway.EstimatedUsageFieldBody,
		VarDecls:  b.decls,
		VarValues: b.vars,
		Breadth:   BreadthEstimatedUsage,
		Item:      item,
	}
}
