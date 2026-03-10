package railway

import (
	"fmt"
	"strings"
)

// metricsFieldBody is the selection set shared by all metrics aliases.
const metricsFieldBody = `{
      measurement
      tags {
        projectId
        serviceId
        environmentId
        deploymentId
        deploymentInstanceId
        region
      }
      values {
        ts
        value
      }
    }`

// deploymentLogsFieldBody is the selection set for deployment runtime logs.
const deploymentLogsFieldBody = `{
      timestamp
      message
      severity
      attributes { key value }
      tags {
        deploymentId
        deploymentInstanceId
        serviceId
        projectId
        environmentId
      }
    }`

// buildLogsFieldBody is the selection set for build logs.
const buildLogsFieldBody = deploymentLogsFieldBody

// httpLogsFieldBody is the selection set for HTTP access logs.
const httpLogsFieldBody = `{
      timestamp
      requestId
      method
      path
      host
      httpStatus
      totalDuration
      upstreamRqDuration
      srcIp
      clientUa
      rxBytes
      txBytes
      edgeRegion
      deploymentId
      deploymentInstanceId
      downstreamProto
      upstreamProto
      upstreamAddress
      responseDetails
      upstreamErrors
    }`

// environmentLogsFieldBody is the selection set for environment logs.
const environmentLogsFieldBody = deploymentLogsFieldBody

// BuildBatchMetricsQuery constructs a GraphQL query that fetches metrics for
// multiple projects in a single request using aliases.
//
// Each projectID gets its own aliased `metrics(...)` call with shared parameters
// passed as variables. Returns the query string and variables map.
func BuildBatchMetricsQuery(
	projectIDs []string,
	measurements []MetricMeasurement,
	groupBy []MetricTag,
	startDate string,
	endDate *string,
	sampleRateSeconds *int,
	averagingWindowSeconds *int,
) (query string, variables map[string]any) {
	variables = map[string]any{
		"startDate":    startDate,
		"measurements": measurements,
	}
	if endDate != nil {
		variables["endDate"] = *endDate
	}
	if len(groupBy) > 0 {
		variables["groupBy"] = groupBy
	}
	if sampleRateSeconds != nil {
		variables["sampleRateSeconds"] = *sampleRateSeconds
	}
	if averagingWindowSeconds != nil {
		variables["averagingWindowSeconds"] = *averagingWindowSeconds
	}

	var varDecls []string
	varDecls = append(varDecls, "$startDate: DateTime!")
	varDecls = append(varDecls, "$measurements: [MetricMeasurement!]!")
	if endDate != nil {
		varDecls = append(varDecls, "$endDate: DateTime")
	}
	if len(groupBy) > 0 {
		varDecls = append(varDecls, "$groupBy: [MetricTag!]")
	}
	if sampleRateSeconds != nil {
		varDecls = append(varDecls, "$sampleRateSeconds: Int")
	}
	if averagingWindowSeconds != nil {
		varDecls = append(varDecls, "$averagingWindowSeconds: Int")
	}

	var aliases []string
	for _, pid := range projectIDs {
		alias := SanitizeAlias(pid)
		args := fmt.Sprintf(
			`projectId: %q, startDate: $startDate, measurements: $measurements`,
			pid,
		)
		if endDate != nil {
			args += ", endDate: $endDate"
		}
		if len(groupBy) > 0 {
			args += ", groupBy: $groupBy"
		}
		if sampleRateSeconds != nil {
			args += ", sampleRateSeconds: $sampleRateSeconds"
		}
		if averagingWindowSeconds != nil {
			args += ", averagingWindowSeconds: $averagingWindowSeconds"
		}
		aliases = append(aliases, fmt.Sprintf("    %s: metrics(%s) %s", alias, args, metricsFieldBody))
	}

	query = fmt.Sprintf("query BatchMetrics(%s) {\n%s\n}", strings.Join(varDecls, ", "), strings.Join(aliases, "\n"))
	return query, variables
}

// BuildBatchDeploymentLogsQuery constructs a query fetching deployment logs
// for multiple deployments using aliases. buildLogs and httpLogs for the same
// deployment can be combined by passing both sets.
type DeploymentLogRequest struct {
	DeploymentID string
	// Which log types to include in this alias group
	IncludeDeploymentLogs bool
	IncludeBuildLogs      bool
	IncludeHttpLogs       bool
}

// BuildBatchDeploymentLogsQuery builds a batched query for deployment-level logs.
func BuildBatchDeploymentLogsQuery(
	requests []DeploymentLogRequest,
	limit *int,
	startDate, endDate *string,
) (query string, variables map[string]any) {
	variables = make(map[string]any)
	if limit != nil {
		variables["limit"] = *limit
	}
	if startDate != nil {
		variables["startDate"] = *startDate
	}
	if endDate != nil {
		variables["endDate"] = *endDate
	}

	var varDecls []string
	varDecls = append(varDecls, "$limit: Int")
	varDecls = append(varDecls, "$startDate: DateTime")
	varDecls = append(varDecls, "$endDate: DateTime")
	// httpLogs uses String type for dates, not DateTime
	varDecls = append(varDecls, "$startDateStr: String")
	varDecls = append(varDecls, "$endDateStr: String")

	// Pass string versions of dates for httpLogs
	if startDate != nil {
		variables["startDateStr"] = *startDate
	}
	if endDate != nil {
		variables["endDateStr"] = *endDate
	}

	var aliases []string
	for _, req := range requests {
		alias := SanitizeAlias(req.DeploymentID)
		if req.IncludeDeploymentLogs {
			args := fmt.Sprintf(`deploymentId: %q, limit: $limit, startDate: $startDate, endDate: $endDate`, req.DeploymentID)
			aliases = append(aliases, fmt.Sprintf("    %s_deploy: deploymentLogs(%s) %s", alias, args, deploymentLogsFieldBody))
		}
		if req.IncludeBuildLogs {
			args := fmt.Sprintf(`deploymentId: %q, limit: $limit, startDate: $startDate, endDate: $endDate`, req.DeploymentID)
			aliases = append(aliases, fmt.Sprintf("    %s_build: buildLogs(%s) %s", alias, args, buildLogsFieldBody))
		}
		if req.IncludeHttpLogs {
			args := fmt.Sprintf(`deploymentId: %q, limit: $limit, startDate: $startDateStr, endDate: $endDateStr`, req.DeploymentID)
			aliases = append(aliases, fmt.Sprintf("    %s_http: httpLogs(%s) %s", alias, args, httpLogsFieldBody))
		}
	}

	query = fmt.Sprintf("query BatchDeploymentLogs(%s) {\n%s\n}", strings.Join(varDecls, ", "), strings.Join(aliases, "\n"))
	return query, variables
}

// BuildBatchEnvironmentLogsQuery constructs a query fetching environment logs
// for multiple environments using aliases.
func BuildBatchEnvironmentLogsQuery(
	environmentIDs []string,
	afterDate, beforeDate *string,
	afterLimit, beforeLimit *int,
) (query string, variables map[string]any) {
	variables = make(map[string]any)
	if afterDate != nil {
		variables["afterDate"] = *afterDate
	}
	if beforeDate != nil {
		variables["beforeDate"] = *beforeDate
	}
	if afterLimit != nil {
		variables["afterLimit"] = *afterLimit
	}
	if beforeLimit != nil {
		variables["beforeLimit"] = *beforeLimit
	}

	var varDecls []string
	varDecls = append(varDecls, "$afterDate: String")
	varDecls = append(varDecls, "$beforeDate: String")
	varDecls = append(varDecls, "$afterLimit: Int")
	varDecls = append(varDecls, "$beforeLimit: Int")

	var aliases []string
	for _, envID := range environmentIDs {
		alias := SanitizeAlias(envID)
		args := fmt.Sprintf(
			`environmentId: %q, afterDate: $afterDate, beforeDate: $beforeDate, afterLimit: $afterLimit, beforeLimit: $beforeLimit`,
			envID,
		)
		aliases = append(aliases, fmt.Sprintf("    %s: environmentLogs(%s) %s", alias, args, environmentLogsFieldBody))
	}

	query = fmt.Sprintf("query BatchEnvironmentLogs(%s) {\n%s\n}", strings.Join(varDecls, ", "), strings.Join(aliases, "\n"))
	return query, variables
}

// SanitizeAlias converts an ID (which may contain hyphens) into a valid
// GraphQL alias. GraphQL aliases must match [_A-Za-z][_0-9A-Za-z]*.
func SanitizeAlias(id string) string {
	return "p_" + strings.ReplaceAll(id, "-", "_")
}
