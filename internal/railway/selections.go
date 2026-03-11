package railway

import (
	"strings"
)

// MetricsFieldBody is the selection set shared by all metrics aliases.
const MetricsFieldBody = `{
      measurement
      tags {
        projectId
        serviceId
        environmentId
        deploymentId
        deploymentInstanceId
        region
        volumeId
        volumeInstanceId
      }
      values {
        ts
        value
      }
    }`

// DeploymentLogsFieldBody is the selection set for deployment runtime logs.
const DeploymentLogsFieldBody = `{
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
const BuildLogsFieldBody = DeploymentLogsFieldBody

// HttpLogsFieldBody is the selection set for HTTP access logs.
const HttpLogsFieldBody = `{
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

// EnvironmentLogsFieldBody is the selection set for environment logs.
const EnvironmentLogsFieldBody = DeploymentLogsFieldBody

// ReplicaMetricsFieldBody is the selection set for replica metrics.
const ReplicaMetricsFieldBody = `{
      measurement
      replicaName
      values {
        ts
        value
      }
    }`

// HttpDurationMetricsFieldBody is the selection set for HTTP duration metrics.
const HttpDurationMetricsFieldBody = `{
      samples {
        ts
        p50
        p90
        p95
        p99
      }
    }`

// HttpMetricsGroupedByStatusFieldBody is the selection set for HTTP metrics grouped by status.
const HttpMetricsGroupedByStatusFieldBody = `{
      statusCode
      samples {
        ts
        value
      }
    }`

// UsageFieldBody is the selection set for aggregated usage.
const UsageFieldBody = `{
      measurement
      value
      tags {
        projectId
        serviceId
        environmentId
      }
    }`

// EstimatedUsageFieldBody is the selection set for estimated usage.
const EstimatedUsageFieldBody = `{
      estimatedValue
      measurement
      projectId
    }`

// SanitizeAlias converts an ID (which may contain hyphens) into a valid
// GraphQL alias. GraphQL aliases must match [_A-Za-z][_0-9A-Za-z]*.
func SanitizeAlias(id string) string {
	return "p_" + strings.ReplaceAll(id, "-", "_")
}
