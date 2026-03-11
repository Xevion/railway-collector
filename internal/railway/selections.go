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

// SanitizeAlias converts an ID (which may contain hyphens) into a valid
// GraphQL alias. GraphQL aliases must match [_A-Za-z][_0-9A-Za-z]*.
func SanitizeAlias(id string) string {
	return "p_" + strings.ReplaceAll(id, "-", "_")
}
