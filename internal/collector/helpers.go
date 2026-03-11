package collector

import (
	"strings"

	"github.com/xevion/railway-collector/internal/railway"
)

var measurementMap = map[string]railway.MetricMeasurement{
	"cpu":            railway.MetricMeasurementCpuUsage,
	"cpu_limit":      railway.MetricMeasurementCpuLimit,
	"memory":         railway.MetricMeasurementMemoryUsageGb,
	"memory_limit":   railway.MetricMeasurementMemoryLimitGb,
	"network_rx":     railway.MetricMeasurementNetworkRxGb,
	"network_tx":     railway.MetricMeasurementNetworkTxGb,
	"disk":           railway.MetricMeasurementDiskUsageGb,
	"ephemeral_disk": railway.MetricMeasurementEphemeralDiskUsageGb,
	"backup":         railway.MetricMeasurementBackupUsageGb,
}

var metricNameMap = map[railway.MetricMeasurement]string{
	railway.MetricMeasurementCpuUsage:             "railway_cpu_usage_cores",
	railway.MetricMeasurementCpuLimit:             "railway_cpu_limit_cores",
	railway.MetricMeasurementMemoryUsageGb:        "railway_memory_usage_gb",
	railway.MetricMeasurementMemoryLimitGb:        "railway_memory_limit_gb",
	railway.MetricMeasurementNetworkRxGb:          "railway_network_rx_gb",
	railway.MetricMeasurementNetworkTxGb:          "railway_network_tx_gb",
	railway.MetricMeasurementDiskUsageGb:          "railway_disk_usage_gb",
	railway.MetricMeasurementEphemeralDiskUsageGb: "railway_ephemeral_disk_usage_gb",
	railway.MetricMeasurementBackupUsageGb:        "railway_backup_usage_gb",
}

// usageMetricSuffix maps measurement enum values to clean suffixes for usage
// metric names, avoiding redundancy like "railway_usage_cpu_usage".
var usageMetricSuffix = map[string]string{
	"CPU_USAGE":               "cpu",
	"CPU_LIMIT":               "cpu_limit",
	"MEMORY_USAGE_GB":         "memory_gb",
	"MEMORY_LIMIT_GB":         "memory_limit_gb",
	"NETWORK_RX_GB":           "network_rx_gb",
	"NETWORK_TX_GB":           "network_tx_gb",
	"DISK_USAGE_GB":           "disk_gb",
	"EPHEMERAL_DISK_USAGE_GB": "ephemeral_disk_gb",
	"BACKUP_USAGE_GB":         "backup_gb",
}

// usageMetricName returns a clean metric name for billing/usage measurements,
// using the given prefix (e.g. "railway_usage" or "railway_estimated_usage").
func usageMetricName(prefix, measurement string) string {
	if suffix, ok := usageMetricSuffix[measurement]; ok {
		return prefix + "_" + suffix
	}
	return prefix + "_" + strings.ToLower(measurement)
}

func uniqueProjectIDs(targets []ServiceTarget) []string {
	seen := map[string]bool{}
	var ids []string
	for _, t := range targets {
		if !seen[t.ProjectID] {
			seen[t.ProjectID] = true
			ids = append(ids, t.ProjectID)
		}
	}
	return ids
}

// uniqueServiceEnvironments returns deduplicated ServiceTargets by (serviceID, environmentID) pair.
func uniqueServiceEnvironments(targets []ServiceTarget) []ServiceTarget {
	type key struct{ svc, env string }
	seen := map[key]bool{}
	var result []ServiceTarget
	for _, t := range targets {
		k := key{t.ServiceID, t.EnvironmentID}
		if !seen[k] {
			seen[k] = true
			result = append(result, t)
		}
	}
	return result
}

func copyLabels(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// deploymentBaseLabels finds the ServiceTarget matching a deployment ID and
// returns a base label map. Falls back to just {"deployment_id": id}.
func deploymentBaseLabels(deploymentID string, targets []ServiceTarget) map[string]string {
	for _, t := range targets {
		if t.DeploymentID == deploymentID {
			return map[string]string{
				"project_id":       t.ProjectID,
				"project_name":     t.ProjectName,
				"service_id":       t.ServiceID,
				"service_name":     t.ServiceName,
				"environment_id":   t.EnvironmentID,
				"environment_name": t.EnvironmentName,
				"deployment_id":    t.DeploymentID,
			}
		}
	}
	return map[string]string{"deployment_id": deploymentID}
}

// environmentServiceLookup builds a map of serviceID → ServiceTarget for all
// targets in the given environment, also returning the environment name.
func environmentServiceLookup(envID string, targets []ServiceTarget) (services map[string]ServiceTarget, envName string) {
	services = make(map[string]ServiceTarget)
	for _, t := range targets {
		if t.EnvironmentID == envID {
			services[t.ServiceID] = t
			if envName == "" {
				envName = t.EnvironmentName
			}
		}
	}
	return
}

// ResolveMeasurements converts human-friendly measurement names (e.g. "cpu", "memory")
// to the Railway API enum values. Unknown names are skipped.
func ResolveMeasurements(names []string) []railway.MetricMeasurement {
	var result []railway.MetricMeasurement
	for _, name := range names {
		if m, ok := measurementMap[name]; ok {
			result = append(result, m)
		}
	}
	return result
}
