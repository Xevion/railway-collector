package collector

import (
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

var prometheusNameMap = map[railway.MetricMeasurement]string{
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

func copyLabels(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
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
