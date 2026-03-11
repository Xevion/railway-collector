package collector

import (
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector/loader"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/logging"
	"github.com/xevion/railway-collector/internal/railway"
)

// Test_measurementToMetricName_KnownMeasurements verifies that all known
// measurement enum values map to the expected canonical metric names.
func Test_measurementToMetricName_KnownMeasurements(t *testing.T) {
	cases := []struct {
		measurement string
		wantName    string
	}{
		{"CPU_USAGE", "railway_cpu_usage_cores"},
		{"CPU_LIMIT", "railway_cpu_limit_cores"},
		{"MEMORY_USAGE_GB", "railway_memory_usage_gb"},
		{"MEMORY_LIMIT_GB", "railway_memory_limit_gb"},
		{"NETWORK_RX_GB", "railway_network_rx_gb"},
		{"NETWORK_TX_GB", "railway_network_tx_gb"},
		{"DISK_USAGE_GB", "railway_disk_usage_gb"},
		{"EPHEMERAL_DISK_USAGE_GB", "railway_ephemeral_disk_usage_gb"},
		{"BACKUP_USAGE_GB", "railway_backup_usage_gb"},
	}

	for _, tc := range cases {
		t.Run(tc.measurement, func(t *testing.T) {
			got := measurementToMetricName(tc.measurement)
			assert.Equal(t, tc.wantName, got)
		})
	}
}

// Test_measurementToMetricName_UnknownFallback verifies that an unknown
// measurement string falls back to "railway_" + lowercase of the input.
func Test_measurementToMetricName_UnknownFallback(t *testing.T) {
	got := measurementToMetricName("SOME_UNKNOWN_METRIC")
	assert.Equal(t, "railway_some_unknown_metric", got)
}

// Test_usageMetricName_KnownSuffixes verifies that known measurement suffixes
// produce clean, non-redundant metric names.
func Test_usageMetricName_KnownSuffixes(t *testing.T) {
	cases := []struct {
		prefix      string
		measurement string
		wantName    string
	}{
		{"railway_usage", "CPU_USAGE", "railway_usage_cpu"},
		{"railway_usage", "CPU_LIMIT", "railway_usage_cpu_limit"},
		{"railway_usage", "MEMORY_USAGE_GB", "railway_usage_memory_gb"},
		{"railway_usage", "MEMORY_LIMIT_GB", "railway_usage_memory_limit_gb"},
		{"railway_usage", "NETWORK_RX_GB", "railway_usage_network_rx_gb"},
		{"railway_usage", "NETWORK_TX_GB", "railway_usage_network_tx_gb"},
		{"railway_usage", "DISK_USAGE_GB", "railway_usage_disk_gb"},
		{"railway_usage", "EPHEMERAL_DISK_USAGE_GB", "railway_usage_ephemeral_disk_gb"},
		{"railway_usage", "BACKUP_USAGE_GB", "railway_usage_backup_gb"},
		{"railway_estimated_usage", "CPU_USAGE", "railway_estimated_usage_cpu"},
		{"railway_estimated_usage", "DISK_USAGE_GB", "railway_estimated_usage_disk_gb"},
	}

	for _, tc := range cases {
		t.Run(tc.prefix+"/"+tc.measurement, func(t *testing.T) {
			got := usageMetricName(tc.prefix, tc.measurement)
			assert.Equal(t, tc.wantName, got)
		})
	}
}

// Test_usageMetricName_UnknownFallback verifies that an unknown measurement
// falls back to prefix + "_" + lowercase(measurement).
func Test_usageMetricName_UnknownFallback(t *testing.T) {
	got := usageMetricName("railway_usage", "SOME_UNKNOWN")
	assert.Equal(t, "railway_usage_some_unknown", got)
}

// Test_parseCompositeKey consolidates all parseCompositeKey cases into a single
// table-driven test. Each row verifies a distinct aspect of key splitting and
// target lookup.
func Test_parseCompositeKey(t *testing.T) {
	baseTargets := []types.ServiceTarget{
		{
			ServiceID:       "svc-1",
			EnvironmentID:   "env-1",
			ServiceName:     "web",
			EnvironmentName: "production",
			ProjectName:     "my-project",
			ProjectID:       "proj-1",
		},
	}

	tests := []struct {
		name    string
		input   string
		targets []types.ServiceTarget
		wantKey compositeKeyInfo
	}{
		{
			name:    "FullMatch",
			input:   "svc-1:env-1",
			targets: baseTargets,
			wantKey: compositeKeyInfo{
				serviceID:       "svc-1",
				environmentID:   "env-1",
				serviceName:     "web",
				environmentName: "production",
				projectName:     "my-project",
				projectID:       "proj-1",
			},
		},
		{
			name:  "NoMatch",
			input: "svc-1:env-1",
			targets: []types.ServiceTarget{
				{ServiceID: "svc-2", EnvironmentID: "env-2"},
			},
			wantKey: compositeKeyInfo{
				serviceID:     "svc-1",
				environmentID: "env-1",
			},
		},
		{
			name:    "NoColon",
			input:   "svc-1",
			targets: nil,
			wantKey: compositeKeyInfo{
				serviceID: "svc-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCompositeKey(tt.input, tt.targets)
			assert.Equal(t, tt.wantKey.serviceID, got.serviceID)
			assert.Equal(t, tt.wantKey.environmentID, got.environmentID)
			assert.Equal(t, tt.wantKey.serviceName, got.serviceName)
			assert.Equal(t, tt.wantKey.environmentName, got.environmentName)
			assert.Equal(t, tt.wantKey.projectName, got.projectName)
			assert.Equal(t, tt.wantKey.projectID, got.projectID)
		})
	}
}

// Test_deploymentBaseLabels consolidates deployment label lookup cases into a
// single table-driven test. The checks callback performs per-case assertions.
func Test_deploymentBaseLabels(t *testing.T) {
	tests := []struct {
		name         string
		deploymentID string
		targets      []types.ServiceTarget
		checks       func(t *testing.T, labels map[string]string)
	}{
		{
			name:         "Found",
			deploymentID: "dep-1",
			targets: []types.ServiceTarget{
				{
					DeploymentID:    "dep-1",
					ProjectID:       "proj-1",
					ProjectName:     "my-project",
					ServiceID:       "svc-1",
					ServiceName:     "web",
					EnvironmentID:   "env-1",
					EnvironmentName: "production",
				},
			},
			checks: func(t *testing.T, labels map[string]string) {
				t.Helper()
				assert.Equal(t, "proj-1", labels["project_id"])
				assert.Equal(t, "my-project", labels["project_name"])
				assert.Equal(t, "svc-1", labels["service_id"])
				assert.Equal(t, "web", labels["service_name"])
				assert.Equal(t, "env-1", labels["environment_id"])
				assert.Equal(t, "production", labels["environment_name"])
				assert.Equal(t, "dep-1", labels["deployment_id"])
			},
		},
		{
			name:         "NotFound",
			deploymentID: "dep-1",
			targets: []types.ServiceTarget{
				{DeploymentID: "dep-other"},
			},
			checks: func(t *testing.T, labels map[string]string) {
				t.Helper()
				require.Len(t, labels, 1)
				assert.Equal(t, "dep-1", labels["deployment_id"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := deploymentBaseLabels(tt.deploymentID, tt.targets)
			tt.checks(t, labels)
		})
	}
}

// TestResolveMeasurements_ValidNames verifies that known names map to the
// correct MetricMeasurement enum values.
func TestResolveMeasurements_ValidNames(t *testing.T) {
	result := ResolveMeasurements([]string{"cpu", "memory", "disk"})

	require.Len(t, result, 3)
	assert.Equal(t, railway.MetricMeasurementCpuUsage, result[0])
	assert.Equal(t, railway.MetricMeasurementMemoryUsageGb, result[1])
	assert.Equal(t, railway.MetricMeasurementDiskUsageGb, result[2])
}

// TestResolveMeasurements_SkipsUnknown verifies that unknown names are silently
// skipped without error.
func TestResolveMeasurements_SkipsUnknown(t *testing.T) {
	result := ResolveMeasurements([]string{"cpu", "totally_unknown", "memory"})

	require.Len(t, result, 2)
	assert.Equal(t, railway.MetricMeasurementCpuUsage, result[0])
	assert.Equal(t, railway.MetricMeasurementMemoryUsageGb, result[1])
}

// TestResolveMeasurements_EmptyInput verifies that an empty input returns nil.
func TestResolveMeasurements_EmptyInput(t *testing.T) {
	result := ResolveMeasurements([]string{})
	assert.Nil(t, result)
}

// TestResolveMeasurements_AllNames verifies that all 9 known measurement names
// resolve correctly.
func TestResolveMeasurements_AllNames(t *testing.T) {
	allNames := []string{
		"cpu", "cpu_limit", "memory", "memory_limit",
		"network_rx", "network_tx", "disk", "ephemeral_disk", "backup",
	}
	expected := []railway.MetricMeasurement{
		railway.MetricMeasurementCpuUsage,
		railway.MetricMeasurementCpuLimit,
		railway.MetricMeasurementMemoryUsageGb,
		railway.MetricMeasurementMemoryLimitGb,
		railway.MetricMeasurementNetworkRxGb,
		railway.MetricMeasurementNetworkTxGb,
		railway.MetricMeasurementDiskUsageGb,
		railway.MetricMeasurementEphemeralDiskUsageGb,
		railway.MetricMeasurementBackupUsageGb,
	}

	result := ResolveMeasurements(allNames)
	require.Len(t, result, len(expected))
	assert.Equal(t, expected, result)
}

// Test_uniqueProjectIDs_Deduplicates verifies that duplicate project IDs are
// collapsed and order of first appearance is preserved.
func Test_uniqueProjectIDs_Deduplicates(t *testing.T) {
	targets := []types.ServiceTarget{
		{ProjectID: "proj-1"},
		{ProjectID: "proj-2"},
		{ProjectID: "proj-1"},
		{ProjectID: "proj-3"},
		{ProjectID: "proj-2"},
	}

	ids := uniqueProjectIDs(targets)

	require.Len(t, ids, 3)
	assert.Equal(t, []string{"proj-1", "proj-2", "proj-3"}, ids)
}

// Test_uniqueProjectIDs_Empty verifies that an empty input returns nil.
func Test_uniqueProjectIDs_Empty(t *testing.T) {
	ids := uniqueProjectIDs(nil)
	assert.Nil(t, ids)
}

// Test_uniqueServiceEnvironments_Deduplicates verifies that duplicate
// (serviceID, environmentID) pairs are collapsed, keeping first appearance.
func Test_uniqueServiceEnvironments_Deduplicates(t *testing.T) {
	targets := []types.ServiceTarget{
		{ServiceID: "svc-1", EnvironmentID: "env-1", ServiceName: "first"},
		{ServiceID: "svc-2", EnvironmentID: "env-1", ServiceName: "second"},
		{ServiceID: "svc-1", EnvironmentID: "env-1", ServiceName: "duplicate"},
		{ServiceID: "svc-1", EnvironmentID: "env-2", ServiceName: "third"},
	}

	result := uniqueServiceEnvironments(targets)

	require.Len(t, result, 3)
	assert.Equal(t, "first", result[0].ServiceName)
	assert.Equal(t, "second", result[1].ServiceName)
	assert.Equal(t, "third", result[2].ServiceName)
}

// Test_uniqueServiceEnvironments_Empty verifies that a nil input returns nil.
func Test_uniqueServiceEnvironments_Empty(t *testing.T) {
	result := uniqueServiceEnvironments(nil)
	assert.Nil(t, result)
}

func strPtrHelper(s string) *string { return &s }

// Test_buildMetricLabels consolidates all buildMetricLabels cases into a single
// table-driven test. The checks callback performs per-case assertions.
func Test_buildMetricLabels(t *testing.T) {
	tests := []struct {
		name    string
		tags    rawMetricsTags
		targets []types.ServiceTarget
		checks  func(t *testing.T, labels map[string]string)
	}{
		{
			name: "FullTags",
			tags: rawMetricsTags{
				ProjectID:            strPtrHelper("proj-1"),
				ServiceID:            strPtrHelper("svc-1"),
				EnvironmentID:        strPtrHelper("env-1"),
				DeploymentID:         strPtrHelper("dep-1"),
				DeploymentInstanceID: strPtrHelper("inst-1"),
				Region:               strPtrHelper("us-west2"),
				VolumeID:             strPtrHelper("vol-1"),
				VolumeInstanceID:     strPtrHelper("volinst-1"),
			},
			targets: []types.ServiceTarget{
				{
					ServiceID:       "svc-1",
					EnvironmentID:   "env-1",
					ProjectName:     "my-project",
					ServiceName:     "web",
					EnvironmentName: "production",
				},
			},
			checks: func(t *testing.T, labels map[string]string) {
				t.Helper()
				assert.Equal(t, "proj-1", labels["project_id"])
				assert.Equal(t, "svc-1", labels["service_id"])
				assert.Equal(t, "env-1", labels["environment_id"])
				assert.Equal(t, "dep-1", labels["deployment_id"])
				assert.Equal(t, "inst-1", labels["deployment_instance_id"])
				assert.Equal(t, "us-west2", labels["region"])
				assert.Equal(t, "vol-1", labels["volume_id"])
				assert.Equal(t, "volinst-1", labels["volume_instance_id"])
				assert.Equal(t, "my-project", labels["project_name"])
				assert.Equal(t, "web", labels["service_name"])
				assert.Equal(t, "production", labels["environment_name"])
			},
		},
		{
			name:    "NilTags",
			tags:    rawMetricsTags{},
			targets: nil,
			checks: func(t *testing.T, labels map[string]string) {
				t.Helper()
				assert.Empty(t, labels)
			},
		},
		{
			name: "NoTargetMatch",
			tags: rawMetricsTags{
				ServiceID:     strPtrHelper("svc-unknown"),
				EnvironmentID: strPtrHelper("env-unknown"),
			},
			targets: []types.ServiceTarget{
				{
					ServiceID:       "svc-1",
					EnvironmentID:   "env-1",
					ServiceName:     "web",
					EnvironmentName: "production",
					ProjectName:     "my-project",
				},
			},
			checks: func(t *testing.T, labels map[string]string) {
				t.Helper()
				assert.Equal(t, "svc-unknown", labels["service_id"])
				assert.Equal(t, "env-unknown", labels["environment_id"])
				assert.NotContains(t, labels, "service_name")
				assert.NotContains(t, labels, "environment_name")
				assert.NotContains(t, labels, "project_name")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			labels := buildMetricLabels(tt.tags, tt.targets)
			tt.checks(t, labels)
		})
	}
}

// Test_metricsBatchKey verifies the format of batch keys produced for live-edge
// (open-ended) metric queries. The key encodes sample rate, average window, and
// measurement list so that compatible queries can be aliased together.
func Test_metricsBatchKey(t *testing.T) {
	tests := []struct {
		name         string
		measurements []railway.MetricMeasurement
		sampleRate   int
		avgWindow    int
		want         string
	}{
		{
			name:         "single measurement",
			measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			sampleRate:   60,
			avgWindow:    5,
			want:         "sr=60,aw=5,m=CPU_USAGE",
		},
		{
			name: "multiple measurements",
			measurements: []railway.MetricMeasurement{
				railway.MetricMeasurementCpuUsage,
				railway.MetricMeasurementMemoryUsageGb,
			},
			sampleRate: 30,
			avgWindow:  10,
			want:       "sr=30,aw=10,m=CPU_USAGE+MEMORY_USAGE_GB",
		},
		{
			name:         "zero sample rate and window",
			measurements: []railway.MetricMeasurement{railway.MetricMeasurementDiskUsageGb},
			sampleRate:   0,
			avgWindow:    0,
			want:         "sr=0,aw=0,m=DISK_USAGE_GB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metricsBatchKey(tt.measurements, tt.sampleRate, tt.avgWindow)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Test_metricsBatchKeyChunk verifies the format of batch keys for chunked
// (older-gap) metric queries. Chunk boundaries are encoded in RFC3339 so that
// queries for the exact same window can be aliased together.
func Test_metricsBatchKeyChunk(t *testing.T) {
	t0 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(6 * time.Hour)
	t2 := t0.Add(12 * time.Hour)

	tests := []struct {
		name         string
		measurements []railway.MetricMeasurement
		sampleRate   int
		avgWindow    int
		chunkStart   time.Time
		chunkEnd     time.Time
		want         string
	}{
		{
			name:         "single measurement with chunk",
			measurements: []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			sampleRate:   60,
			avgWindow:    5,
			chunkStart:   t0,
			chunkEnd:     t1,
			want:         "sr=60,aw=5,m=CPU_USAGE,s=2024-01-15T00:00:00Z,e=2024-01-15T06:00:00Z",
		},
		{
			name: "multiple measurements with chunk",
			measurements: []railway.MetricMeasurement{
				railway.MetricMeasurementCpuUsage,
				railway.MetricMeasurementMemoryUsageGb,
			},
			sampleRate: 30,
			avgWindow:  10,
			chunkStart: t1,
			chunkEnd:   t2,
			want:       "sr=30,aw=10,m=CPU_USAGE+MEMORY_USAGE_GB,s=2024-01-15T06:00:00Z,e=2024-01-15T12:00:00Z",
		},
		{
			name:         "zero rates with chunk boundaries",
			measurements: []railway.MetricMeasurement{railway.MetricMeasurementNetworkRxGb},
			sampleRate:   0,
			avgWindow:    0,
			chunkStart:   t0,
			chunkEnd:     t2,
			want:         "sr=0,aw=0,m=NETWORK_RX_GB,s=2024-01-15T00:00:00Z,e=2024-01-15T12:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := metricsBatchKeyChunk(tt.measurements, tt.sampleRate, tt.avgWindow, tt.chunkStart, tt.chunkEnd)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestKindBreakdown verifies that kindBreakdown produces a sorted,
// space-separated "kind:count" summary of fragment counts by QueryKind.
func TestKindBreakdown(t *testing.T) {
	makeItem := func(kind types.QueryKind) loader.AliasFragment {
		return loader.AliasFragment{Item: types.WorkItem{Kind: kind}}
	}

	cases := []struct {
		name      string
		fragments []loader.AliasFragment
		want      string
	}{
		{
			name:      "empty slice",
			fragments: nil,
			want:      "",
		},
		{
			name: "single kind",
			fragments: []loader.AliasFragment{
				makeItem(types.QueryMetrics),
				makeItem(types.QueryMetrics),
				makeItem(types.QueryMetrics),
			},
			want: "metrics:3",
		},
		{
			name: "mixed kinds sorted",
			fragments: []loader.AliasFragment{
				makeItem(types.QueryMetrics),
				makeItem(types.QueryMetrics),
				makeItem(types.QueryEnvironmentLogs),
				makeItem(types.QueryServiceMetrics),
			},
			want: "environmentLogs:1 metrics:2 serviceMetrics:1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := kindBreakdown(tc.fragments)
			assert.Equal(t, tc.want, got)
		})
	}
}

// Test_deliveryLogLevel verifies that deliveryLogLevel returns LevelTrace for
// empty deliveries (count == 0) and LevelDebug for any non-zero count.
func Test_deliveryLogLevel(t *testing.T) {
	tests := []struct {
		name  string
		count int
		want  slog.Level
	}{
		{
			name:  "zero count returns trace",
			count: 0,
			want:  logging.LevelTrace,
		},
		{
			name:  "count of 1 returns debug",
			count: 1,
			want:  slog.LevelDebug,
		},
		{
			name:  "count at threshold returns debug",
			count: 10,
			want:  slog.LevelDebug,
		},
		{
			name:  "large count returns debug",
			count: 10000,
			want:  slog.LevelDebug,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := deliveryLogLevel(tt.count)
			assert.Equal(t, tt.want, got)
		})
	}
}
