package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector/coverage"
	"github.com/xevion/railway-collector/internal/collector/loader"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/logging"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/sink"
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

// Test_applyConfigDefaults verifies that applyConfigDefaults fills zero-valued
// fields with defaults and preserves non-zero values.
func Test_applyConfigDefaults(t *testing.T) {
	customMeasurements := []railway.MetricMeasurement{
		railway.MetricMeasurementCpuUsage,
		railway.MetricMeasurementMemoryUsageGb,
	}
	fallbackMeasurements := []railway.MetricMeasurement{
		railway.MetricMeasurementDiskUsageGb,
	}

	tests := []struct {
		name                string
		cfg                 BaseMetricsConfig
		defaults            []railway.MetricMeasurement
		wantMeasurements    []railway.MetricMeasurement
		wantRetention       time.Duration
		wantChunkSize       time.Duration
		wantMaxItemsPerPoll int
	}{
		{
			name:                "empty measurements uses defaults",
			cfg:                 BaseMetricsConfig{},
			defaults:            fallbackMeasurements,
			wantMeasurements:    fallbackMeasurements,
			wantRetention:       90 * 24 * time.Hour,
			wantChunkSize:       6 * time.Hour,
			wantMaxItemsPerPoll: 10,
		},
		{
			name: "non-empty measurements preserved",
			cfg: BaseMetricsConfig{
				Measurements: customMeasurements,
			},
			defaults:            fallbackMeasurements,
			wantMeasurements:    customMeasurements,
			wantRetention:       90 * 24 * time.Hour,
			wantChunkSize:       6 * time.Hour,
			wantMaxItemsPerPoll: 10,
		},
		{
			name: "non-zero fields preserved",
			cfg: BaseMetricsConfig{
				Measurements:    customMeasurements,
				MetricRetention: 7 * 24 * time.Hour,
				ChunkSize:       1 * time.Hour,
				MaxItemsPerPoll: 50,
			},
			defaults:            fallbackMeasurements,
			wantMeasurements:    customMeasurements,
			wantRetention:       7 * 24 * time.Hour,
			wantChunkSize:       1 * time.Hour,
			wantMaxItemsPerPoll: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			got := applyConfigDefaults(&cfg, tt.defaults)
			assert.Equal(t, tt.wantMeasurements, got)
			assert.Equal(t, tt.wantRetention, cfg.MetricRetention)
			assert.Equal(t, tt.wantChunkSize, cfg.ChunkSize)
			assert.Equal(t, tt.wantMaxItemsPerPoll, cfg.MaxItemsPerPoll)
		})
	}
}

// Test_environmentServiceLookup verifies service map construction and envName
// extraction from targets matching a given environment ID.
func Test_environmentServiceLookup(t *testing.T) {
	tests := []struct {
		name         string
		envID        string
		targets      []types.ServiceTarget
		wantServices map[string]types.ServiceTarget
		wantEnvName  string
	}{
		{
			name:  "no matching targets",
			envID: "env-missing",
			targets: []types.ServiceTarget{
				{ServiceID: "svc-1", EnvironmentID: "env-1", EnvironmentName: "production"},
			},
			wantServices: map[string]types.ServiceTarget{},
			wantEnvName:  "",
		},
		{
			name:  "single match",
			envID: "env-1",
			targets: []types.ServiceTarget{
				{ServiceID: "svc-1", EnvironmentID: "env-1", EnvironmentName: "production", ServiceName: "web"},
			},
			wantServices: map[string]types.ServiceTarget{
				"svc-1": {ServiceID: "svc-1", EnvironmentID: "env-1", EnvironmentName: "production", ServiceName: "web"},
			},
			wantEnvName: "production",
		},
		{
			name:  "multiple services same env, envName from first match",
			envID: "env-1",
			targets: []types.ServiceTarget{
				{ServiceID: "svc-1", EnvironmentID: "env-1", EnvironmentName: "production", ServiceName: "web"},
				{ServiceID: "svc-2", EnvironmentID: "env-1", EnvironmentName: "production", ServiceName: "api"},
				{ServiceID: "svc-3", EnvironmentID: "env-2", EnvironmentName: "staging", ServiceName: "worker"},
			},
			wantServices: map[string]types.ServiceTarget{
				"svc-1": {ServiceID: "svc-1", EnvironmentID: "env-1", EnvironmentName: "production", ServiceName: "web"},
				"svc-2": {ServiceID: "svc-2", EnvironmentID: "env-1", EnvironmentName: "production", ServiceName: "api"},
			},
			wantEnvName: "production",
		},
		{
			name:  "filters to only matching env",
			envID: "env-2",
			targets: []types.ServiceTarget{
				{ServiceID: "svc-1", EnvironmentID: "env-1", EnvironmentName: "production", ServiceName: "web"},
				{ServiceID: "svc-2", EnvironmentID: "env-2", EnvironmentName: "staging", ServiceName: "api"},
			},
			wantServices: map[string]types.ServiceTarget{
				"svc-2": {ServiceID: "svc-2", EnvironmentID: "env-2", EnvironmentName: "staging", ServiceName: "api"},
			},
			wantEnvName: "staging",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			services, envName := environmentServiceLookup(tt.envID, tt.targets)
			assert.Equal(t, tt.wantEnvName, envName)
			assert.Equal(t, tt.wantServices, services)
		})
	}
}

// Test_copyLabels verifies that copyLabels produces an independent copy of a
// label map: all entries are preserved, and mutations to the copy do not
// affect the original.
func Test_copyLabels(t *testing.T) {
	tests := []struct {
		name string
		src  map[string]string
		want map[string]string
	}{
		{
			name: "copies all entries",
			src:  map[string]string{"a": "1", "b": "2", "c": "3"},
			want: map[string]string{"a": "1", "b": "2", "c": "3"},
		},
		{
			name: "empty map returns empty map",
			src:  map[string]string{},
			want: map[string]string{},
		},
		{
			name: "single entry",
			src:  map[string]string{"key": "val"},
			want: map[string]string{"key": "val"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := copyLabels(tt.src)
			assert.Equal(t, tt.want, got)
		})
	}

	t.Run("mutation independence", func(t *testing.T) {
		original := map[string]string{"a": "1", "b": "2"}
		copied := copyLabels(original)

		// Mutate the copy
		copied["a"] = "changed"
		copied["new"] = "added"

		// Original must be unaffected
		assert.Equal(t, "1", original["a"])
		_, hasNew := original["new"]
		assert.False(t, hasNew, "original should not have the key added to the copy")
		assert.Len(t, original, 2)
	})
}

// unitTestSink is a minimal sink.Sink for internal (package collector) tests.
type unitTestSink struct {
	name         string
	writeMetrics func(context.Context, []sink.MetricPoint) error
	writeLogs    func(context.Context, []sink.LogEntry) error
}

func (s *unitTestSink) Name() string { return s.name }
func (s *unitTestSink) WriteMetrics(ctx context.Context, m []sink.MetricPoint) error {
	if s.writeMetrics != nil {
		return s.writeMetrics(ctx, m)
	}
	return nil
}
func (s *unitTestSink) WriteLogs(ctx context.Context, l []sink.LogEntry) error {
	if s.writeLogs != nil {
		return s.writeLogs(ctx, l)
	}
	return nil
}
func (s *unitTestSink) Close() error { return nil }

// Test_writeMetricsToSinks verifies that writeMetricsToSinks calls sinks only
// when there are points, and handles sink errors without panicking.
func Test_writeMetricsToSinks(t *testing.T) {
	noopLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("empty points does not call sink", func(t *testing.T) {
		called := false
		s := &unitTestSink{
			name: "test",
			writeMetrics: func(_ context.Context, _ []sink.MetricPoint) error {
				called = true
				return nil
			},
		}
		writeMetricsToSinks(context.Background(), []sink.Sink{s}, nil, noopLogger)
		assert.False(t, called, "sink should not be called for empty points")
	})

	t.Run("empty slice does not call sink", func(t *testing.T) {
		called := false
		s := &unitTestSink{
			name: "test",
			writeMetrics: func(_ context.Context, _ []sink.MetricPoint) error {
				called = true
				return nil
			},
		}
		writeMetricsToSinks(context.Background(), []sink.Sink{s}, []sink.MetricPoint{}, noopLogger)
		assert.False(t, called, "sink should not be called for empty slice")
	})

	t.Run("non-empty points calls sink", func(t *testing.T) {
		var received []sink.MetricPoint
		s := &unitTestSink{
			name: "test",
			writeMetrics: func(_ context.Context, pts []sink.MetricPoint) error {
				received = pts
				return nil
			},
		}
		points := []sink.MetricPoint{{Name: "metric1", Value: 42.0}}
		writeMetricsToSinks(context.Background(), []sink.Sink{s}, points, noopLogger)
		require.Len(t, received, 1)
		assert.Equal(t, "metric1", received[0].Name)
	})

	t.Run("multiple sinks all called", func(t *testing.T) {
		callCount := 0
		makeSink := func(name string) sink.Sink {
			return &unitTestSink{
				name: name,
				writeMetrics: func(_ context.Context, _ []sink.MetricPoint) error {
					callCount++
					return nil
				},
			}
		}
		sinks := []sink.Sink{makeSink("a"), makeSink("b")}
		points := []sink.MetricPoint{{Name: "m", Value: 1}}
		writeMetricsToSinks(context.Background(), sinks, points, noopLogger)
		assert.Equal(t, 2, callCount, "both sinks should be called")
	})

	t.Run("sink error does not panic", func(t *testing.T) {
		s := &unitTestSink{
			name: "failing",
			writeMetrics: func(_ context.Context, _ []sink.MetricPoint) error {
				return fmt.Errorf("write failed")
			},
		}
		points := []sink.MetricPoint{{Name: "m", Value: 1}}
		assert.NotPanics(t, func() {
			writeMetricsToSinks(context.Background(), []sink.Sink{s}, points, noopLogger)
		})
	})
}

// Test_writeLogsToSinks verifies that writeLogsToSinks calls sinks only when
// there are entries, and handles sink errors without panicking.
func Test_writeLogsToSinks(t *testing.T) {
	noopLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("empty entries does not call sink", func(t *testing.T) {
		called := false
		s := &unitTestSink{
			name: "test",
			writeLogs: func(_ context.Context, _ []sink.LogEntry) error {
				called = true
				return nil
			},
		}
		writeLogsToSinks(context.Background(), []sink.Sink{s}, nil, noopLogger)
		assert.False(t, called, "sink should not be called for nil entries")
	})

	t.Run("empty slice does not call sink", func(t *testing.T) {
		called := false
		s := &unitTestSink{
			name: "test",
			writeLogs: func(_ context.Context, _ []sink.LogEntry) error {
				called = true
				return nil
			},
		}
		writeLogsToSinks(context.Background(), []sink.Sink{s}, []sink.LogEntry{}, noopLogger)
		assert.False(t, called, "sink should not be called for empty slice")
	})

	t.Run("non-empty entries calls sink", func(t *testing.T) {
		var received []sink.LogEntry
		s := &unitTestSink{
			name: "test",
			writeLogs: func(_ context.Context, entries []sink.LogEntry) error {
				received = entries
				return nil
			},
		}
		entries := []sink.LogEntry{{Message: "hello", Severity: "INFO"}}
		writeLogsToSinks(context.Background(), []sink.Sink{s}, entries, noopLogger)
		require.Len(t, received, 1)
		assert.Equal(t, "hello", received[0].Message)
	})

	t.Run("multiple sinks all called", func(t *testing.T) {
		callCount := 0
		makeSink := func(name string) sink.Sink {
			return &unitTestSink{
				name: name,
				writeLogs: func(_ context.Context, _ []sink.LogEntry) error {
					callCount++
					return nil
				},
			}
		}
		sinks := []sink.Sink{makeSink("a"), makeSink("b")}
		entries := []sink.LogEntry{{Message: "log"}}
		writeLogsToSinks(context.Background(), sinks, entries, noopLogger)
		assert.Equal(t, 2, callCount, "both sinks should be called")
	})

	t.Run("sink error does not panic", func(t *testing.T) {
		s := &unitTestSink{
			name: "failing",
			writeLogs: func(_ context.Context, _ []sink.LogEntry) error {
				return fmt.Errorf("write failed")
			},
		}
		entries := []sink.LogEntry{{Message: "log"}}
		assert.NotPanics(t, func() {
			writeLogsToSinks(context.Background(), []sink.Sink{s}, entries, noopLogger)
		})
	})
}

// Test_pollCoverageGaps_BudgetExact verifies that pollCoverageGaps emits
// exactly maxItemsPerPoll items when more gaps are available than budget
// allows. This kills CONDITIONALS_BOUNDARY mutants on the budget checks.
func Test_pollCoverageGaps_BudgetExact(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		maxItems     int
		entityCount  int
		itemsPerEmit int
		wantItems    int // expected number of emitted WorkItems
	}{
		{
			name:         "budget 3, 5 entities, 1 per emit",
			maxItems:     3,
			entityCount:  5,
			itemsPerEmit: 1,
			wantItems:    3,
		},
		{
			name:         "budget 4, 5 entities, 2 per emit",
			maxItems:     4,
			entityCount:  5,
			itemsPerEmit: 2,
			// 2 entities fit (2*2=4), third doesn't (4+2=6 > 4)
			wantItems: 4,
		},
		{
			name:         "budget 1, 3 entities, 1 per emit",
			maxItems:     1,
			entityCount:  3,
			itemsPerEmit: 1,
			wantItems:    1,
		},
		{
			name:         "budget equals entity count",
			maxItems:     3,
			entityCount:  3,
			itemsPerEmit: 1,
			wantItems:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build targets and entities
			var targets []types.ServiceTarget
			for i := 0; i < tt.entityCount; i++ {
				targets = append(targets, types.ServiceTarget{
					ProjectID:     fmt.Sprintf("proj-%d", i),
					ServiceID:     fmt.Sprintf("svc-%d", i),
					EnvironmentID: fmt.Sprintf("env-%d", i),
				})
			}

			builder := func(e pollEntity, chunk coverage.TimeRange, isLiveEdge bool) []types.WorkItem {
				var items []types.WorkItem
				for j := 0; j < tt.itemsPerEmit; j++ {
					items = append(items, types.WorkItem{
						ID:       fmt.Sprintf("%s:%d", e.Key, j),
						Kind:     types.QueryMetrics,
						AliasKey: e.Key,
					})
				}
				return items
			}

			items := pollCoverageGaps(now, gapPollParams{
				store:     &fakeStateStore{},
				discovery: &fakeTargetProvider{targets: targets},
				logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
				// Short retention so each entity has a small gap (no chunking needed)
				metricRetention: 30 * time.Minute,
				chunkSize:       6 * time.Hour,
				maxItemsPerPoll: tt.maxItems,
				nextPoll:        time.Time{},
				entities: func(tgts []types.ServiceTarget) []pollEntity {
					var entities []pollEntity
					for _, tgt := range tgts {
						entities = append(entities, pollEntity{
							Key:          tgt.ProjectID,
							CoverageType: coverage.CoverageTypeMetric,
						})
					}
					return entities
				},
				buildItems:   builder,
				itemsPerEmit: tt.itemsPerEmit,
				logPrefix:    "test",
			})

			assert.Equal(t, tt.wantItems, len(items),
				"should emit exactly maxItemsPerPoll items when budget is the limiting factor")
		})
	}
}
