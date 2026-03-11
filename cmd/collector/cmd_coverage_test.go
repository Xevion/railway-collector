package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/state"
)

// makeEntry builds a RawEntry with JSON-encoded CoverageIntervals.
func makeEntry(t *testing.T, key string, intervals []collector.CoverageInterval) state.RawEntry {
	t.Helper()
	data, err := json.Marshal(intervals)
	require.NoError(t, err)
	return state.RawEntry{Key: key, Value: data}
}

func TestCoverage_GapCounting(t *testing.T) {
	// Window: 00:00 to 01:00 (1 hour)
	// Intervals cover 00:10-00:20 and 00:40-00:50 (20 min collected)
	// Gaps: 00:00-00:10, 00:20-00:40, 00:50-01:00 => 3 gaps totaling 40 min
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "proj-1:metric", []collector.CoverageInterval{
			{
				Start: time.Date(2025, 1, 1, 0, 10, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 20, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
			{
				Start: time.Date(2025, 1, 1, 0, 40, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 50, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
		}),
	}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)

	require.Len(t, summaries, 1, "expected one summary entry")
	s := summaries[0]

	assert.Equal(t, 3, s.GapCount, "should find 3 gaps using FindGaps (before, between, after intervals)")
	assert.Equal(t, 40*time.Minute, s.Gaps, "total gap duration should be 40 minutes")
	assert.Equal(t, 20*time.Minute, s.Collected, "collected duration should be 20 minutes")
}

func TestCoverage_GapCounting_WithEmptyIntervals(t *testing.T) {
	// Window: 00:00 to 01:00
	// Collected: 00:00-00:20 (20 min)
	// Empty:     00:20-00:40 (20 min)
	// Gap:       00:40-01:00 (20 min, uncovered)
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "proj-1:metric", []collector.CoverageInterval{
			{
				Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 20, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
			{
				Start: time.Date(2025, 1, 1, 0, 20, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 40, 0, 0, time.UTC),
				Kind:  collector.CoverageEmpty,
			},
		}),
	}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)

	require.Len(t, summaries, 1)
	s := summaries[0]

	assert.Equal(t, 20*time.Minute, s.Collected, "collected should be 20 min")
	assert.Equal(t, 20*time.Minute, s.Empty, "empty should be 20 min")
	assert.Equal(t, 20*time.Minute, s.Gaps, "gap duration should be 20 min (00:40-01:00)")
	assert.Equal(t, 1, s.GapCount, "should find 1 gap after the empty interval")
}

func TestCoverage_LargestGap_NoGaps(t *testing.T) {
	// Full coverage, no gaps at all -> LargestGap should be 0, not missing/nil
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "proj-1:metric", []collector.CoverageInterval{
			{
				Start: windowStart,
				End:   windowEnd,
				Kind:  collector.CoverageCollected,
			},
		}),
	}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)

	require.Len(t, summaries, 1)
	s := summaries[0]

	assert.Equal(t, 0, s.GapCount, "no gaps expected")
	assert.Equal(t, time.Duration(0), s.LargestGap, "largest gap should be 0s, not unset")
	assert.Equal(t, time.Duration(0), s.Gaps, "total gap duration should be 0")
}

func TestCoverage_LargestGap_WithGaps(t *testing.T) {
	// Window: 00:00 to 01:00
	// Intervals: 00:10-00:20, 00:40-00:50
	// Gaps: 00:00-00:10 (10 min), 00:20-00:40 (20 min), 00:50-01:00 (10 min)
	// Largest gap = 20 min
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "proj-1:metric", []collector.CoverageInterval{
			{
				Start: time.Date(2025, 1, 1, 0, 10, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 20, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
			{
				Start: time.Date(2025, 1, 1, 0, 40, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 50, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
		}),
	}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)

	require.Len(t, summaries, 1)
	assert.Equal(t, 20*time.Minute, summaries[0].LargestGap, "largest gap should be 20 minutes")
}

func TestCoverage_LargestGap_NoCoverage(t *testing.T) {
	// No intervals at all -> entire window is one gap
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "proj-1:metric", []collector.CoverageInterval{}),
	}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)

	require.Len(t, summaries, 1)
	s := summaries[0]

	assert.Equal(t, 1, s.GapCount, "entire window should be 1 gap")
	assert.Equal(t, time.Hour, s.LargestGap, "largest gap should equal the full window (1h)")
	assert.Equal(t, time.Hour, s.Gaps, "total gap should equal the full window (1h)")
}

func TestCoverage_KeyResolution(t *testing.T) {
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "uuid-1:metric", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
		makeEntry(t, "uuid-2:log:environment", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
		makeEntry(t, "uuid-3:log:build", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
	}

	resolver := func(id string) string {
		switch id {
		case "uuid-1":
			return "my-project"
		case "uuid-2":
			return "my-project/my-service"
		case "uuid-3":
			return "other-project/build-svc"
		default:
			return id
		}
	}

	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, resolver)

	require.Len(t, summaries, 3)

	// Keys should have the UUID portion resolved to human-readable names
	keys := make(map[string]bool)
	for _, s := range summaries {
		keys[s.Key] = true
	}
	assert.True(t, keys["my-project:metric"],
		"uuid-1:metric should resolve to my-project:metric")
	assert.True(t, keys["my-project/my-service:log:environment"],
		"uuid-2:log:environment should resolve to my-project/my-service:log:environment")
	assert.True(t, keys["other-project/build-svc:log:build"],
		"uuid-3:log:build should resolve to other-project/build-svc:log:build")
}

func TestCoverage_KeyResolution_CompositeKeys(t *testing.T) {
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "svc-1:env-1:service-metric", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
		makeEntry(t, "svc-1:env-1:replica-metric", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
		makeEntry(t, "svc-2:env-1:http-metric", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
	}

	resolver := func(id string) string {
		switch id {
		case "svc-1:env-1":
			return "my-project/web"
		case "svc-2:env-1":
			return "my-project/api"
		default:
			return ""
		}
	}

	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, resolver)
	require.Len(t, summaries, 3)

	keys := make(map[string]bool)
	for _, s := range summaries {
		keys[s.Key] = true
	}
	assert.True(t, keys["my-project/web:service-metric"],
		"svc-1:env-1:service-metric should resolve to my-project/web:service-metric")
	assert.True(t, keys["my-project/web:replica-metric"],
		"svc-1:env-1:replica-metric should resolve to my-project/web:replica-metric")
	assert.True(t, keys["my-project/api:http-metric"],
		"svc-2:env-1:http-metric should resolve to my-project/api:http-metric")
}

func TestCoverage_KeyResolution_UnknownID(t *testing.T) {
	// When the resolver doesn't recognize an ID, it should pass through unchanged
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		makeEntry(t, "unknown-uuid:metric", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
	}

	// Resolver returns the input unchanged for unknown IDs
	resolver := func(id string) string { return id }

	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, resolver)
	require.Len(t, summaries, 1)
	assert.Equal(t, "unknown-uuid:metric", summaries[0].Key)
}

func TestCoverage_Percentage(t *testing.T) {
	tests := []struct {
		name      string
		intervals []collector.CoverageInterval
		wantPct   float64
		tolerance float64
	}{
		{
			name: "full coverage",
			intervals: []collector.CoverageInterval{
				{
					Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					End:   time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
					Kind:  collector.CoverageCollected,
				},
			},
			wantPct:   100.0,
			tolerance: 0.01,
		},
		{
			name:      "no coverage",
			intervals: []collector.CoverageInterval{},
			wantPct:   0.0,
			tolerance: 0.01,
		},
		{
			name: "half collected half empty",
			intervals: []collector.CoverageInterval{
				{
					Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					End:   time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC),
					Kind:  collector.CoverageCollected,
				},
				{
					Start: time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC),
					End:   time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC),
					Kind:  collector.CoverageEmpty,
				},
			},
			wantPct:   50.0,
			tolerance: 0.01,
		},
		{
			name: "collected plus gap",
			// 20 min collected, 40 min gap => 20/(20+0+40) = 33.33%
			intervals: []collector.CoverageInterval{
				{
					Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					End:   time.Date(2025, 1, 1, 0, 20, 0, 0, time.UTC),
					Kind:  collector.CoverageCollected,
				},
			},
			wantPct:   33.33,
			tolerance: 0.01,
		},
		{
			name: "mixed collected empty and gap",
			// 15 min collected + 15 min empty + 30 min gap = 60 min window
			// percentage = 15/60 = 25%
			intervals: []collector.CoverageInterval{
				{
					Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					End:   time.Date(2025, 1, 1, 0, 15, 0, 0, time.UTC),
					Kind:  collector.CoverageCollected,
				},
				{
					Start: time.Date(2025, 1, 1, 0, 15, 0, 0, time.UTC),
					End:   time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC),
					Kind:  collector.CoverageEmpty,
				},
			},
			wantPct:   25.0,
			tolerance: 0.01,
		},
	}

	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	noopResolver := func(id string) string { return id }

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			entries := []state.RawEntry{
				makeEntry(t, "proj-1:metric", tc.intervals),
			}
			summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)
			require.Len(t, summaries, 1)
			assert.InDelta(t, tc.wantPct, summaries[0].Percentage, tc.tolerance,
				"percentage = collected / (collected + empty + gaps) * 100")
		})
	}
}

func TestCoverage_SummaryTotalRow(t *testing.T) {
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		// Entry 1: 30 min collected, 0 empty, 30 min gap
		makeEntry(t, "proj-1:metric", []collector.CoverageInterval{
			{
				Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
		}),
		// Entry 2: 15 min collected, 15 min empty, 30 min gap
		makeEntry(t, "proj-2:metric", []collector.CoverageInterval{
			{
				Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 15, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
			{
				Start: time.Date(2025, 1, 1, 0, 15, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC),
				Kind:  collector.CoverageEmpty,
			},
		}),
	}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)
	require.Len(t, summaries, 2)

	headers, rows, totalRow := formatSummaryRows(summaries)

	// Headers should include key, collected, empty, gaps, gap count, largest gap, percentage
	require.NotEmpty(t, headers, "headers must not be empty")
	assert.GreaterOrEqual(t, len(headers), 7, "should have at least 7 columns: key, collected, empty, gaps, gap count, largest gap, percentage")

	// Data rows should match the number of summaries
	assert.Len(t, rows, 2, "should have 2 data rows")

	// Total row must exist and have the same column count as headers
	require.NotEmpty(t, totalRow, "total/summary row must not be empty")
	assert.Len(t, totalRow, len(headers), "total row should have same number of columns as headers")

	// The total row's first column should indicate it's a total
	assert.Contains(t, totalRow[0], "Total", "first column of total row should say 'Total'")
}

func TestCoverage_FormatSummaryRows_LargestGapFormatting(t *testing.T) {
	// Verify that largest gap is NEVER shown as "-" or empty
	summaries := []coverageSummary{
		{
			Key:        "proj-1:metric",
			Collected:  time.Hour,
			Empty:      0,
			Gaps:       0,
			GapCount:   0,
			LargestGap: 0,
			Percentage: 100.0,
		},
		{
			Key:        "proj-2:metric",
			Collected:  30 * time.Minute,
			Empty:      0,
			Gaps:       30 * time.Minute,
			GapCount:   1,
			LargestGap: 30 * time.Minute,
			Percentage: 50.0,
		},
	}

	_, rows, _ := formatSummaryRows(summaries)
	require.Len(t, rows, 2)

	// Find the largest gap column index by checking headers
	headers, _, _ := formatSummaryRows(summaries)
	largestGapIdx := -1
	for i, h := range headers {
		if h == "Largest Gap" {
			largestGapIdx = i
			break
		}
	}
	require.NotEqual(t, -1, largestGapIdx, "must have a 'Largest Gap' column")

	// No gaps: should show "0s", never "-" or ""
	assert.Equal(t, "0s", rows[0][largestGapIdx],
		"when no gaps, largest gap should be '0s', not '-' or empty")

	// With gaps: should show the actual duration
	assert.Equal(t, "30m0s", rows[1][largestGapIdx],
		"largest gap should show actual duration")
}

func TestCoverage_FormatSummaryRows_PercentageColumn(t *testing.T) {
	summaries := []coverageSummary{
		{
			Key:        "proj-1:metric",
			Collected:  20 * time.Minute,
			Empty:      10 * time.Minute,
			Gaps:       30 * time.Minute,
			GapCount:   1,
			LargestGap: 30 * time.Minute,
			Percentage: 33.33,
		},
	}

	headers, rows, _ := formatSummaryRows(summaries)

	// Find percentage column
	pctIdx := -1
	for i, h := range headers {
		if h == "Coverage %" || h == "%" || h == "Pct" || h == "Coverage" {
			pctIdx = i
			break
		}
	}
	require.NotEqual(t, -1, pctIdx,
		fmt.Sprintf("must have a percentage column, got headers: %v", headers))

	// Should contain the percentage value
	assert.Contains(t, rows[0][pctIdx], "33.3",
		"percentage cell should contain the computed percentage")
}

func TestCoverage_MultipleEntries(t *testing.T) {
	// Verify that buildCoverageSummaries handles multiple entries independently
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	entries := []state.RawEntry{
		// Full coverage
		makeEntry(t, "proj-1:metric", []collector.CoverageInterval{
			{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
		}),
		// No coverage
		makeEntry(t, "proj-2:log:environment", []collector.CoverageInterval{}),
		// Partial coverage
		makeEntry(t, "proj-3:log:build", []collector.CoverageInterval{
			{
				Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				End:   time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC),
				Kind:  collector.CoverageCollected,
			},
		}),
	}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(entries, windowStart, windowEnd, noopResolver)

	require.Len(t, summaries, 3, "should produce one summary per entry")

	// Build a map by key for easier assertions
	byKey := make(map[string]coverageSummary)
	for _, s := range summaries {
		byKey[s.Key] = s
	}

	// Full coverage
	full := byKey["proj-1:metric"]
	assert.InDelta(t, 100.0, full.Percentage, 0.01)
	assert.Equal(t, 0, full.GapCount)
	assert.Equal(t, time.Duration(0), full.LargestGap)

	// No coverage
	none := byKey["proj-2:log:environment"]
	assert.InDelta(t, 0.0, none.Percentage, 0.01)
	assert.Equal(t, 1, none.GapCount)
	assert.Equal(t, time.Hour, none.LargestGap)

	// Partial coverage
	partial := byKey["proj-3:log:build"]
	assert.InDelta(t, 50.0, partial.Percentage, 0.01)
	assert.Equal(t, 1, partial.GapCount)
	assert.Equal(t, 30*time.Minute, partial.LargestGap)
}

func TestParseCoverageSegments(t *testing.T) {
	tests := []struct {
		key  string
		want []string
	}{
		{"banner:metric", []string{"banner", "metric"}},
		{"banner/banner:log:build", []string{"banner", "banner", "build"}},
		{"banner/banner:log:http", []string{"banner", "banner", "http"}},
		{"banner/banner:log:environment", []string{"banner", "banner", "env"}},
		{"banner/Postgres:log:environment", []string{"banner", "Postgres", "env"}},
		{"runnerspace:metric", []string{"runnerspace", "metric"}},
		{"bare-key", []string{"bare-key"}},
		{"proj:newtype", []string{"proj", "newtype"}},
		// Composite keys (after resolution) produce project/service + type segments
		{"my-project/web:service-metric", []string{"my-project", "web", "service-metric"}},
		{"my-project/web:replica-metric", []string{"my-project", "web", "replica-metric"}},
		{"my-project/api:http-metric", []string{"my-project", "api", "http-metric"}},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.want, parseCoverageSegments(tt.key))
		})
	}
}

// testTargets returns the standard 3-target set used across expectedCoverageKeys tests.
func testTargets() []collector.ServiceTarget {
	return []collector.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"},
		{ProjectID: "proj-1", ServiceID: "svc-2", EnvironmentID: "env-1", DeploymentID: "dep-2"},
		{ProjectID: "proj-2", ServiceID: "svc-3", EnvironmentID: "env-2", DeploymentID: ""},
	}
}

func TestExpectedCoverageKeys_AllEnabled(t *testing.T) {
	cfg := config.DefaultConfig()
	targets := testTargets()
	keys := expectedCoverageKeys(cfg, targets)

	// Project-level metric keys: 2 unique projects
	assert.Contains(t, keys, "proj-1:metric")
	assert.Contains(t, keys, "proj-2:metric")

	// Service metric keys: 3 composite keys
	assert.Contains(t, keys, "svc-1:env-1:service-metric")
	assert.Contains(t, keys, "svc-2:env-1:service-metric")
	assert.Contains(t, keys, "svc-3:env-2:service-metric")

	// Replica metric keys
	assert.Contains(t, keys, "svc-1:env-1:replica-metric")
	assert.Contains(t, keys, "svc-2:env-1:replica-metric")
	assert.Contains(t, keys, "svc-3:env-2:replica-metric")

	// HTTP metric keys
	assert.Contains(t, keys, "svc-1:env-1:http-metric")
	assert.Contains(t, keys, "svc-2:env-1:http-metric")
	assert.Contains(t, keys, "svc-3:env-2:http-metric")

	// Log environment keys: 2 unique environments
	assert.Contains(t, keys, "env-1:log:environment")
	assert.Contains(t, keys, "env-2:log:environment")

	// Log build keys: only targets with DeploymentID (2 of 3)
	assert.Contains(t, keys, "dep-1:log:build")
	assert.Contains(t, keys, "dep-2:log:build")

	// Log http keys: same deployment-bearing targets
	assert.Contains(t, keys, "dep-1:log:http")
	assert.Contains(t, keys, "dep-2:log:http")

	// Total: 2 project + 3*3 service/replica/http + 2 env + 2 build + 2 http = 17
	assert.Len(t, keys, 17)
}

func TestExpectedCoverageKeys_MetricsDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Collect.Metrics.Enabled = false
	targets := testTargets()
	keys := expectedCoverageKeys(cfg, targets)

	// No metric keys at all
	for k := range keys {
		assert.NotContains(t, k, "metric",
			"no metric keys should exist when metrics disabled, got %q", k)
	}

	// Should still have log keys
	assert.Contains(t, keys, "env-1:log:environment")
	assert.Contains(t, keys, "env-2:log:environment")
	assert.Contains(t, keys, "dep-1:log:build")
	assert.Contains(t, keys, "dep-2:log:build")
	assert.Contains(t, keys, "dep-1:log:http")
	assert.Contains(t, keys, "dep-2:log:http")

	// Total: 2 env + 2 build + 2 http = 6
	assert.Len(t, keys, 6)
}

func TestExpectedCoverageKeys_ServiceMetricsDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Collect.Metrics.Service.Enabled = false
	cfg.Collect.Metrics.Replica.Enabled = false
	cfg.Collect.Metrics.HTTP.Enabled = false
	targets := testTargets()
	keys := expectedCoverageKeys(cfg, targets)

	// Should have project-level metric keys only
	assert.Contains(t, keys, "proj-1:metric")
	assert.Contains(t, keys, "proj-2:metric")

	// No service/replica/http metric keys
	for k := range keys {
		assert.NotContains(t, k, "service-metric",
			"no service-metric keys expected, got %q", k)
		assert.NotContains(t, k, "replica-metric",
			"no replica-metric keys expected, got %q", k)
		assert.NotContains(t, k, "http-metric",
			"no http-metric keys expected, got %q", k)
	}

	// Should still have log keys
	assert.Contains(t, keys, "env-1:log:environment")
	assert.Contains(t, keys, "dep-1:log:build")

	// Total: 2 project + 2 env + 2 build + 2 http = 8
	assert.Len(t, keys, 8)
}

func TestExpectedCoverageKeys_LogsDisabled(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Collect.Logs.Enabled = false
	targets := testTargets()
	keys := expectedCoverageKeys(cfg, targets)

	// No log keys
	for k := range keys {
		assert.NotContains(t, k, "log:",
			"no log keys should exist when logs disabled, got %q", k)
	}

	// Should have all metric keys
	assert.Contains(t, keys, "proj-1:metric")
	assert.Contains(t, keys, "proj-2:metric")
	assert.Contains(t, keys, "svc-1:env-1:service-metric")
	assert.Contains(t, keys, "svc-1:env-1:replica-metric")
	assert.Contains(t, keys, "svc-1:env-1:http-metric")

	// Total: 2 project + 3*3 service/replica/http = 11
	assert.Len(t, keys, 11)
}

func TestExpectedCoverageKeys_PartialLogTypes(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Collect.Logs.Types = []string{"deployment"}
	targets := testTargets()
	keys := expectedCoverageKeys(cfg, targets)

	// Should have log:environment (deployment type maps to env logs)
	assert.Contains(t, keys, "env-1:log:environment")
	assert.Contains(t, keys, "env-2:log:environment")

	// Should NOT have log:build or log:http
	for k := range keys {
		assert.NotContains(t, k, "log:build",
			"no log:build keys expected with only deployment type, got %q", k)
		assert.NotContains(t, k, "log:http",
			"no log:http keys expected with only deployment type, got %q", k)
	}

	// Total: 2 project + 9 service-level + 2 env = 13
	assert.Len(t, keys, 13)
}

func TestIsLogCoverageKey(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		{"proj-1:metric", false},
		{"svc-1:env-1:service-metric", false},
		{"env-1:log:environment", true},
		{"dep-1:log:build", true},
		{"dep-1:log:http", true},
		{"svc-1:env-1:replica-metric", false},
		{"svc-1:env-1:http-metric", false},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.want, isLogCoverageKey(tt.key))
		})
	}
}

func TestCoverage_SyntheticEntriesAppear(t *testing.T) {
	windowStart := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	// Real entry with 30 min of coverage
	realEntry := makeEntry(t, "proj-1:metric", []collector.CoverageInterval{
		{
			Start: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			End:   time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC),
			Kind:  collector.CoverageCollected,
		},
	})

	// Synthetic entry with empty intervals (represents a key with no data)
	emptyJSON, err := json.Marshal([]collector.CoverageInterval{})
	require.NoError(t, err)
	syntheticEntry := state.RawEntry{Key: "svc-1:env-1:service-metric", Value: emptyJSON}

	noopResolver := func(id string) string { return id }
	summaries := buildCoverageSummaries(
		[]state.RawEntry{realEntry, syntheticEntry},
		windowStart, windowEnd, noopResolver,
	)

	require.Len(t, summaries, 2, "should produce summaries for both real and synthetic entries")

	byKey := make(map[string]coverageSummary)
	for _, s := range summaries {
		byKey[s.Key] = s
	}

	// Real entry: 30 min collected out of 60 min window = 50%
	real := byKey["proj-1:metric"]
	assert.InDelta(t, 50.0, real.Percentage, 0.01)
	assert.True(t, real.Collected > 0, "real entry should have non-zero collected time")

	// Synthetic entry: 0% coverage, entire window is one gap
	synthetic := byKey["svc-1:env-1:service-metric"]
	assert.InDelta(t, 0.0, synthetic.Percentage, 0.01, "synthetic entry should have 0%% coverage")
	assert.Equal(t, 1, synthetic.GapCount, "synthetic entry should have 1 gap (entire window)")
	assert.Equal(t, time.Hour, synthetic.LargestGap, "synthetic entry largest gap should equal full window")
}

func TestCompositeKey(t *testing.T) {
	target := collector.ServiceTarget{ServiceID: "svc-1", EnvironmentID: "env-1"}
	assert.Equal(t, "svc-1:env-1", target.CompositeKey())
}
