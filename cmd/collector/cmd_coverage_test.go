package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector"
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
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			assert.Equal(t, tt.want, parseCoverageSegments(tt.key))
		})
	}
}
