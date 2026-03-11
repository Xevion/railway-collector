package coverage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xevion/railway-collector/internal/collector/coverage"
)

func TestMergeIntervals(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)
	t4 := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		intervals []coverage.CoverageInterval
		wantLen   int
		checks    func(t *testing.T, merged []coverage.CoverageInterval)
	}{
		{
			name: "adjacent same kind",
			intervals: []coverage.CoverageInterval{
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
				{Start: t2, End: t3, Kind: coverage.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t3, merged[0].End)
				assert.Equal(t, coverage.CoverageCollected, merged[0].Kind)
				assert.Equal(t, 30, merged[0].Resolution)
			},
		},
		{
			name: "adjacent different kind",
			intervals: []coverage.CoverageInterval{
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
				{Start: t2, End: t3, Kind: coverage.CoverageEmpty},
			},
			wantLen: 2,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, coverage.CoverageCollected, merged[0].Kind)
				assert.Equal(t, coverage.CoverageEmpty, merged[1].Kind)
			},
		},
		{
			name: "overlapping same kind",
			intervals: []coverage.CoverageInterval{
				{Start: t1, End: t3, Kind: coverage.CoverageCollected, Resolution: 30},
				{Start: t2, End: t4, Kind: coverage.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t4, merged[0].End)
			},
		},
		{
			name: "gap between intervals",
			intervals: []coverage.CoverageInterval{
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
				{Start: t3, End: t4, Kind: coverage.CoverageCollected, Resolution: 30},
			},
			wantLen: 2,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t2, merged[0].End)
				assert.Equal(t, t3, merged[1].Start)
				assert.Equal(t, t4, merged[1].End)
			},
		},
		{
			name: "single interval",
			intervals: []coverage.CoverageInterval{
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t2, merged[0].End)
			},
		},
		{
			name:      "empty input",
			intervals: []coverage.CoverageInterval{},
			wantLen:   0,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Nil(t, merged)
			},
		},
		{
			name: "identical intervals",
			intervals: []coverage.CoverageInterval{
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t2, merged[0].End)
			},
		},
		{
			name: "different resolution prevents merge",
			intervals: []coverage.CoverageInterval{
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 60},
			},
			wantLen: 2,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, 30, merged[0].Resolution)
				assert.Equal(t, 60, merged[1].Resolution)
			},
		},
		{
			name: "unsorted input",
			intervals: []coverage.CoverageInterval{
				{Start: t2, End: t3, Kind: coverage.CoverageCollected, Resolution: 30},
				{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []coverage.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t3, merged[0].End)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := coverage.MergeIntervals(tt.intervals)
			if tt.wantLen == 0 {
				tt.checks(t, merged)
				return
			}
			if !assert.Len(t, merged, tt.wantLen) {
				return
			}
			tt.checks(t, merged)
		})
	}
}

func TestFindGaps(t *testing.T) {
	windowStart := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	windowEnd := windowStart.Add(6 * time.Hour)

	tests := []struct {
		name      string
		intervals []coverage.CoverageInterval
		wantLen   int
		checks    func(t *testing.T, gaps []coverage.TimeRange)
	}{
		{
			name:      "no intervals",
			intervals: []coverage.CoverageInterval{},
			wantLen:   1,
			checks: func(t *testing.T, gaps []coverage.TimeRange) {
				assert.Equal(t, windowStart, gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
		{
			name: "full coverage",
			intervals: []coverage.CoverageInterval{
				{Start: windowStart, End: windowEnd, Kind: coverage.CoverageCollected},
			},
			wantLen: 0,
			checks:  func(t *testing.T, gaps []coverage.TimeRange) {},
		},
		{
			name: "gap at start",
			intervals: []coverage.CoverageInterval{
				{Start: windowStart.Add(1 * time.Hour), End: windowEnd, Kind: coverage.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []coverage.TimeRange) {
				assert.Equal(t, windowStart, gaps[0].Start)
				assert.Equal(t, windowStart.Add(1*time.Hour), gaps[0].End)
			},
		},
		{
			name: "gap at end",
			intervals: []coverage.CoverageInterval{
				{Start: windowStart, End: windowStart.Add(5 * time.Hour), Kind: coverage.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []coverage.TimeRange) {
				assert.Equal(t, windowStart.Add(5*time.Hour), gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
		{
			name: "gap in middle",
			intervals: []coverage.CoverageInterval{
				{Start: windowStart.Add(1 * time.Hour), End: windowStart.Add(2 * time.Hour), Kind: coverage.CoverageCollected},
				{Start: windowStart.Add(4 * time.Hour), End: windowStart.Add(5 * time.Hour), Kind: coverage.CoverageCollected},
			},
			wantLen: 3,
			checks: func(t *testing.T, gaps []coverage.TimeRange) {
				assert.Equal(t, windowStart, gaps[0].Start)
				assert.Equal(t, windowStart.Add(1*time.Hour), gaps[0].End)
				assert.Equal(t, windowStart.Add(2*time.Hour), gaps[1].Start)
				assert.Equal(t, windowStart.Add(4*time.Hour), gaps[1].End)
				assert.Equal(t, windowStart.Add(5*time.Hour), gaps[2].Start)
				assert.Equal(t, windowEnd, gaps[2].End)
			},
		},
		{
			name: "overlapping intervals",
			intervals: []coverage.CoverageInterval{
				{Start: windowStart, End: windowStart.Add(3 * time.Hour), Kind: coverage.CoverageCollected},
				{Start: windowStart.Add(2 * time.Hour), End: windowStart.Add(5 * time.Hour), Kind: coverage.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []coverage.TimeRange) {
				assert.Equal(t, windowStart.Add(5*time.Hour), gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
		{
			name: "intervals outside window",
			intervals: []coverage.CoverageInterval{
				{Start: windowStart.Add(-2 * time.Hour), End: windowStart.Add(-1 * time.Hour), Kind: coverage.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []coverage.TimeRange) {
				assert.Equal(t, windowStart, gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gaps := coverage.FindGaps(tt.intervals, windowStart, windowEnd)
			if !assert.Len(t, gaps, tt.wantLen) {
				return
			}
			tt.checks(t, gaps)
		})
	}
}

func TestPrioritizeGaps_RecentFirst(t *testing.T) {
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	retentionLimit := now.Add(-90 * 24 * time.Hour)

	gaps := []coverage.TimeRange{
		{Start: now.Add(-1 * time.Hour), End: now},                                         // recent (live edge)
		{Start: retentionLimit.Add(1 * time.Hour), End: retentionLimit.Add(3 * time.Hour)}, // old, near expiry
	}

	prioritized := coverage.PrioritizeGaps(gaps, now)
	// Recent gap (live edge) should rank first with recency-based scoring
	assert.Equal(t, gaps[0].Start, prioritized[0].Start)
}

func TestInsertInterval_MergesWithExisting(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)

	existing := []coverage.CoverageInterval{
		{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
	}

	newInterval := coverage.CoverageInterval{
		Start: t2, End: t3, Kind: coverage.CoverageCollected, Resolution: 30,
	}

	result := coverage.InsertInterval(existing, newInterval)
	assert.Len(t, result, 1)
	assert.Equal(t, t1, result[0].Start)
	assert.Equal(t, t3, result[0].End)
}
