package collector_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xevion/railway-collector/internal/collector"
)

func TestMergeIntervals(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)
	t4 := time.Date(2026, 3, 1, 3, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		intervals []collector.CoverageInterval
		wantLen   int
		checks    func(t *testing.T, merged []collector.CoverageInterval)
	}{
		{
			name: "adjacent same kind",
			intervals: []collector.CoverageInterval{
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
				{Start: t2, End: t3, Kind: collector.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t3, merged[0].End)
				assert.Equal(t, collector.CoverageCollected, merged[0].Kind)
				assert.Equal(t, 30, merged[0].Resolution)
			},
		},
		{
			name: "adjacent different kind",
			intervals: []collector.CoverageInterval{
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
				{Start: t2, End: t3, Kind: collector.CoverageEmpty},
			},
			wantLen: 2,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, collector.CoverageCollected, merged[0].Kind)
				assert.Equal(t, collector.CoverageEmpty, merged[1].Kind)
			},
		},
		{
			name: "overlapping same kind",
			intervals: []collector.CoverageInterval{
				{Start: t1, End: t3, Kind: collector.CoverageCollected, Resolution: 30},
				{Start: t2, End: t4, Kind: collector.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t4, merged[0].End)
			},
		},
		{
			name: "gap between intervals",
			intervals: []collector.CoverageInterval{
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
				{Start: t3, End: t4, Kind: collector.CoverageCollected, Resolution: 30},
			},
			wantLen: 2,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t2, merged[0].End)
				assert.Equal(t, t3, merged[1].Start)
				assert.Equal(t, t4, merged[1].End)
			},
		},
		{
			name: "single interval",
			intervals: []collector.CoverageInterval{
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t2, merged[0].End)
			},
		},
		{
			name:      "empty input",
			intervals: []collector.CoverageInterval{},
			wantLen:   0,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Nil(t, merged)
			},
		},
		{
			name: "identical intervals",
			intervals: []collector.CoverageInterval{
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t2, merged[0].End)
			},
		},
		{
			name: "different resolution prevents merge",
			intervals: []collector.CoverageInterval{
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 60},
			},
			wantLen: 2,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, 30, merged[0].Resolution)
				assert.Equal(t, 60, merged[1].Resolution)
			},
		},
		{
			name: "unsorted input",
			intervals: []collector.CoverageInterval{
				{Start: t2, End: t3, Kind: collector.CoverageCollected, Resolution: 30},
				{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
			},
			wantLen: 1,
			checks: func(t *testing.T, merged []collector.CoverageInterval) {
				assert.Equal(t, t1, merged[0].Start)
				assert.Equal(t, t3, merged[0].End)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := collector.MergeIntervals(tt.intervals)
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
		intervals []collector.CoverageInterval
		wantLen   int
		checks    func(t *testing.T, gaps []collector.TimeRange)
	}{
		{
			name:      "no intervals",
			intervals: []collector.CoverageInterval{},
			wantLen:   1,
			checks: func(t *testing.T, gaps []collector.TimeRange) {
				assert.Equal(t, windowStart, gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
		{
			name: "full coverage",
			intervals: []collector.CoverageInterval{
				{Start: windowStart, End: windowEnd, Kind: collector.CoverageCollected},
			},
			wantLen: 0,
			checks:  func(t *testing.T, gaps []collector.TimeRange) {},
		},
		{
			name: "gap at start",
			intervals: []collector.CoverageInterval{
				{Start: windowStart.Add(1 * time.Hour), End: windowEnd, Kind: collector.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []collector.TimeRange) {
				assert.Equal(t, windowStart, gaps[0].Start)
				assert.Equal(t, windowStart.Add(1*time.Hour), gaps[0].End)
			},
		},
		{
			name: "gap at end",
			intervals: []collector.CoverageInterval{
				{Start: windowStart, End: windowStart.Add(5 * time.Hour), Kind: collector.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []collector.TimeRange) {
				assert.Equal(t, windowStart.Add(5*time.Hour), gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
		{
			name: "gap in middle",
			intervals: []collector.CoverageInterval{
				{Start: windowStart.Add(1 * time.Hour), End: windowStart.Add(2 * time.Hour), Kind: collector.CoverageCollected},
				{Start: windowStart.Add(4 * time.Hour), End: windowStart.Add(5 * time.Hour), Kind: collector.CoverageCollected},
			},
			wantLen: 3,
			checks: func(t *testing.T, gaps []collector.TimeRange) {
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
			intervals: []collector.CoverageInterval{
				{Start: windowStart, End: windowStart.Add(3 * time.Hour), Kind: collector.CoverageCollected},
				{Start: windowStart.Add(2 * time.Hour), End: windowStart.Add(5 * time.Hour), Kind: collector.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []collector.TimeRange) {
				assert.Equal(t, windowStart.Add(5*time.Hour), gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
		{
			name: "intervals outside window",
			intervals: []collector.CoverageInterval{
				{Start: windowStart.Add(-2 * time.Hour), End: windowStart.Add(-1 * time.Hour), Kind: collector.CoverageCollected},
			},
			wantLen: 1,
			checks: func(t *testing.T, gaps []collector.TimeRange) {
				assert.Equal(t, windowStart, gaps[0].Start)
				assert.Equal(t, windowEnd, gaps[0].End)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gaps := collector.FindGaps(tt.intervals, windowStart, windowEnd)
			if !assert.Len(t, gaps, tt.wantLen) {
				return
			}
			tt.checks(t, gaps)
		})
	}
}

func TestPrioritizeGaps_OlderFirst(t *testing.T) {
	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	retentionLimit := now.Add(-90 * 24 * time.Hour)

	gaps := []collector.TimeRange{
		{Start: now.Add(-1 * time.Hour), End: now},                                         // recent
		{Start: retentionLimit.Add(1 * time.Hour), End: retentionLimit.Add(3 * time.Hour)}, // old, near expiry
	}

	prioritized := collector.PrioritizeGaps(gaps, now)
	// Older gap (near retention expiry) should rank first
	assert.Equal(t, gaps[1].Start, prioritized[0].Start)
}

func TestInsertInterval_MergesWithExisting(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)

	existing := []collector.CoverageInterval{
		{Start: t1, End: t2, Kind: collector.CoverageCollected, Resolution: 30},
	}

	newInterval := collector.CoverageInterval{
		Start: t2, End: t3, Kind: collector.CoverageCollected, Resolution: 30,
	}

	result := collector.InsertInterval(existing, newInterval)
	assert.Len(t, result, 1)
	assert.Equal(t, t1, result[0].Start)
	assert.Equal(t, t3, result[0].End)
}
