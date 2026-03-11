package coverage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

// fakeStore is a minimal in-memory implementation of the GetCoverage/SetCoverage
// interfaces used by LoadCoverage and SaveCoverage, for use in tests only.
type fakeStore struct {
	data map[string][]byte
}

func (f *fakeStore) GetCoverage(key string) ([]byte, error) {
	return f.data[key], nil
}

func (f *fakeStore) SetCoverage(key string, data []byte) error {
	if f.data == nil {
		f.data = make(map[string][]byte)
	}
	f.data[key] = data
	return nil
}

// TestLoadCoverage consolidates the three load scenarios into a single table.
func TestLoadCoverage(t *testing.T) {
	tests := []struct {
		name      string
		storeData map[string][]byte
		key       string
		wantErr   bool
		wantNil   bool
		wantEmpty bool
	}{
		{
			name:      "corrupted JSON returns error",
			storeData: map[string][]byte{"test-key": []byte(`{not valid json`)},
			key:       "test-key",
			wantErr:   true,
			wantNil:   true,
		},
		{
			name:      "missing key returns nil without error",
			storeData: map[string][]byte{},
			key:       "missing-key",
			wantErr:   false,
			wantNil:   true,
		},
		{
			name:      "empty array returns empty slice without error",
			storeData: map[string][]byte{"test-key": []byte(`[]`)},
			key:       "test-key",
			wantErr:   false,
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakeStore{data: tt.storeData}
			intervals, err := coverage.LoadCoverage(store, tt.key)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.wantNil {
				assert.Nil(t, intervals)
			} else if tt.wantEmpty {
				assert.Empty(t, intervals)
			}
		})
	}
}

// TestSaveCoverage verifies that SaveCoverage correctly persists intervals and
// that a subsequent LoadCoverage round-trip restores the original data.
func TestSaveCoverage(t *testing.T) {
	t1 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 3, 1, 1, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)

	t.Run("round-trip preserves intervals", func(t *testing.T) {
		store := &fakeStore{data: make(map[string][]byte)}
		key := "proj-1:metric"

		original := []coverage.CoverageInterval{
			{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
			{Start: t2, End: t3, Kind: coverage.CoverageEmpty},
		}

		err := coverage.SaveCoverage(store, key, original)
		require.NoError(t, err)

		loaded, err := coverage.LoadCoverage(store, key)
		require.NoError(t, err)
		require.Len(t, loaded, 2)

		assert.Equal(t, original[0].Start, loaded[0].Start)
		assert.Equal(t, original[0].End, loaded[0].End)
		assert.Equal(t, original[0].Kind, loaded[0].Kind)
		assert.Equal(t, original[0].Resolution, loaded[0].Resolution)

		assert.Equal(t, original[1].Start, loaded[1].Start)
		assert.Equal(t, original[1].End, loaded[1].End)
		assert.Equal(t, original[1].Kind, loaded[1].Kind)
	})

	t.Run("overwrite replaces previous intervals", func(t *testing.T) {
		store := &fakeStore{data: make(map[string][]byte)}
		key := "proj-1:metric"

		first := []coverage.CoverageInterval{
			{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 30},
		}
		second := []coverage.CoverageInterval{
			{Start: t1, End: t2, Kind: coverage.CoverageCollected, Resolution: 60},
			{Start: t2, End: t3, Kind: coverage.CoverageCollected, Resolution: 60},
		}

		err := coverage.SaveCoverage(store, key, first)
		require.NoError(t, err)

		err = coverage.SaveCoverage(store, key, second)
		require.NoError(t, err)

		loaded, err := coverage.LoadCoverage(store, key)
		require.NoError(t, err)
		require.Len(t, loaded, 2, "overwrite should replace all previous intervals")

		assert.Equal(t, 60, loaded[0].Resolution)
		assert.Equal(t, 60, loaded[1].Resolution)
	})
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
