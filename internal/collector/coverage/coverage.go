package coverage

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
)

// CoverageKind indicates whether an interval contains collected data or was checked and found empty.
type CoverageKind int

const (
	CoverageCollected CoverageKind = iota
	CoverageEmpty
)

// Coverage type suffixes used in coverage keys across all generators.
const (
	CoverageTypeMetric        = "metric"
	CoverageTypeServiceMetric = "service-metric"
	CoverageTypeReplicaMetric = "replica-metric"
	CoverageTypeHTTPMetric    = "http-metric"
	CoverageTypeLogEnv        = "log:environment"
	CoverageTypeLogBuild      = "log:build"
	CoverageTypeLogHTTP       = "log:http"
)

// CoverageInterval represents a time range that has been collected (or checked).
type CoverageInterval struct {
	Start      time.Time    `json:"start"`
	End        time.Time    `json:"end"`
	Kind       CoverageKind `json:"kind"`
	Resolution int          `json:"resolution,omitempty"` // sampleRateSeconds for metrics, 0 for logs/empty
}

// ToWindow converts the interval's time bounds to a TimeWindow.
func (iv CoverageInterval) ToWindow() TimeWindow {
	return WindowFromInterval(iv)
}

// MergeIntervals combines adjacent/overlapping intervals with the same Kind and Resolution.
func MergeIntervals(intervals []CoverageInterval) []CoverageInterval {
	if len(intervals) == 0 {
		return nil
	}

	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].Start.Before(intervals[j].Start)
	})

	merged := []CoverageInterval{intervals[0]}
	for _, iv := range intervals[1:] {
		last := &merged[len(merged)-1]
		if iv.Kind == last.Kind && iv.Resolution == last.Resolution &&
			!iv.Start.After(last.End) {
			if iv.End.After(last.End) {
				last.End = iv.End
			}
		} else {
			merged = append(merged, iv)
		}
	}
	return merged
}

// InsertInterval adds a new interval to an existing sorted list and merges.
func InsertInterval(existing []CoverageInterval, newIv CoverageInterval) []CoverageInterval {
	all := append(existing, newIv)
	return MergeIntervals(all)
}

// FindGaps returns uncovered time windows within [windowStart, windowEnd].
func FindGaps(intervals []CoverageInterval, windowStart, windowEnd time.Time) []TimeWindow {
	if len(intervals) == 0 {
		return []TimeWindow{NewTimeWindow(windowStart, windowEnd)}
	}

	sorted := make([]CoverageInterval, len(intervals))
	copy(sorted, intervals)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Start.Before(sorted[j].Start)
	})

	var gaps []TimeWindow
	cursor := windowStart

	for _, iv := range sorted {
		if iv.Start.After(cursor) {
			gapEnd := iv.Start
			if gapEnd.After(windowEnd) {
				gapEnd = windowEnd
			}
			if cursor.Before(gapEnd) {
				gaps = append(gaps, NewTimeWindow(cursor, gapEnd))
			}
		}
		if iv.End.After(cursor) {
			cursor = iv.End
		}
	}

	if cursor.Before(windowEnd) {
		gaps = append(gaps, NewTimeWindow(cursor, windowEnd))
	}

	return gaps
}

// PrioritizeGaps sorts gaps by recency: the most recent gap (live edge) always
// wins, older gaps score lower. Score = 1.0 / max(gap_age_seconds, 1) where
// gap_age is now minus the gap's end time.
func PrioritizeGaps(gaps []TimeWindow, now time.Time) []TimeWindow {
	type scored struct {
		gap   TimeWindow
		score float64
	}

	scoredGaps := make([]scored, len(gaps))
	for i, g := range gaps {
		ageSecs := g.Age(now).Seconds()
		if ageSecs < 1 {
			ageSecs = 1
		}
		scoredGaps[i] = scored{gap: g, score: 1.0 / ageSecs}
	}

	sort.Slice(scoredGaps, func(i, j int) bool {
		return scoredGaps[i].score > scoredGaps[j].score
	})

	result := make([]TimeWindow, len(scoredGaps))
	for i, s := range scoredGaps {
		result[i] = s.gap
	}
	return result
}

// CoverageKey builds a composite key for the coverage store.
// Examples: "proj-1:metric", "env-1:log:environment", "dep-1:log:build"
func CoverageKey(parts ...string) string {
	return strings.Join(parts, ":")
}

// LoadCoverage deserializes coverage intervals from the store for a given key.
// Returns nil (not error) if the key doesn't exist yet.
func LoadCoverage(store interface {
	GetCoverage(key string) ([]byte, error)
}, key string) ([]CoverageInterval, error) {
	raw, err := store.GetCoverage(key)
	if err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	var intervals []CoverageInterval
	if err := json.Unmarshal(raw, &intervals); err != nil {
		return nil, err
	}
	return intervals, nil
}

// SaveCoverage serializes and persists coverage intervals to the store.
func SaveCoverage(store interface {
	SetCoverage(key string, data []byte) error
}, key string, intervals []CoverageInterval) error {
	data, err := json.Marshal(intervals)
	if err != nil {
		return err
	}
	return store.SetCoverage(key, data)
}
