package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/state"
)

// coverageSummary holds computed coverage statistics for a single key.
type coverageSummary struct {
	Key        string
	Collected  time.Duration
	Empty      time.Duration
	Gaps       time.Duration
	GapCount   int
	LargestGap time.Duration
	Percentage float64
}

// buildCoverageSummaries computes coverage summaries from raw state entries
// within the given time window. nameResolver translates IDs in keys to
// human-readable names (e.g. UUID -> "project/service").
func buildCoverageSummaries(
	entries []state.RawEntry,
	windowStart, windowEnd time.Time,
	nameResolver func(id string) string,
) []coverageSummary {
	var summaries []coverageSummary

	for _, e := range entries {
		var intervals []collector.CoverageInterval
		if err := json.Unmarshal(e.Value, &intervals); err != nil {
			continue
		}

		var s coverageSummary

		// Sum collected and empty durations
		for _, iv := range intervals {
			dur := iv.End.Sub(iv.Start)
			switch iv.Kind {
			case collector.CoverageCollected:
				s.Collected += dur
			case collector.CoverageEmpty:
				s.Empty += dur
			}
		}

		// Find gaps using the window-aware FindGaps function
		gaps := collector.FindGaps(intervals, windowStart, windowEnd)
		s.GapCount = len(gaps)
		for _, g := range gaps {
			d := g.Duration()
			s.Gaps += d
			if d > s.LargestGap {
				s.LargestGap = d
			}
		}

		// Calculate coverage percentage
		total := s.Collected + s.Empty + s.Gaps
		if total > 0 {
			s.Percentage = float64(s.Collected) / float64(total) * 100
		}

		// Resolve the key: parse first segment as ID, resolve name
		s.Key = resolveKey(e.Key, nameResolver)

		summaries = append(summaries, s)
	}

	return summaries
}

// buildNameResolver creates a name resolver function from the discovery cache.
// It maps project IDs, environment IDs, and deployment IDs to human-readable
// names like "project-name" or "project-name/service-name".
func buildNameResolver(reader interface {
	DiscoveryEntries() ([]state.RawEntry, error)
}) func(id string) string {
	idToName := make(map[string]string)

	discoveryEntries, err := reader.DiscoveryEntries()
	if err != nil {
		return func(id string) string { return "" }
	}

	for _, e := range discoveryEntries {
		var cached collector.PersistedProjectCache
		if err := json.Unmarshal(e.Value, &cached); err != nil {
			continue
		}

		for _, t := range cached.Targets {
			// Project ID → project name
			if _, ok := idToName[t.ProjectID]; !ok {
				idToName[t.ProjectID] = t.ProjectName
			}
			// Environment ID → project/service (environment context)
			label := t.ProjectName
			if t.ServiceName != "" {
				label = t.ProjectName + "/" + t.ServiceName
			}
			if t.EnvironmentID != "" {
				idToName[t.EnvironmentID] = label
			}
			// Deployment ID → project/service
			if t.DeploymentID != "" {
				idToName[t.DeploymentID] = label
			}
		}
	}

	return func(id string) string {
		return idToName[id]
	}
}

// resolveKey parses the first segment of a colon-separated key as an ID,
// calls nameResolver to get a human-readable name, and rebuilds the key.
func resolveKey(key string, nameResolver func(id string) string) string {
	idx := strings.Index(key, ":")
	if idx < 0 {
		// No colon — the whole key is the ID
		resolved := nameResolver(key)
		if resolved == "" {
			return key
		}
		return resolved
	}

	id := key[:idx]
	rest := key[idx:] // includes the leading ":"
	resolved := nameResolver(id)
	if resolved == "" {
		return key
	}
	return resolved + rest
}

// parseCoverageSegments splits a resolved coverage key into tree path segments.
// Input formats: "project:metric", "project/service:log:environment", etc.
// The "log:" prefix is stripped and "environment" is shortened to "env".
func parseCoverageSegments(key string) []string {
	colonIdx := strings.Index(key, ":")
	if colonIdx < 0 {
		return []string{key}
	}

	name := key[:colonIdx]
	typeStr := key[colonIdx+1:]

	var segments []string
	parts := strings.Split(name, "/")
	segments = append(segments, parts...)

	typeStr = strings.TrimPrefix(typeStr, "log:")
	if typeStr == "environment" {
		typeStr = "env"
	}
	segments = append(segments, typeStr)

	return segments
}

// formatSummaryRows converts coverage summaries into table-ready string slices.
// Returns headers, data rows, and a total/summary row.
func formatSummaryRows(summaries []coverageSummary) (headers []string, rows [][]string, totalRow []string) {
	headers = []string{"Key", "Collected", "Empty", "Gaps", "Gap Count", "Largest Gap", "Coverage"}

	var totalCollected, totalEmpty, totalGaps time.Duration
	var totalGapCount int
	var maxLargestGap time.Duration

	for _, s := range summaries {
		largestGapStr := "0s"
		if s.LargestGap > 0 {
			largestGapStr = s.LargestGap.Round(time.Second).String()
		}

		rows = append(rows, []string{
			s.Key,
			s.Collected.Round(time.Second).String(),
			s.Empty.Round(time.Second).String(),
			s.Gaps.Round(time.Second).String(),
			fmt.Sprintf("%d", s.GapCount),
			largestGapStr,
			fmt.Sprintf("%.1f%%", s.Percentage),
		})

		totalCollected += s.Collected
		totalEmpty += s.Empty
		totalGaps += s.Gaps
		totalGapCount += s.GapCount
		if s.LargestGap > maxLargestGap {
			maxLargestGap = s.LargestGap
		}
	}

	// Overall percentage
	overallTotal := totalCollected + totalEmpty + totalGaps
	var overallPct float64
	if overallTotal > 0 {
		overallPct = float64(totalCollected) / float64(overallTotal) * 100
	}

	totalLargestGapStr := "0s"
	if maxLargestGap > 0 {
		totalLargestGapStr = maxLargestGap.Round(time.Second).String()
	}

	totalRow = []string{
		"Total",
		totalCollected.Round(time.Second).String(),
		totalEmpty.Round(time.Second).String(),
		totalGaps.Round(time.Second).String(),
		fmt.Sprintf("%d", totalGapCount),
		totalLargestGapStr,
		fmt.Sprintf("%.1f%%", overallPct),
	}

	return headers, rows, totalRow
}
