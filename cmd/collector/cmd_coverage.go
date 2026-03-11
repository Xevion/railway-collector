package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"

	"github.com/xevion/railway-collector/internal/cli"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/state"
)

// CoverageCmd summarizes backfill coverage per project.
type CoverageCmd struct {
	Project   string `help:"Show detailed intervals for a specific project ID."`
	Intervals bool   `help:"Show individual intervals instead of summary."`
}

type coverageSummaryJSON struct {
	Key        string `json:"key"`
	Collected  string `json:"collected"`
	Empty      string `json:"empty"`
	Gaps       string `json:"gaps"`
	GapCount   int    `json:"gap_count"`
	LargestGap string `json:"largest_gap"`
	Coverage   string `json:"coverage"`
}

type coverageIntervalJSON struct {
	Key        string `json:"key"`
	Start      string `json:"start"`
	End        string `json:"end"`
	Duration   string `json:"duration"`
	Kind       string `json:"kind"`
	Resolution int    `json:"resolution,omitempty"`
}

func (cmd *CoverageCmd) Run(c *CLI) error {
	reader, err := openReader(c.State, c.Config)
	if err != nil {
		return err
	}
	defer reader.Close()

	entries, err := reader.CoverageEntries()
	if err != nil {
		return fmt.Errorf("reading coverage: %w", err)
	}

	f := formatter(c.JSON)

	// Filter by project if specified
	if cmd.Project != "" {
		var filtered []struct {
			key       string
			intervals []collector.CoverageInterval
		}
		for _, e := range entries {
			if e.Key != cmd.Project && !hasPrefix(e.Key, cmd.Project+":") {
				continue
			}
			var intervals []collector.CoverageInterval
			if err := json.Unmarshal(e.Value, &intervals); err != nil {
				return fmt.Errorf("parsing coverage for %s: %w", e.Key, err)
			}
			filtered = append(filtered, struct {
				key       string
				intervals []collector.CoverageInterval
			}{e.Key, intervals})
		}
		if len(filtered) == 0 {
			fmt.Printf("No coverage data found for project %s.\n", cmd.Project)
			return nil
		}

		if c.JSON {
			var jsonOut []coverageIntervalJSON
			for _, f := range filtered {
				for _, iv := range f.intervals {
					jsonOut = append(jsonOut, coverageIntervalJSON{
						Key:        f.key,
						Start:      iv.Start.Format(time.RFC3339),
						End:        iv.End.Format(time.RFC3339),
						Duration:   iv.End.Sub(iv.Start).String(),
						Kind:       coverageKindStr(iv.Kind),
						Resolution: iv.Resolution,
					})
				}
			}
			return f.WriteJSON(jsonOut)
		}

		headers := []string{"Key", "Start", "End", "Duration", "Kind", "Resolution"}
		var rows [][]string
		for _, entry := range filtered {
			for _, iv := range entry.intervals {
				res := ""
				if iv.Resolution > 0 {
					res = fmt.Sprintf("%ds", iv.Resolution)
				}
				rows = append(rows, []string{
					entry.key,
					iv.Start.Format(time.RFC3339),
					iv.End.Format(time.RFC3339),
					iv.End.Sub(iv.Start).Round(time.Second).String(),
					coverageKindStr(iv.Kind),
					res,
				})
			}
		}
		f.WriteTable(headers, rows)
		return nil
	}

	if cmd.Intervals {
		return cmd.renderIntervals(c, f, entries)
	}

	// Summary mode
	now := time.Now()

	// Load config for retention windows and expected stream enumeration
	_ = godotenv.Load()
	cfg, err := config.Load(c.Config)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}
	metricWindowStart := now.Add(-cfg.Collect.GapFill.MetricRetention)
	logWindowStart := now.Add(-cfg.Collect.GapFill.LogRetention)

	// Build name resolver from discovery cache
	resolver := buildNameResolver(reader)

	// Enumerate expected coverage keys from config + discovery
	targets := extractDiscoveryTargets(reader)
	expected := expectedCoverageKeys(cfg, targets)

	// Inject synthetic empty entries for expected keys missing from the database
	actualKeys := make(map[string]struct{})
	for _, e := range entries {
		actualKeys[e.Key] = struct{}{}
	}
	emptyIntervals, _ := json.Marshal([]collector.CoverageInterval{})
	for key := range expected {
		if _, exists := actualKeys[key]; !exists {
			entries = append(entries, state.RawEntry{Key: key, Value: emptyIntervals})
		}
	}

	// Partition entries by stream type and compute summaries with appropriate windows
	var metricEntries, logEntries []state.RawEntry
	for _, e := range entries {
		if isLogCoverageKey(e.Key) {
			logEntries = append(logEntries, e)
		} else {
			metricEntries = append(metricEntries, e)
		}
	}

	var summaries []coverageSummary
	summaries = append(summaries, buildCoverageSummaries(metricEntries, metricWindowStart, now, resolver)...)
	summaries = append(summaries, buildCoverageSummaries(logEntries, logWindowStart, now, resolver)...)

	if c.JSON {
		var jsonOut []coverageSummaryJSON
		for _, s := range summaries {
			largestGapStr := "0s"
			if s.LargestGap > 0 {
				largestGapStr = s.LargestGap.Round(time.Second).String()
			}
			jsonOut = append(jsonOut, coverageSummaryJSON{
				Key:        s.Key,
				Collected:  s.Collected.Round(time.Second).String(),
				Empty:      s.Empty.Round(time.Second).String(),
				Gaps:       s.Gaps.Round(time.Second).String(),
				GapCount:   s.GapCount,
				LargestGap: largestGapStr,
				Coverage:   fmt.Sprintf("%.1f%%", s.Percentage),
			})
		}
		return f.WriteJSON(jsonOut)
	}

	tb := cli.NewTreeBuilder()
	for _, s := range summaries {
		path := parseCoverageSegments(s.Key)
		tb.Add(path, &cli.NodeStats{
			Coverage:   s.Percentage,
			GapCount:   s.GapCount,
			LargestGap: s.LargestGap,
			Collected:  s.Collected,
			Total:      s.Collected + s.Empty + s.Gaps,
		})
	}
	tree := tb.Build()
	title := fmt.Sprintf("Coverage Report - %d streams, %s window",
		len(summaries), cli.FormatDuration(now.Sub(metricWindowStart)))
	cli.RenderTree(os.Stdout, tree, title)
	return nil
}

func (cmd *CoverageCmd) renderIntervals(c *CLI, f *cli.Formatter, entries []state.RawEntry) error {
	if c.JSON {
		var jsonOut []coverageIntervalJSON
		for _, e := range entries {
			var intervals []collector.CoverageInterval
			if err := json.Unmarshal(e.Value, &intervals); err != nil {
				continue
			}
			for _, iv := range intervals {
				jsonOut = append(jsonOut, coverageIntervalJSON{
					Key:        e.Key,
					Start:      iv.Start.Format(time.RFC3339),
					End:        iv.End.Format(time.RFC3339),
					Duration:   iv.End.Sub(iv.Start).String(),
					Kind:       coverageKindStr(iv.Kind),
					Resolution: iv.Resolution,
				})
			}
		}
		return f.WriteJSON(jsonOut)
	}

	headers := []string{"Key", "Start", "End", "Duration", "Kind", "Resolution"}
	var rows [][]string
	for _, e := range entries {
		var intervals []collector.CoverageInterval
		if err := json.Unmarshal(e.Value, &intervals); err != nil {
			continue
		}
		for _, iv := range intervals {
			res := ""
			if iv.Resolution > 0 {
				res = fmt.Sprintf("%ds", iv.Resolution)
			}
			rows = append(rows, []string{
				e.Key,
				iv.Start.Format(time.RFC3339),
				iv.End.Format(time.RFC3339),
				iv.End.Sub(iv.Start).Round(time.Second).String(),
				coverageKindStr(iv.Kind),
				res,
			})
		}
	}
	f.WriteTable(headers, rows)
	return nil
}

func coverageKindStr(k collector.CoverageKind) string {
	switch k {
	case collector.CoverageCollected:
		return "collected"
	case collector.CoverageEmpty:
		return "empty"
	default:
		return fmt.Sprintf("unknown(%d)", k)
	}
}

// computeGaps finds uncovered time between sorted intervals.
func computeGaps(intervals []collector.CoverageInterval) []collector.TimeRange {
	if len(intervals) < 2 {
		return nil
	}
	var gaps []collector.TimeRange
	for i := 1; i < len(intervals); i++ {
		if intervals[i].Start.After(intervals[i-1].End) {
			gaps = append(gaps, collector.TimeRange{
				Start: intervals[i-1].End,
				End:   intervals[i].Start,
			})
		}
	}
	return gaps
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
