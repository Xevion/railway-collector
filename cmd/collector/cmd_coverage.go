package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/xevion/railway-collector/internal/cli"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/state"
)

// CoverageCmd summarizes backfill coverage per project.
type CoverageCmd struct {
	Project   string `help:"Show detailed intervals for a specific project ID."`
	Intervals bool   `help:"Show individual intervals instead of summary."`
}

type coverageSummaryJSON struct {
	Key       string `json:"key"`
	Collected string `json:"collected"`
	Empty     string `json:"empty"`
	Gaps      int    `json:"gaps"`
	Largest   string `json:"largest_gap"`
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

	if len(entries) == 0 {
		fmt.Println("No coverage data found.")
		return nil
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

	// Summary mode
	type summary struct {
		key       string
		collected time.Duration
		empty     time.Duration
		gaps      int
		largest   time.Duration
	}

	var summaries []summary
	for _, e := range entries {
		var intervals []collector.CoverageInterval
		if err := json.Unmarshal(e.Value, &intervals); err != nil {
			continue
		}

		var s summary
		s.key = e.Key
		for _, iv := range intervals {
			dur := iv.End.Sub(iv.Start)
			switch iv.Kind {
			case collector.CoverageCollected:
				s.collected += dur
			case collector.CoverageEmpty:
				s.empty += dur
			}
		}

		// Count gaps (uncovered time between intervals)
		gaps := computeGaps(intervals)
		s.gaps = len(gaps)
		for _, g := range gaps {
			if d := g.End.Sub(g.Start); d > s.largest {
				s.largest = d
			}
		}

		summaries = append(summaries, s)
	}

	if cmd.Intervals {
		return cmd.renderIntervals(c, f, entries)
	}

	if c.JSON {
		var jsonOut []coverageSummaryJSON
		for _, s := range summaries {
			largest := "-"
			if s.largest > 0 {
				largest = s.largest.Round(time.Second).String()
			}
			jsonOut = append(jsonOut, coverageSummaryJSON{
				Key:       s.key,
				Collected: s.collected.Round(time.Second).String(),
				Empty:     s.empty.Round(time.Second).String(),
				Gaps:      s.gaps,
				Largest:   largest,
			})
		}
		return f.WriteJSON(jsonOut)
	}

	headers := []string{"Key", "Collected", "Empty", "Gaps", "Largest Gap"}
	var rows [][]string
	for _, s := range summaries {
		largest := "-"
		if s.largest > 0 {
			largest = humanize.RelTime(time.Now().Add(-s.largest), time.Now(), "", "")
		}
		rows = append(rows, []string{
			s.key,
			s.collected.Round(time.Second).String(),
			s.empty.Round(time.Second).String(),
			fmt.Sprintf("%d", s.gaps),
			largest,
		})
	}
	f.WriteTable(headers, rows)
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
