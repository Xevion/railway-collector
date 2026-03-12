package coverage

import (
	"fmt"
	"log/slog"
	"time"
)

// TimeWindow is a UTC-normalized time span that replaces the bare TimeRange
// struct. Exported fields allow drop-in use in struct literals and algorithm
// code, while constructors enforce UTC normalization.
//
// End is zero for open/live-edge windows where no endDate has been set.
// Use IsOpen, ResolvedEnd, or the Duration method accordingly.
type TimeWindow struct {
	Start time.Time // always UTC
	End   time.Time // always UTC; zero = open/live-edge (no endDate)
}

// NewTimeWindow creates a closed TimeWindow, normalizing both endpoints to UTC.
func NewTimeWindow(start, end time.Time) TimeWindow {
	return TimeWindow{Start: start.UTC(), End: end.UTC()}
}

// NewOpenWindow creates a live-edge TimeWindow with no fixed end, normalizing
// start to UTC.
func NewOpenWindow(start time.Time) TimeWindow {
	return TimeWindow{Start: start.UTC()}
}

// WindowFromInterval creates a TimeWindow from a CoverageInterval.
func WindowFromInterval(iv CoverageInterval) TimeWindow {
	return NewTimeWindow(iv.Start, iv.End)
}

// WindowFromParams extracts a TimeWindow from WorkItem params, normalizing to UTC.
// Supports both startDate/endDate (metrics) and afterDate/beforeDate (logs).
// Returns (zero, false) if start is absent or unparseable.
// If end is absent or unparseable, returns an open window.
func WindowFromParams(params map[string]any, now time.Time) (TimeWindow, bool) {
	startStr, _ := params["startDate"].(string)
	if startStr == "" {
		startStr, _ = params["afterDate"].(string)
	}
	if startStr == "" {
		return TimeWindow{}, false
	}
	start, err := parseFlexTime(startStr)
	if err != nil {
		return TimeWindow{}, false
	}

	endStr, _ := params["endDate"].(string)
	if endStr == "" {
		endStr, _ = params["beforeDate"].(string)
	}
	if endStr == "" {
		return NewOpenWindow(start), true
	}
	end, err := parseFlexTime(endStr)
	if err != nil {
		return NewOpenWindow(start), true
	}
	return NewTimeWindow(start, end), true
}

// parseFlexTime parses a time string, trying RFC3339Nano then RFC3339.
func parseFlexTime(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// IsOpen reports whether the window has no fixed end (live-edge).
func (w TimeWindow) IsOpen() bool { return w.End.IsZero() }

// IsZero reports whether the window is the zero value (unset start).
func (w TimeWindow) IsZero() bool { return w.Start.IsZero() }

// ResolvedEnd returns End if set, otherwise now.
func (w TimeWindow) ResolvedEnd(now time.Time) time.Time {
	if w.End.IsZero() {
		return now
	}
	return w.End
}

// Duration returns the window's span. Returns 0 for open windows; callers
// that need a meaningful duration for open windows should use ResolvedEnd.
func (w TimeWindow) Duration() time.Duration {
	if w.End.IsZero() {
		return 0
	}
	return w.End.Sub(w.Start)
}

// Age returns how long ago the window ended. Returns 0 for open windows.
// Used for recency scoring in PrioritizeGaps.
func (w TimeWindow) Age(now time.Time) time.Duration {
	if w.End.IsZero() {
		return 0
	}
	return now.Sub(w.End)
}

// Contains reports whether t falls within [Start, End). For open windows the
// upper bound is unbounded.
func (w TimeWindow) Contains(t time.Time) bool {
	t = t.UTC()
	if t.Before(w.Start) {
		return false
	}
	if !w.End.IsZero() && !t.Before(w.End) {
		return false
	}
	return true
}

// Shift returns a new TimeWindow with both endpoints shifted by d.
// For open windows only Start is shifted.
func (w TimeWindow) Shift(d time.Duration) TimeWindow {
	shifted := TimeWindow{Start: w.Start.Add(d)}
	if !w.End.IsZero() {
		shifted.End = w.End.Add(d)
	}
	return shifted
}

// Intersect returns the overlap of two windows. Returns (zero, false) if they
// do not overlap. Open windows are treated as unbounded on the right.
func (w TimeWindow) Intersect(other TimeWindow) (TimeWindow, bool) {
	start := w.Start
	if other.Start.After(start) {
		start = other.Start
	}

	// Both closed
	if !w.End.IsZero() && !other.End.IsZero() {
		end := w.End
		if other.End.Before(end) {
			end = other.End
		}
		if !start.Before(end) {
			return TimeWindow{}, false
		}
		return NewTimeWindow(start, end), true
	}

	// At least one is open — result is open
	if w.End.IsZero() && other.End.IsZero() {
		return NewOpenWindow(start), true
	}
	closedEnd := w.End
	if other.End.IsZero() {
		closedEnd = other.End
	}
	_ = closedEnd
	return NewOpenWindow(start), true
}

// Extend returns a new TimeWindow with End extended to t if t is after the
// current End. For open windows, sets End to t.
func (w TimeWindow) Extend(t time.Time) TimeWindow {
	t = t.UTC()
	if w.End.IsZero() || t.After(w.End) {
		return TimeWindow{Start: w.Start, End: t}
	}
	return w
}

// BatchKey returns a canonical UTC RFC3339 string pair for use as a map or
// batch deduplication key: "s=<start>,e=<end>". For open windows, omits the
// end component.
func (w TimeWindow) BatchKey() string {
	if w.End.IsZero() {
		return fmt.Sprintf("s=%s", w.Start.Format(time.RFC3339))
	}
	return fmt.Sprintf("s=%s,e=%s", w.Start.Format(time.RFC3339), w.End.Format(time.RFC3339))
}

// LogValue implements slog.LogValuer. Emits a compact slog group with two
// keys: "start" (UTC, precision scaled to duration) and "dur" (compact
// duration string). For open windows the duration is measured from Start
// to now at the time of logging.
//
// Example output:
//
//	window.start=2026-03-11T23:35:48Z window.dur=1m56s
//	window.start=2026-01-07T00:00Z    window.dur=10d14h
func (w TimeWindow) LogValue() slog.Value {
	var dur time.Duration
	if w.End.IsZero() {
		dur = time.Since(w.Start)
	} else {
		dur = w.End.Sub(w.Start)
	}
	return slog.GroupValue(
		slog.String("start", formatWindowStart(w.Start, dur)),
		slog.String("dur", formatWindowDuration(dur)),
	)
}

// String returns the same compact representation as LogValue for non-slog
// contexts: "<start>+<dur>".
func (w TimeWindow) String() string {
	var dur time.Duration
	if w.End.IsZero() {
		dur = time.Since(w.Start)
	} else {
		dur = w.End.Sub(w.Start)
	}
	return fmt.Sprintf("%s+%s", formatWindowStart(w.Start, dur), formatWindowDuration(dur))
}

// formatWindowStart formats a UTC start time with precision appropriate to
// the window's duration. Uses second precision for short windows, minute
// precision for longer ones.
func formatWindowStart(t time.Time, dur time.Duration) string {
	if dur < time.Hour {
		return t.Format("2006-01-02T15:04:05Z07:00")
	}
	return t.Format("2006-01-02T15:04Z07:00")
}

// formatWindowDuration formats a duration compactly, omitting zero-value
// subdivisions. Tiers: s / Xm Ys / Xh Ym / Xd Yh.
func formatWindowDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		if s == 0 {
			return fmt.Sprintf("%dm", m)
		}
		return fmt.Sprintf("%dm%ds", m, s)
	}
	if d < 24*time.Hour {
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	if hours == 0 {
		return fmt.Sprintf("%dd", days)
	}
	return fmt.Sprintf("%dd%dh", days, hours)
}
