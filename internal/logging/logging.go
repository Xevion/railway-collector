package logging

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	humanize "github.com/dustin/go-humanize"
	slogformatter "github.com/samber/slog-formatter"
)

// LevelTrace is below Debug, for very noisy per-datapoint logs.
const LevelTrace = slog.LevelDebug - 4

// commaThreshold is the minimum absolute value at which integers get
// comma formatting. Values below this pass through as plain numbers.
const commaThreshold = 10_000

// Pct wraps a float64 for percentage display in structured logs.
// Pass it via slog.Any: slog.Any("match_rate", logging.Pct(94.3))
type Pct float64

func (p Pct) LogValue() slog.Value {
	v := float64(p)
	if v == float64(int64(v)) {
		return slog.StringValue(fmt.Sprintf("%d%%", int64(v)))
	}
	return slog.StringValue(fmt.Sprintf("%.1f%%", v))
}

// Formatters returns slog-formatter middleware that auto-formats durations,
// large integers, and percentages for human-readable log output.
func Formatters() []slogformatter.Formatter {
	return []slogformatter.Formatter{
		slogformatter.FormatByKind(slog.KindDuration, formatDuration),
		slogformatter.FormatByKind(slog.KindInt64, formatInt),
		slogformatter.FormatByType(func(p Pct) slog.Value {
			return p.LogValue()
		}),
	}
}

func formatDuration(v slog.Value) slog.Value {
	d := v.Duration()
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Second:
		return slog.StringValue(fmt.Sprintf("%dms", d.Milliseconds()))
	case d < time.Minute:
		return slog.StringValue(fmt.Sprintf("%.1fs", d.Seconds()))
	default:
		mins := int(d.Minutes())
		secs := int(d.Seconds()) % 60
		return slog.StringValue(fmt.Sprintf("%dm%02ds", mins, secs))
	}
}

func formatInt(v slog.Value) slog.Value {
	n := v.Int64()
	if n >= commaThreshold || n <= -commaThreshold {
		return slog.StringValue(humanize.Comma(n))
	}
	return v
}

// FilteringHandler wraps an slog.Handler and silently drops log records
// whose message contains any of the suppressed substrings.
type FilteringHandler struct {
	inner    slog.Handler
	suppress []string
}

func NewFilteringHandler(inner slog.Handler, suppress ...string) *FilteringHandler {
	return &FilteringHandler{inner: inner, suppress: suppress}
}

func (h *FilteringHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *FilteringHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, s := range h.suppress {
		if strings.Contains(r.Message, s) {
			return nil
		}
	}
	return h.inner.Handle(ctx, r)
}

func (h *FilteringHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &FilteringHandler{inner: h.inner.WithAttrs(attrs), suppress: h.suppress}
}

func (h *FilteringHandler) WithGroup(name string) slog.Handler {
	return &FilteringHandler{inner: h.inner.WithGroup(name), suppress: h.suppress}
}

// ReplaceAttrFunc returns a tint-compatible ReplaceAttr function that renders
// LevelTrace as "TRC" with ANSI color 13 (magenta).
func ReplaceAttrFunc(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey {
		level, ok := a.Value.Any().(slog.Level)
		if ok && level <= LevelTrace {
			a.Value = slog.StringValue("\033[38;5;13mTRC\033[0m")
		}
	}
	return a
}
