package sink

import (
	"context"
	"time"
)

// MetricPoint represents a single metric data point to be sent to a sink.
type MetricPoint struct {
	Name      string
	Value     float64
	Timestamp time.Time
	Labels    map[string]string
}

// LogEntry represents a log line to be sent to a sink.
type LogEntry struct {
	Timestamp time.Time
	Message   string
	Severity  string
	Labels    map[string]string
	// Structured attributes from Railway
	Attributes map[string]string
}

// Sink is the interface that all metric/log backends implement.
type Sink interface {
	// Name returns the sink's display name.
	Name() string
	// WriteMetrics sends metric data points to the backend.
	WriteMetrics(ctx context.Context, metrics []MetricPoint) error
	// WriteLogs sends log entries to the backend.
	WriteLogs(ctx context.Context, logs []LogEntry) error
	// Close gracefully shuts down the sink.
	Close() error
}
