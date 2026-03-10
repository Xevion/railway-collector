package collector_test

import (
	"context"

	"github.com/xevion/railway-collector/internal/sink"
)

func strPtr(s string) *string { return &s }

// recordingSink captures sink writes for assertion.
type recordingSink struct {
	writeMetrics func(context.Context, []sink.MetricPoint) error
	writeLogs    func(context.Context, []sink.LogEntry) error
}

func (r *recordingSink) Name() string { return "test" }
func (r *recordingSink) WriteMetrics(ctx context.Context, m []sink.MetricPoint) error {
	if r.writeMetrics != nil {
		return r.writeMetrics(ctx, m)
	}
	return nil
}
func (r *recordingSink) WriteLogs(ctx context.Context, l []sink.LogEntry) error {
	if r.writeLogs != nil {
		return r.writeLogs(ctx, l)
	}
	return nil
}
func (r *recordingSink) Close() error { return nil }
