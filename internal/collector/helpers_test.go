package collector_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/xevion/railway-collector/internal/collector/mocks"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/sink"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// testCreditConfig is a stable fixture used across scheduler tests.
var testCreditConfig = config.CreditsConfig{
	MetricsRate:   8.0,
	LogsRate:      6.0,
	DiscoveryRate: 1.0,
	UsageRate:     1.0,
	MaxCredits:    4.0,
}

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

// genTestEnv bundles mock dependencies shared by all generator tests.
type genTestEnv struct {
	Ctrl    *gomock.Controller
	Store   *mocks.MockStateStore
	Targets *mocks.MockTargetProvider
	Clock   *clockwork.FakeClock
	Now     time.Time
}

// setupGenTest creates a genTestEnv with standard mocks and a fake clock
// pinned to 2026-03-09 12:00:00 UTC.
func setupGenTest(t *testing.T) *genTestEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := mocks.NewMockStateStore(ctrl)
	targets := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClockAt(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC))
	return &genTestEnv{
		Ctrl:    ctrl,
		Store:   store,
		Targets: targets,
		Clock:   fakeClock,
		Now:     fakeClock.Now(),
	}
}

// testDeliverHandlesError verifies that calling Deliver with an error does not
// panic or write to sinks. makeGen receives a genTestEnv configured with a
// no-op recordingSink.
func testDeliverHandlesError(t *testing.T, targets []types.ServiceTarget, makeGen func(env *genTestEnv, s sink.Sink) types.TaskGenerator, item types.WorkItem) {
	t.Helper()
	env := setupGenTest(t)
	env.Targets.EXPECT().Targets().Return(targets).AnyTimes()
	fakeSink := &recordingSink{}
	gen := makeGen(env, fakeSink)
	gen.Deliver(context.Background(), item, nil, assert.AnError)
}

// testDeliverEmptyResults verifies that delivering empty JSON results updates
// coverage without panicking.
func testDeliverEmptyResults(t *testing.T, targets []types.ServiceTarget, makeGen func(env *genTestEnv) types.TaskGenerator, item types.WorkItem, emptyJSON []byte) {
	t.Helper()
	env := setupGenTest(t)
	env.Targets.EXPECT().Targets().Return(targets).AnyTimes()
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil)
	env.Store.EXPECT().SetCoverage(gomock.Any(), gomock.Any()).Return(nil)
	gen := makeGen(env)
	gen.Deliver(context.Background(), item, emptyJSON, nil)
}

// testRespectsInterval verifies that a generator returns items on first poll,
// nil on immediate second poll, and items again after the interval elapses.
func testRespectsInterval(t *testing.T, interval time.Duration, makeGen func(env *genTestEnv) types.TaskGenerator) {
	t.Helper()
	env := setupGenTest(t)
	env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
		{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1", DeploymentID: "dep-1"},
	}).AnyTimes()
	env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

	gen := makeGen(env)

	items := gen.Poll(env.Now)
	require.NotEmpty(t, items, "first poll should return items")

	items = gen.Poll(env.Now.Add(1 * time.Second))
	assert.Nil(t, items, "immediate second poll should return nil")

	items = gen.Poll(env.Now.Add(interval))
	require.NotEmpty(t, items, "poll after interval should return items")
}

// testDeliverInvalidJSON verifies that delivering malformed JSON does not panic
// or write to sinks. item must carry a valid QueryKind for the generator.
func testDeliverInvalidJSON(t *testing.T, targets []types.ServiceTarget, makeGen func(env *genTestEnv, s sink.Sink) types.TaskGenerator, item types.WorkItem) {
	t.Helper()
	env := setupGenTest(t)
	env.Targets.EXPECT().Targets().Return(targets).AnyTimes()
	sinkCalled := false
	fakeSink := &recordingSink{
		writeMetrics: func(_ context.Context, _ []sink.MetricPoint) error {
			sinkCalled = true
			return nil
		},
		writeLogs: func(_ context.Context, _ []sink.LogEntry) error {
			sinkCalled = true
			return nil
		},
	}
	gen := makeGen(env, fakeSink)
	gen.Deliver(context.Background(), item, []byte("{not valid json"), nil)
	assert.False(t, sinkCalled, "sink should not be called on invalid JSON")
}

// gapChunkCase defines a test case for gap-chunking behavior shared across generators.
type gapChunkCase struct {
	name            string
	retention       time.Duration
	chunkSize       time.Duration
	maxItems        int
	wantMinItems    int
	wantMaxOpenEnd  int // max items without endDate/beforeDate
	wantMinChunked  int // min items with endDate/beforeDate
	wantTotalCapped bool
}

// commonGapChunkCases returns test cases shared by metrics and log generators.
// The "gap exactly one chunk size" case is only used by project and service
// metrics (which have 6 cases each); the other generators use only these 5.
func commonGapChunkCases() []gapChunkCase {
	return []gapChunkCase{
		{
			name:           "7d live-edge gap with 6h chunks",
			retention:      7 * 24 * time.Hour,
			chunkSize:      6 * time.Hour,
			maxItems:       200,
			wantMinItems:   2,
			wantMaxOpenEnd: 1,
			wantMinChunked: 1,
		},
		{
			name:           "90d live-edge gap with 6h chunks",
			retention:      90 * 24 * time.Hour,
			chunkSize:      6 * time.Hour,
			maxItems:       500,
			wantMinItems:   2,
			wantMaxOpenEnd: 1,
			wantMinChunked: 10,
		},
		{
			name:           "gap smaller than chunk size stays single item",
			retention:      1 * time.Hour,
			chunkSize:      6 * time.Hour,
			maxItems:       50,
			wantMinItems:   1,
			wantMaxOpenEnd: 1,
			wantMinChunked: 0,
		},
		{
			name:            "maxItems caps output",
			retention:       7 * 24 * time.Hour,
			chunkSize:       6 * time.Hour,
			maxItems:        5,
			wantMinItems:    5,
			wantMaxOpenEnd:  1,
			wantMinChunked:  1,
			wantTotalCapped: true,
		},
		{
			name:           "1h chunks on 2d gap produces many items",
			retention:      2 * 24 * time.Hour,
			chunkSize:      1 * time.Hour,
			maxItems:       200,
			wantMinItems:   2,
			wantMaxOpenEnd: 1,
			wantMinChunked: 10,
		},
	}
}

// generatorFactory creates a TaskGenerator from a genTestEnv and gap-chunking params.
type generatorFactory func(env *genTestEnv, retention, chunkSize time.Duration, maxItems int) types.TaskGenerator

// runGapChunkTests runs the standard gap-chunking test suite for any generator.
// endDateKey is "endDate" for metrics or "beforeDate" for logs.
func runGapChunkTests(t *testing.T, cases []gapChunkCase, endDateKey string, makeGen generatorFactory) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			env := setupGenTest(t)
			env.Targets.EXPECT().Targets().Return([]types.ServiceTarget{
				{ProjectID: "proj-1", ProjectName: "one", ServiceID: "svc-1", ServiceName: "web", EnvironmentID: "env-1", EnvironmentName: "production"},
			})
			env.Store.EXPECT().GetCoverage(gomock.Any()).Return(nil, nil).AnyTimes()

			gen := makeGen(env, tt.retention, tt.chunkSize, tt.maxItems)
			items := gen.Poll(env.Now)
			require.GreaterOrEqual(t, len(items), tt.wantMinItems, "minimum item count")

			var chunked, openEnded int
			for _, item := range items {
				if _, hasEnd := item.Params[endDateKey]; hasEnd {
					chunked++
				} else {
					openEnded++
				}
			}

			assert.LessOrEqual(t, openEnded, tt.wantMaxOpenEnd, "open-ended item count")
			assert.GreaterOrEqual(t, chunked, tt.wantMinChunked, "chunked item count")

			if tt.wantTotalCapped {
				assert.Equal(t, tt.maxItems, len(items), "output should be capped at maxItems")
			}
		})
	}
}
