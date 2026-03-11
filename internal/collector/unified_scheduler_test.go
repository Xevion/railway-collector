package collector_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/credit"
	"github.com/xevion/railway-collector/internal/collector/mocks"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/railway"
	"go.uber.org/mock/gomock"
)

// countingHandler is a slog.Handler that counts log records matching a pattern.
type countingHandler struct {
	inner   slog.Handler
	pattern string
	count   *atomic.Int32
}

func (h *countingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *countingHandler) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.pattern) {
		h.count.Add(1)
	}
	return h.inner.Handle(ctx, r)
}

func (h *countingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &countingHandler{inner: h.inner.WithAttrs(attrs), pattern: h.pattern, count: h.count}
}

func (h *countingHandler) WithGroup(name string) slog.Handler {
	return &countingHandler{inner: h.inner.WithGroup(name), pattern: h.pattern, count: h.count}
}

// stubGenerator implements TaskGenerator with configurable behavior for testing.
type stubGenerator struct {
	mu         sync.Mutex
	taskType   types.TaskType
	pollItems  []types.WorkItem
	deliveries []stubDelivery
	pollCount  atomic.Int32
}

type stubDelivery struct {
	Item types.WorkItem
	Data json.RawMessage
	Err  error
}

func (g *stubGenerator) Type() types.TaskType { return g.taskType }

func (g *stubGenerator) Poll(_ time.Time) []types.WorkItem {
	g.pollCount.Add(1)
	g.mu.Lock()
	defer g.mu.Unlock()
	items := g.pollItems
	g.pollItems = nil // consume items on first poll
	return items
}

func (g *stubGenerator) NextPoll() time.Time { return time.Time{} }

func (g *stubGenerator) Deliver(_ context.Context, item types.WorkItem, data json.RawMessage, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.deliveries = append(g.deliveries, stubDelivery{Item: item, Data: data, Err: err})
}

func (g *stubGenerator) getDeliveries() []stubDelivery {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]stubDelivery, len(g.deliveries))
	copy(out, g.deliveries)
	return out
}

// waitForDeliveries polls until the expected number of deliveries arrive or timeout.
func (g *stubGenerator) waitForDeliveries(t *testing.T, n int, timeout time.Duration) []stubDelivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d := g.getDeliveries()
		if len(d) >= n {
			return d
		}
		time.Sleep(10 * time.Millisecond)
	}
	d := g.getDeliveries()
	require.Len(t, d, n, "timed out waiting for deliveries")
	return d
}

// newTestScheduler creates a UnifiedScheduler wired to the provided dependencies
// and a stable test configuration.
func newTestScheduler(
	t *testing.T,
	fakeClock clockwork.Clock,
	api types.RailwayAPI,
	credits *credit.CreditAllocator,
	logger *slog.Logger,
	generators ...types.TaskGenerator,
) *collector.UnifiedScheduler {
	t.Helper()
	return collector.NewUnifiedScheduler(collector.UnifiedSchedulerConfig{
		Clock:        fakeClock,
		API:          api,
		Credits:      credits,
		Generators:   generators,
		Logger:       logger,
		TickInterval: 100 * time.Millisecond,
		MaxRPS:       100.0,
	})
}

// newMetricsWorkItem creates a project-metrics WorkItem with standard test params.
// projectID is used as the AliasKey and in the ID field. startDate and endDate
// are ISO-8601 strings (e.g. "2025-01-01T00:00:00Z").
func newMetricsWorkItem(projectID, startDate, endDate string) types.WorkItem {
	return types.WorkItem{
		ID:       "metrics:" + projectID,
		Kind:     types.QueryMetrics,
		TaskType: types.TaskTypeMetrics,
		AliasKey: projectID,
		BatchKey: "sr=30",
		Params: map[string]any{
			"startDate":         startDate,
			"endDate":           endDate,
			"measurements":      []railway.MetricMeasurement{railway.MetricMeasurementCpuUsage},
			"groupBy":           []railway.MetricTag{railway.MetricTagServiceId},
			"sampleRateSeconds": 30,
		},
	}
}

func TestUnifiedScheduler_ExecutesBatchedMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType: types.TaskTypeMetrics,
		pollItems: []types.WorkItem{
			newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"),
			newMetricsWorkItem("proj-b", "2025-01-02T00:00:00Z", "2025-01-02T01:00:00Z"),
		},
	}

	aliasA := railway.SanitizeAlias("proj-a")
	aliasB := railway.SanitizeAlias("proj-b")

	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ map[string]any) (*railway.RawQueryResponse, error) {
			return &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{
					aliasA: json.RawMessage(`[{"measurement":"CPU_USAGE","tags":{},"values":[{"ts":1000,"value":0.5}]}]`),
					aliasB: json.RawMessage(`[{"measurement":"CPU_USAGE","tags":{},"values":[{"ts":2000,"value":0.7}]}]`),
				},
			}, nil
		}).Times(1)

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	// Wait for the ticker to be registered, then fire it.
	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	deliveries := gen.waitForDeliveries(t, 2, 2*time.Second)
	assert.Nil(t, deliveries[0].Err)
	assert.Nil(t, deliveries[1].Err)
	assert.Contains(t, string(deliveries[0].Data), "CPU_USAGE")
	assert.Contains(t, string(deliveries[1].Data), "CPU_USAGE")

	cancel()
}

func TestUnifiedScheduler_SkipsWhenNoCredits(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	// Create allocator with exhausted credits.
	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())
	credits.UpdateRegime(0, 1000, 3600) // exhausted regime, zeroes all rates

	// Drain any initial credit.
	credits.TryDeduct(types.TaskTypeMetrics, fakeClock.Now())
	credits.TryDeduct(types.TaskTypeMetrics, fakeClock.Now())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	// Should have polled but not delivered (no credits, exhausted regime).
	assert.Greater(t, gen.pollCount.Load(), int32(0))
	assert.Empty(t, gen.getDeliveries())

	cancel()
}

func TestUnifiedScheduler_DiscoverySpecialCase(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	disc := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClock()

	disc.EXPECT().Refresh(gomock.Any()).Return(nil).Times(1)
	disc.EXPECT().Targets().Return([]types.ServiceTarget{}).AnyTimes()

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	discoveryGen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: disc,
		Interval:  0,
		Logger:    slog.Default(),
	})

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), discoveryGen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)
	time.Sleep(500 * time.Millisecond) // give time for Refresh to be called

	cancel()
	// gomock verifies Refresh was called exactly once (not RawQuery).
}

func TestUnifiedScheduler_MixedTypeBatching(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	metricsGen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	logsGen := &stubGenerator{
		taskType: types.TaskTypeLogs,
		pollItems: []types.WorkItem{
			{
				ID: "envlogs:env-a", Kind: types.QueryEnvironmentLogs,
				TaskType: types.TaskTypeLogs, AliasKey: "env-a",
				BatchKey: "limit=500",
				Params: map[string]any{
					"afterDate":  "2024-01-01T00:00:00Z",
					"afterLimit": 500,
				},
			},
		},
	}

	aliasA := railway.SanitizeAlias("proj-a")
	envAlias := railway.SanitizeAlias("env-a")

	// Both metrics and logs should be packed into one request.
	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ map[string]any) (*railway.RawQueryResponse, error) {
			return &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{
					aliasA:   json.RawMessage(`[]`),
					envAlias: json.RawMessage(`[]`),
				},
			}, nil
		}).Times(1)

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), metricsGen, logsGen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	// Both metrics and logs should be delivered in one tick.
	metricsDeliveries := metricsGen.waitForDeliveries(t, 1, 2*time.Second)
	logDeliveries := logsGen.waitForDeliveries(t, 1, 2*time.Second)
	assert.Len(t, metricsDeliveries, 1)
	assert.Len(t, logDeliveries, 1)
	assert.Nil(t, metricsDeliveries[0].Err)
	assert.Nil(t, logDeliveries[0].Err)

	cancel()
}

func TestUnifiedScheduler_StopCancelsLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := collector.NewUnifiedScheduler(collector.UnifiedSchedulerConfig{
		Clock:      fakeClock,
		API:        api,
		Credits:    credits,
		Generators: nil,
		Logger:     slog.Default(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- s.Run(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	s.Stop()

	select {
	case err := <-runDone:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Stop()")
	}
}

func TestUnifiedScheduler_ContextCancelReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := collector.NewUnifiedScheduler(collector.UnifiedSchedulerConfig{
		Clock:      fakeClock,
		API:        api,
		Credits:    credits,
		Generators: nil,
		Logger:     slog.Default(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- s.Run(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-runDone:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestUnifiedScheduler_UpdatesRateState(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	aliasA := railway.SanitizeAlias("proj-a")

	api.EXPECT().RawQuery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).AnyTimes()

	// remaining=5 out of estimated 1000 = 0.5% -> Scarce.
	api.EXPECT().RateLimitInfo().Return(5, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	// Wait for delivery (confirms the tick executed).
	gen.waitForDeliveries(t, 1, 2*time.Second)

	assert.Equal(t, credit.RegimeScarce, credits.Regime())

	cancel()
}

func TestUnifiedScheduler_DefaultConfig(t *testing.T) {
	// Verify that zero-value config fields get defaults applied.
	// TickInterval=0 -> 1s, MaxRPS=0 -> 16/60, DrainTimeout=0 -> 5s.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	aliasA := railway.SanitizeAlias("proj-a")

	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).Times(1)

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	// Create scheduler with all zero config values to test defaults.
	s := collector.NewUnifiedScheduler(collector.UnifiedSchedulerConfig{
		Clock:      fakeClock,
		API:        api,
		Credits:    credits,
		Generators: []types.TaskGenerator{gen},
		Logger:     slog.Default(),
		// TickInterval, MaxRPS, DrainTimeout all zero -> defaults.
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	// Default TickInterval is 1s, so advancing 500ms should NOT trigger a tick.
	fakeClock.BlockUntil(1)
	fakeClock.Advance(500 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, gen.getDeliveries(), "should not tick before 1s default interval")

	// Advance past 1s total to trigger the tick.
	fakeClock.Advance(600 * time.Millisecond)
	deliveries := gen.waitForDeliveries(t, 1, 2*time.Second)
	assert.Len(t, deliveries, 1)
	assert.Nil(t, deliveries[0].Err)

	cancel()
}

func TestUnifiedScheduler_NonDefaultConfig(t *testing.T) {
	// Verify that non-zero config values are preserved (not overwritten by defaults).
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	aliasA := railway.SanitizeAlias("proj-a")
	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).Times(1)

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	// Explicit non-zero values.
	s := collector.NewUnifiedScheduler(collector.UnifiedSchedulerConfig{
		Clock:        fakeClock,
		API:          api,
		Credits:      credits,
		Generators:   []types.TaskGenerator{gen},
		Logger:       slog.Default(),
		TickInterval: 2 * time.Second,
		MaxRPS:       50.0,
		DrainTimeout: 10 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	// With 2s tick interval, 1.5s advance should NOT trigger a tick.
	fakeClock.BlockUntil(1)
	fakeClock.Advance(1500 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Empty(t, gen.getDeliveries(), "should not tick before 2s custom interval")

	// Advance past 2s total.
	fakeClock.Advance(600 * time.Millisecond)
	deliveries := gen.waitForDeliveries(t, 1, 2*time.Second)
	assert.Len(t, deliveries, 1)

	cancel()
}

func TestUnifiedScheduler_EmptyGeneratorPoll(t *testing.T) {
	// When a generator returns empty items, tick should not call RawQuery.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: nil, // empty poll
	}

	// RawQuery should NOT be called.
	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)
	time.Sleep(200 * time.Millisecond)

	assert.Greater(t, gen.pollCount.Load(), int32(0), "generator should have been polled")
	assert.Empty(t, gen.getDeliveries(), "no deliveries expected for empty poll")

	cancel()
}

func TestUnifiedScheduler_SingleFragmentComputationLimitNoRetry(t *testing.T) {
	// When a single-fragment batch hits "Problem processing request",
	// it should NOT retry individually (len(req.Fragments) > 1 is false).
	// The error should be delivered directly to the generator.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType: types.TaskTypeMetrics,
		pollItems: []types.WorkItem{
			newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"),
		},
	}

	callCount := atomic.Int32{}
	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ map[string]any) (*railway.RawQueryResponse, error) {
			callCount.Add(1)
			return nil, fmt.Errorf("Problem processing request: computation limit exceeded")
		}).Times(1) // exactly 1 call - no retry

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	deliveries := gen.waitForDeliveries(t, 1, 2*time.Second)
	assert.Len(t, deliveries, 1)
	assert.Error(t, deliveries[0].Err, "error should be delivered, not retried")
	assert.Contains(t, deliveries[0].Err.Error(), "Problem processing request")
	assert.Equal(t, int32(1), callCount.Load(), "should have made exactly 1 API call (no retry)")

	cancel()
}

func TestUnifiedScheduler_GraphQLErrorNoComputationMessage(t *testing.T) {
	// GraphQL errors without "Problem processing request" should NOT trigger
	// individual retry - error should be delivered via DispatchRequestResults.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType: types.TaskTypeMetrics,
		pollItems: []types.WorkItem{
			newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"),
			newMetricsWorkItem("proj-b", "2025-01-02T00:00:00Z", "2025-01-02T01:00:00Z"),
		},
	}

	callCount := atomic.Int32{}
	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ map[string]any) (*railway.RawQueryResponse, error) {
			callCount.Add(1)
			return &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{},
				Errors: []railway.GraphQLError{
					{Message: "Some other GraphQL error that is not computation related"},
				},
			}, nil
		}).Times(1) // exactly 1 call - no retry

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	deliveries := gen.waitForDeliveries(t, 2, 2*time.Second)
	assert.Len(t, deliveries, 2)
	// Both items should receive the error (not retried individually).
	for _, d := range deliveries {
		assert.Error(t, d.Err)
		assert.Contains(t, d.Err.Error(), "Some other GraphQL error")
	}
	assert.Equal(t, int32(1), callCount.Load(), "should NOT retry for non-computation errors")

	cancel()
}

func TestUnifiedScheduler_DiscoveryPiggybackWithQueries(t *testing.T) {
	// When both metrics and discovery items are present in the same tick,
	// both the query AND discovery.Refresh() should execute (piggyback behavior).
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	disc := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClock()

	disc.EXPECT().Refresh(gomock.Any()).Return(nil).Times(1)
	disc.EXPECT().Targets().Return([]types.ServiceTarget{}).AnyTimes()

	metricsGen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	discoveryGen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: disc,
		Interval:  0, // always ready
		Logger:    slog.Default(),
	})

	aliasA := railway.SanitizeAlias("proj-a")

	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).Times(1)

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), metricsGen, discoveryGen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	// Metrics should be delivered.
	metricsDeliveries := metricsGen.waitForDeliveries(t, 1, 2*time.Second)
	assert.Len(t, metricsDeliveries, 1)
	assert.Nil(t, metricsDeliveries[0].Err)

	// Give time for discovery refresh to complete (piggybacked after query).
	time.Sleep(500 * time.Millisecond)

	cancel()
	// gomock verifies Refresh was called exactly once (piggyback).
}

func TestUnifiedScheduler_PartialCreditDeduction(t *testing.T) {
	// Set up metrics + logs generators. Drain logs credits so only metrics
	// gets credited. Verify metrics items execute but logs are skipped.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	metricsGen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	logsGen := &stubGenerator{
		taskType: types.TaskTypeLogs,
		pollItems: []types.WorkItem{
			{
				ID: "envlogs:env-a", Kind: types.QueryEnvironmentLogs,
				TaskType: types.TaskTypeLogs, AliasKey: "env-a",
				BatchKey: "limit=500",
				Params: map[string]any{
					"afterDate":  "2024-01-01T00:00:00Z",
					"afterLimit": 500,
				},
			},
		},
	}

	aliasA := railway.SanitizeAlias("proj-a")

	// Only metrics should be in the query (logs skipped).
	api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _, _ string, _ map[string]any) (*railway.RawQueryResponse, error) {
			return &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{
					aliasA: json.RawMessage(`[]`),
				},
			}, nil
		}).Times(1)

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())
	// Exhaust logs credits by switching to exhausted regime, draining, then back.
	credits.UpdateRegime(0, 1000, 3600) // exhausted - zeroes all rates
	credits.TryDeduct(types.TaskTypeLogs, fakeClock.Now())
	credits.TryDeduct(types.TaskTypeLogs, fakeClock.Now())
	// Restore metrics rate but keep logs drained.
	credits.UpdateRegime(600, 1000, 3600) // abundant regime (restores rates)
	// Now immediately drain logs credit (it starts with tokens from regime change).
	for credits.TryDeduct(types.TaskTypeLogs, fakeClock.Now()) {
		// drain all logs credits
	}
	// Ensure metrics has a credit.
	// The abundant regime restored metrics rate, and pool starts with some tokens.

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), metricsGen, logsGen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	// Metrics should be delivered.
	metricsDeliveries := metricsGen.waitForDeliveries(t, 1, 2*time.Second)
	assert.Len(t, metricsDeliveries, 1)
	assert.Nil(t, metricsDeliveries[0].Err)

	// Logs should NOT be delivered (no credits).
	time.Sleep(300 * time.Millisecond)
	assert.Empty(t, logsGen.getDeliveries(), "logs should be skipped due to no credits")

	cancel()
}

func TestUnifiedScheduler_UpdateRateState_ExhaustedRemaining(t *testing.T) {
	// When remaining=0 after a query, the rate limiter should be set to minimum (0.001).
	// Verify by checking the regime becomes Exhausted.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	aliasA := railway.SanitizeAlias("proj-a")

	api.EXPECT().RawQuery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).AnyTimes()

	// remaining=0 -> exhausted regime, rate limiter set to 0.001.
	api.EXPECT().RateLimitInfo().Return(0, fakeClock.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	gen.waitForDeliveries(t, 1, 2*time.Second)

	assert.Equal(t, credit.RegimeExhausted, credits.Regime())

	cancel()
}

func TestUnifiedScheduler_UpdateRateState_HighRemaining(t *testing.T) {
	// remaining=900, resetAt=now+3600s. availableRate = 900/3600 = 0.25.
	// targetRate = 0.25 * 0.9 = 0.225. MaxRPS default = 16/60 ~ 0.267.
	// Since targetRate < MaxRPS, limiter should be set to targetRate.
	// Verify the regime becomes Abundant (900/1000 = 90%).
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	aliasA := railway.SanitizeAlias("proj-a")

	api.EXPECT().RawQuery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).AnyTimes()

	// 900 remaining out of estimated 1000 = 90% -> Abundant.
	api.EXPECT().RateLimitInfo().Return(900, fakeClock.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	// Use default MaxRPS (16/60 ~ 0.267) by passing zero.
	s := collector.NewUnifiedScheduler(collector.UnifiedSchedulerConfig{
		Clock:        fakeClock,
		API:          api,
		Credits:      credits,
		Generators:   []types.TaskGenerator{gen},
		Logger:       slog.Default(),
		TickInterval: 100 * time.Millisecond,
		// MaxRPS: 0 -> default 16/60 ~ 0.267
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	gen.waitForDeliveries(t, 1, 2*time.Second)

	// Regime should be abundant with 90% remaining.
	assert.Equal(t, credit.RegimeAbundant, credits.Regime())

	cancel()
}

func TestUnifiedScheduler_UpdateRateState_RateCappedAtMaxRPS(t *testing.T) {
	// remaining=50000, resetAt=now+10s. availableRate = 50000/10 = 5000.
	// targetRate = 5000 * 0.9 = 4500. MaxRPS = 100.
	// Since targetRate > MaxRPS, limiter should be capped at MaxRPS.
	// Verify the regime stays Abundant.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	aliasA := railway.SanitizeAlias("proj-a")

	api.EXPECT().RawQuery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).AnyTimes()

	// Very high remaining with short reset -> computed rate >> MaxRPS.
	api.EXPECT().RateLimitInfo().Return(50000, fakeClock.Now().Add(10*time.Second)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	gen.waitForDeliveries(t, 1, 2*time.Second)

	// With 50000 remaining out of estimated 1000 limit, ratio > 50% -> Abundant.
	assert.Equal(t, credit.RegimeAbundant, credits.Regime())

	cancel()
}

func TestUnifiedScheduler_UpdateRateState_LowRemainingPositive(t *testing.T) {
	// remaining=1, resetAt=now+3600s. availableRate = 1/3600 ~ 0.000278.
	// targetRate = 0.000278 * 0.9 ~ 0.00025. Min is 0.01, so should be clamped to 0.01.
	// remaining=1 out of 1000 = 0.1% -> Exhausted (remaining <= 0 check is false,
	// but ratio < 0.10 -> Scarce). Actually ratio = 0.001 < 0.10, so Scarce.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: []types.WorkItem{newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z")},
	}

	aliasA := railway.SanitizeAlias("proj-a")

	api.EXPECT().RawQuery(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&railway.RawQueryResponse{
			Data: map[string]json.RawMessage{
				aliasA: json.RawMessage(`[]`),
			},
		}, nil).AnyTimes()

	// remaining=1 -> scarce (0.1%), not exhausted (remaining > 0).
	api.EXPECT().RateLimitInfo().Return(1, fakeClock.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)

	gen.waitForDeliveries(t, 1, 2*time.Second)

	// remaining=1, ratio=0.001 < 0.10 -> Scarce (NOT Exhausted).
	assert.Equal(t, credit.RegimeScarce, credits.Regime())

	cancel()
}

func TestUnifiedScheduler_IdleLogThrottling(t *testing.T) {
	// Verify logIdleStatus respects the 30s idle log interval.
	// Multiple ticks within 30s should not flood idle logs.
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	fakeClock := clockwork.NewFakeClock()

	gen := &stubGenerator{
		taskType:  types.TaskTypeMetrics,
		pollItems: nil, // always empty -> idle path
	}

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

	// Use a recording logger to count idle log messages.
	var idleLogCount atomic.Int32
	logHandler := &countingHandler{
		inner:   slog.Default().Handler(),
		pattern: "scheduler idle",
		count:   &idleLogCount,
	}
	logger := slog.New(logHandler)

	s := newTestScheduler(t, fakeClock, api, credits, logger, gen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	// First tick: should log idle.
	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	// Second tick within 30s: should NOT log idle again.
	fakeClock.Advance(200 * time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	firstCount := idleLogCount.Load()

	// Third tick: advance past 30s total -> should log idle again.
	fakeClock.Advance(31 * time.Second)
	time.Sleep(100 * time.Millisecond)

	secondCount := idleLogCount.Load()
	assert.Greater(t, secondCount, firstCount, "should log idle again after 30s interval")

	cancel()
}

func TestUnifiedScheduler_DiscoveryOnlyNoCredits(t *testing.T) {
	// When only discovery items are present but discovery has no credits,
	// the scheduler should go idle (not crash or call Refresh).
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	disc := mocks.NewMockTargetProvider(ctrl)
	fakeClock := clockwork.NewFakeClock()

	// Refresh should NOT be called.
	disc.EXPECT().Targets().Return([]types.ServiceTarget{}).AnyTimes()

	api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(time.Hour)).AnyTimes()

	discoveryGen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: disc,
		Interval:  0,
		Logger:    slog.Default(),
	})

	credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())
	// Exhaust discovery credits.
	credits.UpdateRegime(0, 1000, 3600) // exhausted regime
	credits.TryDeduct(types.TaskTypeDiscovery, fakeClock.Now())
	credits.TryDeduct(types.TaskTypeDiscovery, fakeClock.Now())

	s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), discoveryGen)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.Run(ctx)

	fakeClock.BlockUntil(1)
	fakeClock.Advance(200 * time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	cancel()
	// gomock verifies Refresh was NOT called (no expectation set for it).
}

func TestUnifiedScheduler_RetriesOnComputationLimit(t *testing.T) {
	aliasA := railway.SanitizeAlias("proj-a")
	aliasB := railway.SanitizeAlias("proj-b")

	cases := []struct {
		name          string
		firstResponse *railway.RawQueryResponse
		firstErr      error
	}{
		{
			name:     "transport error",
			firstErr: fmt.Errorf("Problem processing request: computation limit exceeded"),
		},
		{
			name: "graphql error body",
			firstResponse: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{},
				Errors: []railway.GraphQLError{
					{Message: "Problem processing request: too complex"},
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			api := mocks.NewMockRailwayAPI(ctrl)
			fakeClock := clockwork.NewFakeClock()

			gen := &stubGenerator{
				taskType: types.TaskTypeMetrics,
				pollItems: []types.WorkItem{
					newMetricsWorkItem("proj-a", "2025-01-01T00:00:00Z", "2025-01-01T01:00:00Z"),
					newMetricsWorkItem("proj-b", "2025-01-02T00:00:00Z", "2025-01-02T01:00:00Z"),
				},
			}

			callCount := atomic.Int32{}
			api.EXPECT().RawQuery(gomock.Any(), "Batch", gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _, _ string, _ map[string]any) (*railway.RawQueryResponse, error) {
					n := callCount.Add(1)
					if n == 1 {
						return tc.firstResponse, tc.firstErr
					}
					// Subsequent individual retries succeed.
					return &railway.RawQueryResponse{
						Data: map[string]json.RawMessage{
							aliasA: json.RawMessage(`[]`),
							aliasB: json.RawMessage(`[]`),
						},
					}, nil
				}).MinTimes(2)

			// Return a short reset window so updateRateState keeps the rate limiter fast (near MaxRPS).
			api.EXPECT().RateLimitInfo().Return(500, time.Now().Add(5*time.Second)).AnyTimes()

			credits := credit.NewCreditAllocator(testCreditConfig, fakeClock.Now(), slog.Default())

			s := newTestScheduler(t, fakeClock, api, credits, slog.Default(), gen)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go s.Run(ctx)

			fakeClock.BlockUntil(1)
			fakeClock.Advance(200 * time.Millisecond)

			deliveries := gen.waitForDeliveries(t, 2, 5*time.Second)
			assert.Len(t, deliveries, 2)
			assert.GreaterOrEqual(t, callCount.Load(), int32(2))

			cancel()
		})
	}
}
