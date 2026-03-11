package collector_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
