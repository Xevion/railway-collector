package collector_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/mocks"
	"go.uber.org/mock/gomock"
)

// blockingCollector blocks until unblocked, counting calls.
type blockingCollector struct {
	calls   atomic.Int32
	unblock chan struct{}
}

func (b *blockingCollector) Collect(ctx context.Context) error {
	b.calls.Add(1)
	select {
	case <-b.unblock:
	case <-ctx.Done():
	}
	return nil
}

// simpleCollector runs a function on Collect.
type simpleCollector struct {
	fn func(context.Context) error
}

func (s *simpleCollector) Collect(ctx context.Context) error {
	return s.fn(ctx)
}

func TestScheduler_OverlapProtection(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	api.EXPECT().IsRateLimited().Return(false, time.Duration(0)).AnyTimes()

	fakeClock := clockwork.NewFakeClock()
	mc := &blockingCollector{unblock: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := collector.NewScheduler(collector.SchedulerConfig{
		Clock:           fakeClock,
		API:             api,
		Metrics:         mc,
		MetricsInterval: 1 * time.Minute,
		Logger:          slog.Default(),
	})

	go s.Run(ctx)

	// Wait for initial collection to start
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), mc.calls.Load())

	// Advance past the ticker -- should skip because first is still running
	fakeClock.Advance(2 * time.Minute)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(1), mc.calls.Load()) // still 1, skipped

	// Unblock the first collection
	close(mc.unblock)
	time.Sleep(50 * time.Millisecond)

	cancel()
}

func TestScheduler_RateLimitSkip(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	// Rate limited on all checks
	api.EXPECT().IsRateLimited().Return(true, 5*time.Minute).AnyTimes()
	api.EXPECT().RateLimitInfo().Return(0, time.Now().Add(5*time.Minute)).AnyTimes()

	fakeClock := clockwork.NewFakeClock()
	var collectCalls atomic.Int32
	mc := &simpleCollector{fn: func(ctx context.Context) error {
		collectCalls.Add(1)
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := collector.NewScheduler(collector.SchedulerConfig{
		Clock:           fakeClock,
		API:             api,
		Metrics:         mc,
		MetricsInterval: 1 * time.Minute,
		Logger:          slog.Default(),
	})

	go s.Run(ctx)

	// Initial collection fires (no rate limit check on initial)
	time.Sleep(50 * time.Millisecond)
	initialCalls := collectCalls.Load()

	// Advance clock -- ticker fires but rate limited, should skip
	fakeClock.Advance(2 * time.Minute)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, initialCalls, collectCalls.Load())

	cancel()
}

func TestScheduler_DiscoveryBudgetRecalc(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	api.EXPECT().IsRateLimited().Return(false, time.Duration(0)).AnyTimes()
	disc := mocks.NewMockTargetProvider(ctrl)
	disc.EXPECT().Targets().Return([]collector.ServiceTarget{}).AnyTimes()
	disc.EXPECT().Refresh(gomock.Any()).Return(nil).AnyTimes()

	fakeClock := clockwork.NewFakeClock()
	budgetCalled := make(chan struct{}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := collector.NewScheduler(collector.SchedulerConfig{
		Clock:             fakeClock,
		API:               api,
		Discovery:         disc,
		DiscoveryInterval: 5 * time.Minute,
		MetricsInterval:   1 * time.Minute,
		LogsInterval:      2 * time.Minute,
		Budget: func(targets []collector.ServiceTarget) collector.BudgetResult {
			budgetCalled <- struct{}{}
			return collector.BudgetResult{
				MetricsInterval: 3 * time.Minute,
				LogsInterval:    4 * time.Minute,
			}
		},
		Logger: slog.Default(),
	})

	go s.Run(ctx)

	// Let the Run loop enter the select
	time.Sleep(50 * time.Millisecond)

	// Advance past discovery interval — triggers discovery goroutine
	fakeClock.Advance(6 * time.Minute)

	// Wait for the discovery goroutine to complete and budget to be recalculated
	select {
	case <-budgetCalled:
		// Budget recalculation was triggered
	case <-time.After(2 * time.Second):
		t.Fatal("budget recalculation was not triggered after discovery")
	}

	cancel()
}

func TestScheduler_TwoPhaseShutdown(t *testing.T) {
	ctrl := gomock.NewController(t)
	api := mocks.NewMockRailwayAPI(ctrl)
	api.EXPECT().IsRateLimited().Return(false, time.Duration(0)).AnyTimes()

	fakeClock := clockwork.NewFakeClock()

	// Collector that blocks until context is cancelled (simulates slow HTTP)
	collectStarted := make(chan struct{})
	mc := &simpleCollector{fn: func(ctx context.Context) error {
		close(collectStarted)
		<-ctx.Done()
		return ctx.Err()
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := collector.NewScheduler(collector.SchedulerConfig{
		Clock:           fakeClock,
		API:             api,
		Metrics:         mc,
		MetricsInterval: 1 * time.Minute,
		Logger:          slog.Default(),
	})

	runDone := make(chan error, 1)
	go func() {
		runDone <- s.Run(ctx)
	}()

	// Wait for collection to start
	<-collectStarted

	// Phase 1: Stop() -- tickers stop, drain starts but won't finish
	// because collector is blocked on ctx.Done()
	s.Stop()

	// Advance fake clock past the 5s phase-1 drain timeout so Run
	// transitions to blocking on <-ctx.Done()
	time.Sleep(50 * time.Millisecond) // let Run enter drainWithTimeout
	fakeClock.Advance(6 * time.Second)

	// Run should NOT have returned yet -- it's now blocking on <-ctx.Done()
	select {
	case <-runDone:
		t.Fatal("Run returned before context was cancelled")
	case <-time.After(100 * time.Millisecond):
		// expected: Run is waiting for phase 2
	}

	// Phase 2: cancel context -- collector unblocks, Run drains and returns
	cancel()
	// Advance clock again for phase-2 drain timeout (though drain should
	// succeed immediately since the collector unblocked)
	fakeClock.Advance(1 * time.Second)

	select {
	case err := <-runDone:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}
