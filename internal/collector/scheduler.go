package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

// BudgetCalculator computes collection intervals from the current target set.
// This is injected so main.go can own the config-aware budget logic while
// the Scheduler only knows about the resulting intervals.
type BudgetCalculator func(targets []ServiceTarget) BudgetResult

// BudgetResult holds the intervals computed by the budget calculator.
type BudgetResult struct {
	MetricsInterval time.Duration
	LogsInterval    time.Duration
}

// SchedulerConfig configures the collection scheduler.
type SchedulerConfig struct {
	Clock             clockwork.Clock
	API               RailwayAPI
	Discovery         TargetProvider
	Metrics           Collector // nil if metrics collection disabled
	Logs              Collector // nil if logs collection disabled
	Backfill          Collector // nil if backfill disabled
	MetricsInterval   time.Duration
	LogsInterval      time.Duration
	DiscoveryInterval time.Duration // zero disables discovery refresh
	BackfillInterval  time.Duration // how often to run backfill (e.g. 30m)
	Budget            BudgetCalculator
	Logger            *slog.Logger
}

// Scheduler orchestrates periodic collection of metrics and logs with
// overlap protection, rate-limit awareness, and two-phase graceful shutdown.
type Scheduler struct {
	cfg SchedulerConfig

	// Mutable state protected by mu
	mu              sync.Mutex
	metricsInterval time.Duration
	logsInterval    time.Duration

	// Two-phase shutdown: Stop() signals the run loop to stop tickers,
	// then waits for in-flight work to drain.
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewScheduler creates a Scheduler from the given config.
func NewScheduler(cfg SchedulerConfig) *Scheduler {
	return &Scheduler{
		cfg:             cfg,
		metricsInterval: cfg.MetricsInterval,
		logsInterval:    cfg.LogsInterval,
		stopCh:          make(chan struct{}),
	}
}

// Run starts the collection loop and blocks until Stop() is called or ctx
// is cancelled. It returns after all in-flight collections have drained
// (or the drain timeout expires).
//
// Two-phase shutdown:
//
//	Phase 1: Stop() is called -- tickers stop, no new work is spawned,
//	         in-flight goroutines get up to 5s to finish naturally.
//	Phase 2: If drain times out, the caller cancels ctx to abort in-flight
//	         HTTP requests, then Run waits another 5s before returning.
func (s *Scheduler) Run(ctx context.Context) error {
	// Create tickers via clock (nil channels disable that select case)
	var metricsTicker, logsTicker, discoveryTicker clockwork.Ticker

	var metricsCh, logsCh, discoveryCh <-chan time.Time
	if s.cfg.Metrics != nil {
		metricsTicker = s.cfg.Clock.NewTicker(s.metricsInterval)
		defer metricsTicker.Stop()
		metricsCh = metricsTicker.Chan()
	}
	if s.cfg.Logs != nil {
		logsTicker = s.cfg.Clock.NewTicker(s.logsInterval)
		defer logsTicker.Stop()
		logsCh = logsTicker.Chan()
	}
	if s.cfg.DiscoveryInterval > 0 {
		discoveryTicker = s.cfg.Clock.NewTicker(s.cfg.DiscoveryInterval)
		defer discoveryTicker.Stop()
		discoveryCh = discoveryTicker.Chan()
	}

	var backfillTicker clockwork.Ticker
	var backfillCh <-chan time.Time
	if s.cfg.Backfill != nil && s.cfg.BackfillInterval > 0 {
		backfillTicker = s.cfg.Clock.NewTicker(s.cfg.BackfillInterval)
		defer backfillTicker.Stop()
		backfillCh = backfillTicker.Chan()
	}

	var wg sync.WaitGroup
	discoveryDone := make(chan struct{}, 1)

	// Overlap protection: skip a cycle if the previous one is still running.
	var metricsRunning, logsRunning, discoveryRunning, backfillRunning sync.Mutex

	// Fire immediate collection on start
	s.fireCollector(ctx, s.cfg.Metrics, &metricsRunning, &wg, "metrics", "startup")
	s.fireCollector(ctx, s.cfg.Logs, &logsRunning, &wg, "logs", "startup")
	s.fireCollector(ctx, s.cfg.Backfill, &backfillRunning, &wg, "backfill", "startup")

	for {
		select {
		case <-s.stopCh:
			// Phase 1: tickers already stopped by deferred Stop() calls above.
			// Wait for in-flight work to drain naturally.
			if s.drainWithTimeout(&wg, 5*time.Second) {
				s.cfg.Logger.Debug("all collections drained")
				return nil
			}
			// Phase 2: drain timed out. Block until ctx is cancelled (the
			// caller is expected to cancel it), then wait again.
			s.cfg.Logger.Warn("in-flight collections still running, waiting for context cancellation...")
			<-ctx.Done()
			if s.drainWithTimeout(&wg, 5*time.Second) {
				s.cfg.Logger.Debug("all collections drained after context cancel")
			} else {
				s.cfg.Logger.Warn("timed out waiting for in-flight collections after cancel")
			}
			return ctx.Err()

		case <-ctx.Done():
			// Hard shutdown without Stop() -- drain once and return.
			s.drainWithTimeout(&wg, 5*time.Second)
			return ctx.Err()

		case <-metricsCh:
			if limited, _ := s.cfg.API.IsRateLimited(); limited {
				remaining, resetAt := s.cfg.API.RateLimitInfo()
				s.cfg.Logger.Warn("skipping metrics collection, API rate limited",
					"remaining", remaining,
					"reset_at", resetAt.Format(time.RFC3339),
					"reset_in", time.Until(resetAt).Round(time.Second),
				)
				continue
			}
			s.fireCollector(ctx, s.cfg.Metrics, &metricsRunning, &wg, "metrics", "scheduled")

		case <-logsCh:
			if limited, _ := s.cfg.API.IsRateLimited(); limited {
				remaining, resetAt := s.cfg.API.RateLimitInfo()
				s.cfg.Logger.Warn("skipping logs collection, API rate limited",
					"remaining", remaining,
					"reset_at", resetAt.Format(time.RFC3339),
					"reset_in", time.Until(resetAt).Round(time.Second),
				)
				continue
			}
			s.fireCollector(ctx, s.cfg.Logs, &logsRunning, &wg, "logs", "scheduled")

		case <-backfillCh:
			if limited, _ := s.cfg.API.IsRateLimited(); limited {
				remaining, resetAt := s.cfg.API.RateLimitInfo()
				s.cfg.Logger.Warn("skipping backfill, API rate limited",
					"remaining", remaining,
					"reset_at", resetAt.Format(time.RFC3339),
					"reset_in", time.Until(resetAt).Round(time.Second),
				)
				continue
			}
			s.fireCollector(ctx, s.cfg.Backfill, &backfillRunning, &wg, "backfill", "scheduled")

		case <-discoveryCh:
			if limited, _ := s.cfg.API.IsRateLimited(); limited {
				remaining, resetAt := s.cfg.API.RateLimitInfo()
				s.cfg.Logger.Warn("skipping discovery refresh, API rate limited",
					"remaining", remaining,
					"reset_at", resetAt.Format(time.RFC3339),
					"reset_in", time.Until(resetAt).Round(time.Second),
				)
				continue
			}
			if !discoveryRunning.TryLock() {
				s.cfg.Logger.Warn("skipping discovery refresh, previous cycle still running")
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer discoveryRunning.Unlock()
				if err := s.cfg.Discovery.Refresh(ctx); err != nil {
					s.cfg.Logger.Error("discovery refresh failed", "error", err)
					return
				}
				select {
				case discoveryDone <- struct{}{}:
				default:
				}
			}()

		case <-discoveryDone:
			if s.cfg.Budget != nil {
				result := s.cfg.Budget(s.cfg.Discovery.Targets())
				s.mu.Lock()
				if metricsTicker != nil && significantChange(s.metricsInterval, result.MetricsInterval) {
					s.cfg.Logger.Info("adjusted metrics interval", "old", s.metricsInterval, "new", result.MetricsInterval, "targets", len(s.cfg.Discovery.Targets()))
					s.metricsInterval = result.MetricsInterval
					metricsTicker.Reset(result.MetricsInterval)
				}
				if logsTicker != nil && significantChange(s.logsInterval, result.LogsInterval) {
					s.cfg.Logger.Info("adjusted logs interval", "old", s.logsInterval, "new", result.LogsInterval, "targets", len(s.cfg.Discovery.Targets()))
					s.logsInterval = result.LogsInterval
					logsTicker.Reset(result.LogsInterval)
				}
				s.mu.Unlock()
			}
		}
	}
}

// Stop initiates graceful shutdown. It stops tickers (no new work will be
// spawned) and causes Run to begin draining in-flight goroutines.
// Safe to call multiple times. Does not block -- Run handles the drain.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// fireCollector launches a collection goroutine with overlap protection.
// trigger describes why this collection was initiated ("startup", "scheduled", etc.)
// and is embedded in the context so collectors can include it in their log output.
func (s *Scheduler) fireCollector(ctx context.Context, c Collector, running *sync.Mutex, wg *sync.WaitGroup, name, trigger string) {
	if c == nil {
		return
	}
	if !running.TryLock() {
		s.cfg.Logger.Warn("skipping collection, previous cycle still running", "collector", name, "trigger", trigger)
		return
	}
	s.cfg.Logger.Debug("firing collection", "collector", name, "trigger", trigger)
	wg.Add(1)
	triggerCtx := withTrigger(ctx, trigger)
	go func() {
		defer wg.Done()
		defer running.Unlock()
		if err := c.Collect(triggerCtx); err != nil {
			s.cfg.Logger.Error("collection failed", "collector", name, "trigger", trigger, "error", err)
		}
	}()
}

// drainWithTimeout waits for in-flight goroutines up to the given timeout.
// Returns true if all goroutines finished, false if the timeout expired.
func (s *Scheduler) drainWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-s.cfg.Clock.After(timeout):
		return false
	}
}

// significantChange reports whether two durations differ by more than 10%.
func significantChange(a, b time.Duration) bool {
	if a == 0 || b == 0 {
		return a != b
	}
	ratio := float64(a) / float64(b)
	return ratio < 0.9 || ratio > 1.1
}
