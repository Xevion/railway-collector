package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/time/rate"
)

// UnifiedSchedulerConfig configures the unified credit-based scheduler.
type UnifiedSchedulerConfig struct {
	Clock      clockwork.Clock
	API        RailwayAPI
	Credits    *CreditAllocator
	Generators []TaskGenerator
	Logger     *slog.Logger

	// TickInterval controls how often the scheduler polls generators for work.
	// Defaults to 1s if zero.
	TickInterval time.Duration

	// MaxRPS is the maximum requests per second. The rate limiter is set to
	// this initially and adjusted dynamically based on rate limit headers.
	// Defaults to 16.0/60 (~0.267 RPS) if zero.
	MaxRPS float64

	// DrainTimeout is how long to wait for in-flight work during shutdown.
	// Defaults to 5s if zero.
	DrainTimeout time.Duration
}

// UnifiedScheduler replaces the old ticker-based Scheduler with a single
// goroutine that polls generators, batches compatible work items, paces
// requests via a rate limiter, and delivers results back.
type UnifiedScheduler struct {
	cfg     UnifiedSchedulerConfig
	limiter *rate.Limiter

	// generatorsByType provides quick lookup of generators by their TaskType.
	generatorsByType map[TaskType]TaskGenerator

	stopCh   chan struct{}
	stopOnce sync.Once

	lastIdleLog time.Time // last time we logged an idle message
}

// NewUnifiedScheduler creates a new scheduler. The rate limiter is initialized
// to MaxRPS (or the default ~16/min).
func NewUnifiedScheduler(cfg UnifiedSchedulerConfig) *UnifiedScheduler {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = time.Second
	}
	if cfg.MaxRPS == 0 {
		cfg.MaxRPS = 16.0 / 60.0 // ~0.267 RPS = 16 requests per minute
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 5 * time.Second
	}

	genByType := make(map[TaskType]TaskGenerator, len(cfg.Generators))
	for _, g := range cfg.Generators {
		genByType[g.Type()] = g
	}

	return &UnifiedScheduler{
		cfg:              cfg,
		limiter:          rate.NewLimiter(rate.Limit(cfg.MaxRPS), 1),
		generatorsByType: genByType,
		stopCh:           make(chan struct{}),
	}
}

// Run starts the scheduler loop and blocks until Stop() is called or ctx
// is cancelled. It polls generators each tick, picks a credited batch,
// waits on the rate limiter, executes the query, and dispatches results.
func (s *UnifiedScheduler) Run(ctx context.Context) error {
	ticker := s.cfg.Clock.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			s.cfg.Logger.Debug("unified scheduler stopping")
			return nil

		case <-ctx.Done():
			s.cfg.Logger.Debug("unified scheduler context cancelled")
			return ctx.Err()

		case <-ticker.Chan():
			s.tick(ctx)
		}
	}
}

// Stop initiates graceful shutdown.
func (s *UnifiedScheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// tick runs one iteration of the scheduler loop.
func (s *UnifiedScheduler) tick(ctx context.Context) {
	now := s.cfg.Clock.Now()

	// 1. Poll all generators for pending work.
	var allItems []WorkItem
	// Track which generator produced each item for result delivery.
	generatorMap := make(map[string]TaskGenerator)

	for _, gen := range s.cfg.Generators {
		items := gen.Poll(now)
		for _, item := range items {
			allItems = append(allItems, item)
			generatorMap[item.ID] = gen
		}
	}

	if len(allItems) == 0 {
		s.logIdleStatus(now)
		return
	}

	// 2. Group into mergeable batches (sorted by priority).
	batches := GroupByCompatibility(allItems)
	if len(batches) == 0 {
		return
	}

	// 3. Select highest-priority batch with available credits.
	var selected *Batch
	for i := range batches {
		if s.cfg.Credits.TryDeduct(batches[i].TaskType, now) {
			selected = &batches[i]
			break
		}
	}
	if selected == nil {
		s.logIdleStatus(now)
		return
	}

	// 4. Wait for rate limiter.
	if err := s.limiter.Wait(ctx); err != nil {
		if ctx.Err() != nil {
			return
		}
		s.cfg.Logger.Warn("rate limiter wait failed", "error", err)
		return
	}

	// 5. Handle discovery specially (cascading genqlient calls, not raw query).
	if selected.Kind == QueryDiscovery {
		s.handleDiscovery(ctx, *selected, generatorMap)
		return
	}

	// 6. Build and execute the batched query.
	opName, query, vars, buildErr := BuildQueryFromBatch(*selected)
	if buildErr != nil {
		s.cfg.Logger.Error("failed to build batch query",
			"kind", selected.Kind, "error", buildErr)
		return
	}

	s.cfg.Logger.Debug("executing batch query",
		"operation", opName,
		"kind", selected.Kind,
		"aliases", len(selected.Items),
	)

	resp, queryErr := s.cfg.API.RawQuery(ctx, opName, query, vars)

	// 7. Update rate limit state from headers.
	s.updateRateState()

	// 8. Dispatch results back to generators.
	DispatchResults(ctx, *selected, resp, queryErr, generatorMap)
}

// handleDiscovery executes discovery refresh via the TargetProvider's Refresh
// method rather than building a raw query.
func (s *UnifiedScheduler) handleDiscovery(ctx context.Context, batch Batch, generatorMap map[string]TaskGenerator) {
	// Find the DiscoveryGenerator (it has a Refresh method).
	gen, ok := s.generatorsByType[TaskTypeDiscovery]
	if !ok {
		s.cfg.Logger.Error("discovery generator not found")
		return
	}

	// DiscoveryGenerator has a Refresh convenience method.
	if dg, ok := gen.(*DiscoveryGenerator); ok {
		err := dg.Refresh(ctx)
		for _, item := range batch.Items {
			if g, exists := generatorMap[item.ID]; exists {
				g.Deliver(ctx, item, nil, err)
			}
		}
	} else {
		s.cfg.Logger.Error("discovery generator does not support Refresh")
	}
}

const idleLogInterval = 30 * time.Second

// logIdleStatus logs a periodic summary of why the scheduler is idle,
// including each generator's next expected poll time.
func (s *UnifiedScheduler) logIdleStatus(now time.Time) {
	if now.Sub(s.lastIdleLog) < idleLogInterval {
		return
	}
	s.lastIdleLog = now

	// Find the earliest next poll across all generators.
	var earliest time.Time
	attrs := make([]any, 0, len(s.cfg.Generators)*2)
	for _, gen := range s.cfg.Generators {
		np := gen.NextPoll()
		if np.IsZero() {
			attrs = append(attrs, gen.Type().String(), "ready")
		} else if np.After(now) {
			wait := np.Sub(now).Truncate(time.Second)
			attrs = append(attrs, gen.Type().String(), wait.String())
			if earliest.IsZero() || np.Before(earliest) {
				earliest = np
			}
		} else {
			attrs = append(attrs, gen.Type().String(), "ready")
		}
	}

	if !earliest.IsZero() {
		wait := earliest.Sub(now).Truncate(time.Second)
		attrs = append(attrs, "next_work_in", wait.String())
	}
	s.cfg.Logger.Info("scheduler idle, waiting for next cycle", attrs...)
}

// updateRateState checks the API's rate limit info and adjusts the credit
// allocator regime and rate limiter accordingly.
func (s *UnifiedScheduler) updateRateState() {
	remaining, resetAt := s.cfg.API.RateLimitInfo()
	now := s.cfg.Clock.Now()
	secondsLeft := resetAt.Sub(now).Seconds()

	// Assume a 1000 RPH limit for hobby tier by default.
	// The actual limit could be detected from headers, but Railway doesn't
	// expose the limit in headers -- only remaining and reset.
	estimatedLimit := 1000

	s.cfg.Credits.UpdateRegime(remaining, estimatedLimit, secondsLeft)

	// Adjust rate limiter: target 90% of available rate, capped at MaxRPS.
	if secondsLeft > 0 && remaining > 0 {
		availableRate := float64(remaining) / secondsLeft
		targetRate := availableRate * 0.9
		if targetRate > s.cfg.MaxRPS {
			targetRate = s.cfg.MaxRPS
		}
		if targetRate < 0.01 {
			targetRate = 0.01 // minimum rate to avoid complete stall
		}
		s.limiter.SetLimit(rate.Limit(targetRate))
	} else if remaining <= 0 {
		// Near-zero rate when exhausted (limiter doesn't support 0).
		s.limiter.SetLimit(rate.Limit(0.001))
	}
}
