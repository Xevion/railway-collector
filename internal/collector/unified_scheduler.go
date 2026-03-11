package collector

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/time/rate"

	"github.com/xevion/railway-collector/internal/collector/credit"
	"github.com/xevion/railway-collector/internal/collector/types"
)

// UnifiedSchedulerConfig configures the unified credit-based scheduler.
type UnifiedSchedulerConfig struct {
	Clock      clockwork.Clock
	API        RailwayAPI
	Credits    *credit.CreditAllocator
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
	generatorsByType map[types.TaskType]TaskGenerator

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

	genByType := make(map[types.TaskType]TaskGenerator, len(cfg.Generators))
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
	var allItems []types.WorkItem
	// Track which generator produced each item for result delivery.
	generatorMap := make(map[string]TaskGenerator)

	for _, gen := range s.cfg.Generators {
		items := gen.Poll(now)
		if len(items) > 0 {
			s.cfg.Logger.Debug("generator polled",
				"type", gen.Type().String(),
				"items", len(items),
			)
		}
		for _, item := range items {
			allItems = append(allItems, item)
			generatorMap[item.ID] = gen
		}
	}

	if len(allItems) == 0 {
		s.logIdleStatus(now)
		return
	}

	// 2. Handle discovery items separately (they use genqlient, not raw queries).
	var queryItems []types.WorkItem
	var discoveryItems []types.WorkItem
	for _, item := range allItems {
		if item.Kind == types.QueryDiscovery {
			discoveryItems = append(discoveryItems, item)
		} else {
			queryItems = append(queryItems, item)
		}
	}

	// If we only have discovery items, handle them directly.
	if len(queryItems) == 0 && len(discoveryItems) > 0 {
		if !s.cfg.Credits.TryDeduct(types.TaskTypeDiscovery, now) {
			s.logIdleStatus(now)
			return
		}
		if err := s.limiter.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.cfg.Logger.Warn("rate limiter wait failed", "error", err)
			return
		}
		s.handleDiscoveryItems(ctx, discoveryItems, generatorMap)
		return
	}

	// 3. Convert work items to self-contained alias fragments.
	var fragments []AliasFragment
	for _, item := range queryItems {
		fragments = append(fragments, FragmentFromWorkItem(item, now))
	}
	SortByPriority(fragments)

	// 4. Try credit deduction. Walk fragments by priority, deduct credits for
	//    each task type encountered, and collect credited fragments.
	var credited []AliasFragment
	deducted := make(map[types.TaskType]bool)
	var skippedTypes []string
	for _, f := range fragments {
		tt := f.Item.TaskType
		if !deducted[tt] {
			if !s.cfg.Credits.TryDeduct(tt, now) {
				skippedTypes = append(skippedTypes, tt.String())
				continue
			}
			deducted[tt] = true
		}
		credited = append(credited, f)
	}

	if len(skippedTypes) > 0 {
		s.cfg.Logger.Debug("task types skipped (insufficient credits)", "types", skippedTypes)
	}

	// Also try discovery if we have discovery items queued.
	if len(discoveryItems) > 0 && s.cfg.Credits.TryDeduct(types.TaskTypeDiscovery, now) {
		deducted[types.TaskTypeDiscovery] = true
	}

	if len(credited) == 0 && !deducted[types.TaskTypeDiscovery] {
		s.logIdleStatus(now)
		return
	}

	// 5. Pack credited fragments into requests (breadth budget).
	requests := Pack(credited)

	if len(requests) > 0 {
		s.cfg.Logger.Debug("packed fragments into requests",
			"fragments", len(credited),
			"requests", len(requests),
			"kind_breakdown", kindBreakdown(credited),
		)
	}

	if len(requests) == 0 && !deducted[types.TaskTypeDiscovery] {
		s.logIdleStatus(now)
		return
	}

	// 6. Execute all packed requests, rate-limited.
	for i, req := range requests {
		if err := s.limiter.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.cfg.Logger.Warn("rate limiter wait failed", "error", err)
			return
		}

		s.cfg.Logger.Debug("scheduler tick: executing request",
			"request", i+1,
			"total_requests", len(requests),
			"aliases", len(req.Fragments),
			"breadth", req.Breadth,
			"estimated_points", req.EstimatedPoints,
			"kind_breakdown", kindBreakdown(req.Fragments),
		)

		query, vars := req.AssembleQuery()
		resp, queryErr := s.cfg.API.RawQuery(ctx, "Batch", query, vars)
		s.updateRateState()

		// On computation limit errors with multi-alias requests, retry individually.
		// The error may come as a transport error (queryErr) or as a GraphQL-level
		// error in a 200 response with empty data.
		computationErr := queryErr
		if computationErr == nil && resp != nil && len(resp.Data) == 0 && len(resp.Errors) > 0 {
			for _, gqlErr := range resp.Errors {
				if strings.Contains(gqlErr.Message, "Problem processing request") {
					computationErr = fmt.Errorf("GraphQL request failed: %s", gqlErr.Message)
					break
				}
			}
		}
		if computationErr != nil && len(req.Fragments) > 1 && strings.Contains(computationErr.Error(), "Problem processing request") {
			s.cfg.Logger.Warn("batch request hit computation limit, retrying aliases individually",
				"aliases", len(req.Fragments),
				"breadth", req.Breadth,
				"estimated_points", req.EstimatedPoints,
				"kind_breakdown", kindBreakdown(req.Fragments),
				"error", computationErr,
			)
			s.retryFragmentsIndividually(ctx, req.Fragments, generatorMap)
			continue
		}

		DispatchRequestResults(ctx, req, resp, queryErr, generatorMap)
	}

	// 7. Handle discovery if credited (piggyback on same tick after the queries).
	if deducted[types.TaskTypeDiscovery] && len(discoveryItems) > 0 {
		s.handleDiscoveryItems(ctx, discoveryItems, generatorMap)
	}
}

// refresher is implemented by generators that perform a refresh instead of
// a raw GraphQL query (e.g., DiscoveryGenerator).
type refresher interface {
	Refresh(ctx context.Context) error
}

// handleDiscoveryItems executes discovery refresh via the generator's
// Refresh method rather than building a raw query.
func (s *UnifiedScheduler) handleDiscoveryItems(ctx context.Context, items []types.WorkItem, generatorMap map[string]TaskGenerator) {
	gen, ok := s.generatorsByType[types.TaskTypeDiscovery]
	if !ok {
		s.cfg.Logger.Error("discovery generator not found")
		return
	}

	r, ok := gen.(refresher)
	if !ok {
		s.cfg.Logger.Error("discovery generator does not support Refresh")
		return
	}

	err := r.Refresh(ctx)
	for _, item := range items {
		if g, exists := generatorMap[item.ID]; exists {
			g.Deliver(ctx, item, nil, err)
		}
	}
}

// retryFragmentsIndividually retries each fragment as a solo request after a batch
// computation limit error. This isolates which alias caused the failure.
func (s *UnifiedScheduler) retryFragmentsIndividually(ctx context.Context, fragments []AliasFragment, generatorMap map[string]TaskGenerator) {
	for _, frag := range fragments {
		if ctx.Err() != nil {
			return
		}
		if err := s.limiter.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.cfg.Logger.Warn("rate limiter wait failed during retry", "error", err)
			return
		}

		soloReq := Request{
			Fragments: []AliasFragment{frag},
			Breadth:   frag.Breadth,
		}

		s.cfg.Logger.Debug("retrying alias individually",
			"alias", frag.Alias,
			"kind", frag.Item.Kind,
			"breadth", frag.Breadth,
			"estimated_points", frag.EstimatedPoints,
			"alias_key", frag.Item.AliasKey,
		)

		query, vars := soloReq.AssembleQuery()
		resp, queryErr := s.cfg.API.RawQuery(ctx, "Batch", query, vars)
		s.updateRateState()

		if queryErr != nil {
			s.cfg.Logger.Warn("individual alias retry failed",
				"alias", frag.Alias,
				"kind", frag.Item.Kind,
				"breadth", frag.Breadth,
				"error", queryErr,
			)
		}

		DispatchRequestResults(ctx, soloReq, resp, queryErr, generatorMap)
	}
}

// kindBreakdown returns a summary string of fragment counts by QueryKind,
// e.g. "metrics:10 replicaMetrics:5 httpDurationMetrics:5".
func kindBreakdown(fragments []AliasFragment) string {
	counts := make(map[types.QueryKind]int)
	for _, f := range fragments {
		counts[f.Item.Kind]++
	}
	var parts []string
	for kind, count := range counts {
		parts = append(parts, fmt.Sprintf("%s:%d", kind, count))
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
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
		oldLimit := float64(s.limiter.Limit())
		s.limiter.SetLimit(rate.Limit(targetRate))
		if int(targetRate*1000) != int(oldLimit*1000) {
			s.cfg.Logger.Debug("rate limiter adjusted",
				"new_rps", targetRate,
				"remaining", remaining,
				"reset_in", time.Duration(secondsLeft*float64(time.Second)).Truncate(time.Second),
			)
		}
	} else if remaining <= 0 {
		// Near-zero rate when exhausted (limiter doesn't support 0).
		s.limiter.SetLimit(rate.Limit(0.001))
		s.cfg.Logger.Debug("rate limiter set to minimum (exhausted)", "remaining", remaining)
	}
}
