package collector

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Regime describes how aggressively the scheduler should use API budget.
type Regime int

const (
	// RegimeAbundant: >50% of hourly limit remaining. Full rate, backfill active.
	RegimeAbundant Regime = iota
	// RegimeNormal: 10-50% remaining. Full rate, backfill scales down.
	RegimeNormal
	// RegimeScarce: <10% remaining. Only realtime tasks get credits.
	RegimeScarce
	// RegimeExhausted: 0 remaining. All collection paused.
	RegimeExhausted
)

func (r Regime) String() string {
	switch r {
	case RegimeAbundant:
		return "abundant"
	case RegimeNormal:
		return "normal"
	case RegimeScarce:
		return "scarce"
	case RegimeExhausted:
		return "exhausted"
	default:
		return "unknown"
	}
}

// CreditConfig defines the base credit allocation per task type at 16 req/min.
type CreditConfig struct {
	// Credits per minute for each task type at full rate
	MetricsRate   float64
	LogsRate      float64
	DiscoveryRate float64
	BackfillRate  float64
	// Maximum accumulated credits per type (prevents burst after idle)
	MaxCredits float64
}

// DefaultCreditConfig returns the default credit allocation.
func DefaultCreditConfig() CreditConfig {
	return CreditConfig{
		MetricsRate:   2.0,  // 1 batched request every 30s
		LogsRate:      2.0,  // 1 env logs + 1 deployment logs every 30s
		DiscoveryRate: 1.0,  // 1/min, mostly unused (TTL cached)
		BackfillRate:  11.0, // absorbs remaining budget
		MaxCredits:    4.0,
	}
}

// creditPool tracks available credits for a single task type.
type creditPool struct {
	tokens    float64
	rate      float64 // credits per second
	maxTokens float64
	lastCheck time.Time
}

func newCreditPool(creditsPerMinute, maxTokens float64, now time.Time) *creditPool {
	return &creditPool{
		tokens:    1.0, // start with 1 credit so we can fire immediately
		rate:      creditsPerMinute / 60.0,
		maxTokens: maxTokens,
		lastCheck: now,
	}
}

// available replenishes credits based on elapsed time and returns current balance.
func (p *creditPool) available(now time.Time) float64 {
	elapsed := now.Sub(p.lastCheck).Seconds()
	if elapsed > 0 {
		p.tokens += elapsed * p.rate
		if p.tokens > p.maxTokens {
			p.tokens = p.maxTokens
		}
		p.lastCheck = now
	}
	return p.tokens
}

// tryDeduct attempts to deduct one credit. Returns true if successful.
func (p *creditPool) tryDeduct(now time.Time) bool {
	p.available(now) // replenish first
	if p.tokens >= 1.0 {
		p.tokens -= 1.0
		return true
	}
	return false
}

// setRate adjusts the credit drip rate (credits per minute).
func (p *creditPool) setRate(creditsPerMinute float64) {
	p.rate = creditsPerMinute / 60.0
}

// CreditAllocator manages credit pools for all task types and adapts to
// rate limit conditions.
type CreditAllocator struct {
	mu     sync.Mutex
	pools  map[TaskType]*creditPool
	config CreditConfig
	regime Regime
	logger *slog.Logger
}

// NewCreditAllocator creates a new allocator with default credit pools.
func NewCreditAllocator(cfg CreditConfig, now time.Time, logger *slog.Logger) *CreditAllocator {
	return &CreditAllocator{
		pools: map[TaskType]*creditPool{
			TaskTypeMetrics:   newCreditPool(cfg.MetricsRate, cfg.MaxCredits, now),
			TaskTypeLogs:      newCreditPool(cfg.LogsRate, cfg.MaxCredits, now),
			TaskTypeDiscovery: newCreditPool(cfg.DiscoveryRate, cfg.MaxCredits, now),
			TaskTypeBackfill:  newCreditPool(cfg.BackfillRate, cfg.MaxCredits, now),
		},
		config: cfg,
		regime: RegimeAbundant,
		logger: logger,
	}
}

// TryDeduct attempts to deduct one credit from the given task type.
func (ca *CreditAllocator) TryDeduct(taskType TaskType, now time.Time) bool {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	pool, ok := ca.pools[taskType]
	if !ok {
		return false
	}
	return pool.tryDeduct(now)
}

// Available returns the current credit balance for a task type.
func (ca *CreditAllocator) Available(taskType TaskType, now time.Time) float64 {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	pool, ok := ca.pools[taskType]
	if !ok {
		return 0
	}
	return pool.available(now)
}

// Regime returns the current rate limit regime.
func (ca *CreditAllocator) Regime() Regime {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	return ca.regime
}

// UpdateRegime adjusts credit rates based on API rate limit state.
// remaining is the number of API calls left in the current window.
// limit is the total hourly limit. secondsUntilReset is time until the window resets.
func (ca *CreditAllocator) UpdateRegime(remaining, limit int, secondsUntilReset float64) {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	if limit <= 0 {
		return
	}

	ratio := float64(remaining) / float64(limit)
	var newRegime Regime

	switch {
	case remaining <= 0:
		newRegime = RegimeExhausted
	case ratio < 0.10:
		newRegime = RegimeScarce
	case ratio < 0.50:
		newRegime = RegimeNormal
	default:
		newRegime = RegimeAbundant
	}

	if newRegime == ca.regime {
		return
	}
	oldRegime := ca.regime
	ca.regime = newRegime

	ca.logger.Info("credit regime changed",
		"from", oldRegime.String(),
		"to", newRegime.String(),
		"remaining", remaining,
		"limit", limit,
		slog.String("budget_pct", fmt.Sprintf("%.1f%%", ratio*100)),
	)

	switch newRegime {
	case RegimeExhausted:
		// Zero out all rates
		for _, pool := range ca.pools {
			pool.setRate(0)
		}
	case RegimeScarce:
		// Only realtime gets credits
		ca.pools[TaskTypeMetrics].setRate(ca.config.MetricsRate)
		ca.pools[TaskTypeLogs].setRate(ca.config.LogsRate)
		ca.pools[TaskTypeDiscovery].setRate(0)
		ca.pools[TaskTypeBackfill].setRate(0)
	case RegimeNormal:
		// Full realtime, scaled backfill
		ca.pools[TaskTypeMetrics].setRate(ca.config.MetricsRate)
		ca.pools[TaskTypeLogs].setRate(ca.config.LogsRate)
		ca.pools[TaskTypeDiscovery].setRate(ca.config.DiscoveryRate)
		// Scale backfill proportionally to remaining budget
		backfillScale := ratio / 0.5 // 0.0 at 10%, 1.0 at 50%
		ca.pools[TaskTypeBackfill].setRate(ca.config.BackfillRate * backfillScale)
	case RegimeAbundant:
		// Full rate for everything
		ca.pools[TaskTypeMetrics].setRate(ca.config.MetricsRate)
		ca.pools[TaskTypeLogs].setRate(ca.config.LogsRate)
		ca.pools[TaskTypeDiscovery].setRate(ca.config.DiscoveryRate)
		ca.pools[TaskTypeBackfill].setRate(ca.config.BackfillRate)
	}
}
