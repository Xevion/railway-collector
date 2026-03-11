package credit

import (
	"log/slog"
	"sync"
	"time"

	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/logging"
)

// Regime describes how aggressively the scheduler should use API budget.
type Regime int

const (
	// RegimeAbundant: >50% of hourly limit remaining. Full rate, all gap-filling active.
	RegimeAbundant Regime = iota
	// RegimeNormal: 10-50% remaining. Full rate, older gap-filling scales down.
	RegimeNormal
	// RegimeScarce: <10% remaining. Only live-edge tasks get credits.
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
	pools  map[types.TaskType]*creditPool
	config config.CreditsConfig
	regime Regime
	logger *slog.Logger
}

// NewCreditAllocator creates a new allocator with default credit pools.
func NewCreditAllocator(cfg config.CreditsConfig, now time.Time, logger *slog.Logger) *CreditAllocator {
	return &CreditAllocator{
		pools: map[types.TaskType]*creditPool{
			types.TaskTypeMetrics:   newCreditPool(cfg.MetricsRate, cfg.MaxCredits, now),
			types.TaskTypeLogs:      newCreditPool(cfg.LogsRate, cfg.MaxCredits, now),
			types.TaskTypeDiscovery: newCreditPool(cfg.DiscoveryRate, cfg.MaxCredits, now),
			types.TaskTypeUsage:     newCreditPool(cfg.UsageRate, cfg.MaxCredits, now),
		},
		config: cfg,
		regime: RegimeAbundant,
		logger: logger,
	}
}

// TryDeduct attempts to deduct one credit from the given task type.
func (ca *CreditAllocator) TryDeduct(taskType types.TaskType, now time.Time) bool {
	ca.mu.Lock()
	defer ca.mu.Unlock()

	pool, ok := ca.pools[taskType]
	if !ok {
		return false
	}
	return pool.tryDeduct(now)
}

// Available returns the current credit balance for a task type.
func (ca *CreditAllocator) Available(taskType types.TaskType, now time.Time) float64 {
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
		slog.Any("budget_pct", logging.Pct(ratio*100)),
	)

	switch newRegime {
	case RegimeExhausted:
		// Zero out all rates
		for _, pool := range ca.pools {
			pool.setRate(0)
		}
	case RegimeScarce:
		// Reduced rates under pressure
		ca.pools[types.TaskTypeMetrics].setRate(ca.config.MetricsRate * 0.5)
		ca.pools[types.TaskTypeLogs].setRate(ca.config.LogsRate * 0.5)
		ca.pools[types.TaskTypeDiscovery].setRate(0)
		ca.pools[types.TaskTypeUsage].setRate(0) // usage can wait
	case RegimeNormal:
		// Full rate, discovery scaled
		ca.pools[types.TaskTypeMetrics].setRate(ca.config.MetricsRate)
		ca.pools[types.TaskTypeLogs].setRate(ca.config.LogsRate)
		ca.pools[types.TaskTypeDiscovery].setRate(ca.config.DiscoveryRate)
		ca.pools[types.TaskTypeUsage].setRate(ca.config.UsageRate)
	case RegimeAbundant:
		// Full rate for everything
		ca.pools[types.TaskTypeMetrics].setRate(ca.config.MetricsRate)
		ca.pools[types.TaskTypeLogs].setRate(ca.config.LogsRate)
		ca.pools[types.TaskTypeDiscovery].setRate(ca.config.DiscoveryRate)
		ca.pools[types.TaskTypeUsage].setRate(ca.config.UsageRate)
	}
}
