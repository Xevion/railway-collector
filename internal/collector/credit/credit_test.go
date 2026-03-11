package credit_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xevion/railway-collector/internal/collector/credit"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/config"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// testCreditConfig is a stable fixture used across credit tests.
var testCreditConfig = config.CreditsConfig{
	MetricsRate:   8.0,
	LogsRate:      6.0,
	DiscoveryRate: 1.0,
	UsageRate:     1.0,
	MaxCredits:    4.0,
}

// newTestAllocator creates a CreditAllocator with testCreditConfig and a fixed baseline time.
func newTestAllocator() (*credit.CreditAllocator, time.Time) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	return credit.NewCreditAllocator(testCreditConfig, now, discardLogger), now
}

// TestCreditPool_Accumulation verifies credit drip rate and max cap behavior.
func TestCreditPool_Accumulation(t *testing.T) {
	tests := []struct {
		name      string
		elapsed   time.Duration
		taskType  types.TaskType
		wantAvail float64
		delta     float64
	}{
		{
			// After 5 seconds, metrics (8/min = 0.133/s) should have accumulated ~0.67 more.
			name:      "drip rate after 5s",
			elapsed:   5 * time.Second,
			taskType:  types.TaskTypeMetrics,
			wantAvail: 1.67, // 1.0 initial + 0.67 from 5s at 8/min
			delta:     0.1,
		},
		{
			// After 10 minutes, metrics at 8/min should be capped at MaxCredits (4.0).
			name:      "max cap after 10min",
			elapsed:   10 * time.Minute,
			taskType:  types.TaskTypeMetrics,
			wantAvail: testCreditConfig.MaxCredits,
			delta:     0.01,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ca, now := newTestAllocator()
			later := now.Add(tc.elapsed)
			avail := ca.Available(tc.taskType, later)
			assert.InDelta(t, tc.wantAvail, avail, tc.delta)
		})
	}
}

func TestCreditPool_InitialCredit(t *testing.T) {
	ca, now := newTestAllocator()
	// Initially has 1.0 credit (startup)
	assert.InDelta(t, 1.0, ca.Available(types.TaskTypeMetrics, now), 0.01)
}

func TestCreditAllocator_TryDeduct(t *testing.T) {
	ca, now := newTestAllocator()

	// Should have 1 initial credit
	assert.True(t, ca.TryDeduct(types.TaskTypeMetrics, now))
	// Should not have another yet
	assert.False(t, ca.TryDeduct(types.TaskTypeMetrics, now))

	// Wait 8 seconds (just over 1 credit at 8/min = 0.133/s)
	later := now.Add(8 * time.Second)
	assert.True(t, ca.TryDeduct(types.TaskTypeMetrics, later))
}

func TestCreditAllocator_RegimeTransitions(t *testing.T) {
	ca, _ := newTestAllocator()

	assert.Equal(t, credit.RegimeAbundant, ca.Regime())

	// Drop to normal (25% remaining)
	ca.UpdateRegime(250, 1000, 3600)
	assert.Equal(t, credit.RegimeNormal, ca.Regime())

	// Drop to scarce (5% remaining)
	ca.UpdateRegime(50, 1000, 3600)
	assert.Equal(t, credit.RegimeScarce, ca.Regime())

	// Exhausted
	ca.UpdateRegime(0, 1000, 3600)
	assert.Equal(t, credit.RegimeExhausted, ca.Regime())

	// Recovery to abundant
	ca.UpdateRegime(900, 1000, 3600)
	assert.Equal(t, credit.RegimeAbundant, ca.Regime())
}

// TestCreditAllocator_ReverseRegimeTransition verifies recovery from degraded regimes.
// When credits are replenished after being scarce/exhausted, the regime should promote
// back toward Abundant. This tests the UpdateRegime promotion path directly.
func TestCreditAllocator_ReverseRegimeTransition(t *testing.T) {
	tests := []struct {
		name       string
		setupRatio int // remaining/1000 to enter degraded state
		recoverTo  int // remaining/1000 to recover
		wantRegime credit.Regime
	}{
		{
			name:       "scarce recovers to normal",
			setupRatio: 50,  // 5% -> Scarce
			recoverTo:  200, // 20% -> Normal
			wantRegime: credit.RegimeNormal,
		},
		{
			name:       "scarce recovers to abundant",
			setupRatio: 50,  // 5% -> Scarce
			recoverTo:  700, // 70% -> Abundant
			wantRegime: credit.RegimeAbundant,
		},
		{
			name:       "exhausted recovers to normal",
			setupRatio: 0,   // 0% -> Exhausted
			recoverTo:  300, // 30% -> Normal
			wantRegime: credit.RegimeNormal,
		},
		{
			name:       "exhausted recovers to abundant",
			setupRatio: 0,   // 0% -> Exhausted
			recoverTo:  600, // 60% -> Abundant
			wantRegime: credit.RegimeAbundant,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ca, _ := newTestAllocator()

			// Degrade the regime
			ca.UpdateRegime(tc.setupRatio, 1000, 3600)

			// Recover
			ca.UpdateRegime(tc.recoverTo, 1000, 3600)
			assert.Equal(t, tc.wantRegime, ca.Regime())
		})
	}
}

func TestCreditAllocator_RegimeBoundaries(t *testing.T) {
	cases := []struct {
		name      string
		remaining int
		expected  credit.Regime
	}{
		// Exhausted: remaining <= 0
		{"remaining=0", 0, credit.RegimeExhausted},
		{"remaining=-1", -1, credit.RegimeExhausted},
		// Scarce: 0 < remaining/limit < 0.10
		{"remaining=1", 1, credit.RegimeScarce},
		{"remaining=99", 99, credit.RegimeScarce},
		// Normal: 0.10 <= remaining/limit < 0.50
		{"remaining=100 (10%)", 100, credit.RegimeNormal},
		{"remaining=250", 250, credit.RegimeNormal},
		{"remaining=499", 499, credit.RegimeNormal},
		// Abundant: remaining/limit >= 0.50
		{"remaining=500 (50%)", 500, credit.RegimeAbundant},
		{"remaining=750", 750, credit.RegimeAbundant},
		{"remaining=1000", 1000, credit.RegimeAbundant},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ca, _ := newTestAllocator()

			ca.UpdateRegime(tc.remaining, 1000, 3600)
			assert.Equal(t, tc.expected, ca.Regime())
		})
	}
}

func TestCreditAllocator_UpdateRegime_ZeroLimit(t *testing.T) {
	ca, _ := newTestAllocator()

	// Default regime is Abundant; zero limit should be a no-op
	assert.Equal(t, credit.RegimeAbundant, ca.Regime())
	ca.UpdateRegime(0, 0, 3600)
	assert.Equal(t, credit.RegimeAbundant, ca.Regime())
}

func TestCreditAllocator_TryDeduct_UnknownTaskType(t *testing.T) {
	ca, now := newTestAllocator()

	// Unknown task type has no pool — should return false
	assert.False(t, ca.TryDeduct(types.TaskType(99), now))
}

func TestCreditAllocator_Available_UnknownTaskType(t *testing.T) {
	ca, now := newTestAllocator()

	// Unknown task type has no pool — should return 0.0
	assert.Equal(t, 0.0, ca.Available(types.TaskType(99), now))
}

func TestCreditAllocator_ScarceReducesDiscovery(t *testing.T) {
	ca, now := newTestAllocator()

	// Use initial discovery credit
	ca.TryDeduct(types.TaskTypeDiscovery, now)

	// Switch to scarce
	ca.UpdateRegime(50, 1000, 3600)

	// Wait -- discovery should not accumulate credits (rate set to 0 in scarce)
	later := now.Add(5 * time.Minute)
	assert.InDelta(t, 0.0, ca.Available(types.TaskTypeDiscovery, later), 0.01)

	// Metrics should still accumulate (at reduced rate)
	assert.True(t, ca.Available(types.TaskTypeMetrics, later) >= 1.0)
}
