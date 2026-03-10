package collector_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xevion/railway-collector/internal/collector"
)

func TestCreditPool_DripRate(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now)

	// Initially has 1.0 credit (startup)
	assert.InDelta(t, 1.0, ca.Available(collector.TaskTypeMetrics, now), 0.01)

	// After 30 seconds, metrics (2/min rate) should have accumulated ~1.0 more
	later := now.Add(30 * time.Second)
	avail := ca.Available(collector.TaskTypeMetrics, later)
	assert.InDelta(t, 2.0, avail, 0.1) // 1.0 initial + 1.0 from 30s at 2/min
}

func TestCreditPool_MaxCap(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now)

	// After 10 minutes, backfill at 11/min should be capped at MaxCredits (4.0)
	later := now.Add(10 * time.Minute)
	avail := ca.Available(collector.TaskTypeBackfill, later)
	assert.InDelta(t, cfg.MaxCredits, avail, 0.01)
}

func TestCreditAllocator_TryDeduct(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now)

	// Should have 1 initial credit
	assert.True(t, ca.TryDeduct(collector.TaskTypeMetrics, now))
	// Should not have another yet
	assert.False(t, ca.TryDeduct(collector.TaskTypeMetrics, now))

	// Wait 30 seconds (1 credit at 2/min)
	later := now.Add(30 * time.Second)
	assert.True(t, ca.TryDeduct(collector.TaskTypeMetrics, later))
}

func TestCreditAllocator_RegimeTransitions(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now)

	assert.Equal(t, collector.RegimeAbundant, ca.Regime())

	// Drop to normal (25% remaining)
	ca.UpdateRegime(250, 1000, 3600)
	assert.Equal(t, collector.RegimeNormal, ca.Regime())

	// Drop to scarce (5% remaining)
	ca.UpdateRegime(50, 1000, 3600)
	assert.Equal(t, collector.RegimeScarce, ca.Regime())

	// Exhausted
	ca.UpdateRegime(0, 1000, 3600)
	assert.Equal(t, collector.RegimeExhausted, ca.Regime())

	// Recovery to abundant
	ca.UpdateRegime(900, 1000, 3600)
	assert.Equal(t, collector.RegimeAbundant, ca.Regime())
}

func TestCreditAllocator_ScarceDisablesBackfill(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now)

	// Use initial backfill credit
	ca.TryDeduct(collector.TaskTypeBackfill, now)

	// Switch to scarce
	ca.UpdateRegime(50, 1000, 3600)

	// Wait -- backfill should not accumulate credits
	later := now.Add(5 * time.Minute)
	assert.InDelta(t, 0.0, ca.Available(collector.TaskTypeBackfill, later), 0.01)

	// Metrics should still accumulate
	assert.True(t, ca.Available(collector.TaskTypeMetrics, later) >= 1.0)
}

func TestCreditAllocator_SelectBestType(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now)

	// All types have 1 initial credit; metrics has highest priority
	candidates := []collector.TaskType{
		collector.TaskTypeBackfill,
		collector.TaskTypeMetrics,
		collector.TaskTypeLogs,
	}
	best, ok := ca.SelectBestType(candidates, now)
	assert.True(t, ok)
	assert.Equal(t, collector.TaskTypeMetrics, best)
}

func TestCreditAllocator_SelectBestType_FallsThrough(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now)

	// Drain metrics credit
	ca.TryDeduct(collector.TaskTypeMetrics, now)

	// Now should fall through to logs
	candidates := []collector.TaskType{
		collector.TaskTypeMetrics,
		collector.TaskTypeLogs,
	}
	best, ok := ca.SelectBestType(candidates, now)
	assert.True(t, ok)
	assert.Equal(t, collector.TaskTypeLogs, best)
}
