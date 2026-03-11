package collector_test

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xevion/railway-collector/internal/collector"
)

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestCreditPool_DripRate(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now, discardLogger)

	// Initially has 1.0 credit (startup)
	assert.InDelta(t, 1.0, ca.Available(collector.TaskTypeMetrics, now), 0.01)

	// After 5 seconds, metrics (8/min = 0.133/s) should have accumulated ~0.67 more
	later := now.Add(5 * time.Second)
	avail := ca.Available(collector.TaskTypeMetrics, later)
	assert.InDelta(t, 1.67, avail, 0.1) // 1.0 initial + 0.67 from 5s at 8/min
}

func TestCreditPool_MaxCap(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now, discardLogger)

	// After 10 minutes, metrics at 8/min should be capped at MaxCredits (4.0)
	later := now.Add(10 * time.Minute)
	avail := ca.Available(collector.TaskTypeMetrics, later)
	assert.InDelta(t, cfg.MaxCredits, avail, 0.01)
}

func TestCreditAllocator_TryDeduct(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now, discardLogger)

	// Should have 1 initial credit
	assert.True(t, ca.TryDeduct(collector.TaskTypeMetrics, now))
	// Should not have another yet
	assert.False(t, ca.TryDeduct(collector.TaskTypeMetrics, now))

	// Wait 8 seconds (just over 1 credit at 8/min = 0.133/s)
	later := now.Add(8 * time.Second)
	assert.True(t, ca.TryDeduct(collector.TaskTypeMetrics, later))
}

func TestCreditAllocator_RegimeTransitions(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now, discardLogger)

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

func TestCreditAllocator_ScarceReducesDiscovery(t *testing.T) {
	cfg := collector.DefaultCreditConfig()
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ca := collector.NewCreditAllocator(cfg, now, discardLogger)

	// Use initial discovery credit
	ca.TryDeduct(collector.TaskTypeDiscovery, now)

	// Switch to scarce
	ca.UpdateRegime(50, 1000, 3600)

	// Wait -- discovery should not accumulate credits (rate set to 0 in scarce)
	later := now.Add(5 * time.Minute)
	assert.InDelta(t, 0.0, ca.Available(collector.TaskTypeDiscovery, later), 0.01)

	// Metrics should still accumulate (at reduced rate)
	assert.True(t, ca.Available(collector.TaskTypeMetrics, later) >= 1.0)
}
