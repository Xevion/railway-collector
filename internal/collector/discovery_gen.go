package collector

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// DiscoveryGeneratorConfig configures a DiscoveryGenerator.
type DiscoveryGeneratorConfig struct {
	Discovery TargetProvider
	Interval  time.Duration // how often to refresh discovery
	Logger    *slog.Logger
}

// DiscoveryGenerator implements TaskGenerator as a thin wrapper around Discovery.
//
// Unlike metrics/logs generators, discovery involves cascading API calls
// (workspaces -> projects -> deployments -> service instances) that don't map
// to a single raw GraphQL query. The scheduler handles QueryDiscovery items
// specially by calling Discovery.Refresh() instead of building a batched query.
//
// Deliver() is a no-op because Refresh() updates Discovery's internal state directly.
type DiscoveryGenerator struct {
	discovery TargetProvider
	interval  time.Duration
	logger    *slog.Logger

	nextPoll time.Time
}

// NewDiscoveryGenerator creates a DiscoveryGenerator.
func NewDiscoveryGenerator(cfg DiscoveryGeneratorConfig) *DiscoveryGenerator {
	return &DiscoveryGenerator{
		discovery: cfg.Discovery,
		interval:  cfg.Interval,
		logger:    cfg.Logger,
	}
}

// Type returns TaskTypeDiscovery.
func (g *DiscoveryGenerator) Type() TaskType {
	return TaskTypeDiscovery
}

// NextPoll returns the earliest time this generator will produce work.
func (g *DiscoveryGenerator) NextPoll() time.Time { return g.nextPoll }

// Poll returns a single WorkItem when discovery refresh is due.
func (g *DiscoveryGenerator) Poll(now time.Time) []WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	g.nextPoll = now.Add(g.interval)

	return []WorkItem{{
		ID:       "discovery",
		Kind:     QueryDiscovery,
		TaskType: TaskTypeDiscovery,
		AliasKey: "discovery",
		BatchKey: "discovery",
	}}
}

// Deliver handles the result of a discovery refresh.
// For QueryDiscovery items, the scheduler calls Discovery.Refresh() directly
// and passes err (if any) here. The data parameter is unused.
func (g *DiscoveryGenerator) Deliver(_ context.Context, _ WorkItem, _ json.RawMessage, err error) {
	if err != nil {
		g.logger.Error("discovery refresh failed", "error", err)
		return
	}
	g.logger.Debug("discovery refresh complete", "targets", len(g.discovery.Targets()))
}

// Refresh is a convenience method that the scheduler can call directly
// for QueryDiscovery items, since discovery doesn't use raw queries.
func (g *DiscoveryGenerator) Refresh(ctx context.Context) error {
	return g.discovery.Refresh(ctx)
}
