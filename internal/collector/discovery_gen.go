package collector

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/xevion/railway-collector/internal/collector/types"
)

// DiscoveryGeneratorConfig configures a DiscoveryGenerator.
type DiscoveryGeneratorConfig struct {
	Discovery TargetProvider
	Interval  time.Duration // how often to refresh discovery
	Logger    *slog.Logger
}

// DiscoveryGenerator implements TaskGenerator as a thin wrapper around Discovery.
//
// Discovery uses a single nested GraphQL query per workspace that returns
// projects, environments, service instances, regions, and latest deployments.
// The scheduler handles types.QueryDiscovery items specially by calling
// Discovery.Refresh() instead of building a batched query.
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

// Type returns types.TaskTypeDiscovery.
func (g *DiscoveryGenerator) Type() types.TaskType {
	return types.TaskTypeDiscovery
}

// NextPoll returns the earliest time this generator will produce work.
func (g *DiscoveryGenerator) NextPoll() time.Time { return g.nextPoll }

// Poll returns a single types.WorkItem when discovery refresh is due.
func (g *DiscoveryGenerator) Poll(now time.Time) []types.WorkItem {
	if now.Before(g.nextPoll) {
		return nil
	}

	g.nextPoll = now.Add(g.interval)

	return []types.WorkItem{{
		ID:       "discovery",
		Kind:     types.QueryDiscovery,
		TaskType: types.TaskTypeDiscovery,
		AliasKey: "discovery",
		BatchKey: "discovery",
	}}
}

// Deliver handles the result of a discovery refresh.
// For types.QueryDiscovery items, the scheduler calls Discovery.Refresh() directly
// and passes err (if any) here. The data parameter is unused.
func (g *DiscoveryGenerator) Deliver(_ context.Context, _ types.WorkItem, _ json.RawMessage, err error) {
	if err != nil {
		g.logger.Error("discovery refresh failed", "error", err)
		return
	}
	g.logger.Debug("discovery refresh complete", "targets", len(g.discovery.Targets()))
}

// Refresh is a convenience method that the scheduler can call directly
// for types.QueryDiscovery items, since discovery doesn't use raw queries.
func (g *DiscoveryGenerator) Refresh(ctx context.Context) error {
	return g.discovery.Refresh(ctx)
}
