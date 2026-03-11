package collector

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"regexp"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/railway"
)

// compiledFilter holds an original filter string and its pre-compiled regex (if valid).
type compiledFilter struct {
	raw string
	re  *regexp.Regexp
}

// Workspace holds the ID and name of a Railway workspace to discover resources from.
type Workspace struct {
	ID   string
	Name string
}

// cachedEntry holds data with a TTL that includes random jitter.
type cachedEntry[T any] struct {
	data      T
	expiresAt time.Time
}

func (c *cachedEntry[T]) expiredAt(now time.Time) bool {
	return now.After(c.expiresAt)
}

// jitteredTTL returns baseTTL ± jitter (uniform random).
func jitteredTTL(base, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return base
	}
	// rand in [-jitter, +jitter]
	offset := time.Duration(rand.Int64N(int64(2*jitter))) - jitter
	ttl := base + offset
	if ttl < time.Minute {
		ttl = time.Minute // minimum 1 minute to avoid pathologically short TTLs
	}
	return ttl
}

// PersistedWorkspaceDiscovery is the JSON-serialized form of a workspace's full discovery result.
type PersistedWorkspaceDiscovery struct {
	Targets   []types.ServiceTarget `json:"targets"`
	ExpiresAt time.Time             `json:"expires_at"`
}

// Discovery handles enumerating Railway resources and building collection targets.
type Discovery struct {
	client  types.RailwayAPI
	store   types.StateStore
	clock   clockwork.Clock
	filters config.FiltersConfig
	logger  *slog.Logger

	compiledProjects     []compiledFilter
	compiledServices     []compiledFilter
	compiledEnvironments []compiledFilter

	// Cache TTL configuration
	workspaceTTL time.Duration // base TTL for workspace list
	jitter       time.Duration // ± jitter applied to each TTL

	mu      sync.RWMutex
	targets []types.ServiceTarget

	// Workspace cache: refreshed at workspaceTTL ± jitter
	wsMu    sync.Mutex
	wsCache *cachedEntry[[]Workspace]
	// Explicit workspace list (from config); if set, skip Me() discovery
	staticWorkspaces []Workspace

	// Per-workspace target cache: full discovery result per workspace
	wsTargetMu    sync.Mutex
	wsTargetCache map[string]*cachedEntry[[]types.ServiceTarget] // keyed by workspaceID
}

func compileFilters(patterns []string, logger *slog.Logger) []compiledFilter {
	out := make([]compiledFilter, len(patterns))
	for i, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			logger.Warn("invalid regex in filter, will match literally only", "pattern", p, "error", err)
		}
		out[i] = compiledFilter{raw: p, re: re}
	}
	return out
}

type DiscoveryConfig struct {
	Client       types.RailwayAPI
	Store        types.StateStore // persistent store for caching discovery data across restarts
	Clock        clockwork.Clock
	Filters      config.FiltersConfig
	Workspaces   []Workspace   // static workspace list (from config or Me response)
	WorkspaceTTL time.Duration // base TTL for workspace discovery (default 1h)
	Jitter       time.Duration // ± jitter for TTLs (default 15m)
	Logger       *slog.Logger
}

func NewDiscovery(cfg DiscoveryConfig) *Discovery {
	wsTTL := cfg.WorkspaceTTL
	if wsTTL == 0 {
		wsTTL = time.Hour
	}
	jitter := cfg.Jitter
	if jitter == 0 {
		jitter = 15 * time.Minute
	}

	clk := cfg.Clock
	if clk == nil {
		clk = clockwork.NewRealClock()
	}

	d := &Discovery{
		client:               cfg.Client,
		store:                cfg.Store,
		clock:                clk,
		filters:              cfg.Filters,
		logger:               cfg.Logger,
		compiledProjects:     compileFilters(cfg.Filters.Projects, cfg.Logger),
		compiledServices:     compileFilters(cfg.Filters.Services, cfg.Logger),
		compiledEnvironments: compileFilters(cfg.Filters.Environments, cfg.Logger),
		workspaceTTL:         wsTTL,
		jitter:               jitter,
		wsTargetCache:        make(map[string]*cachedEntry[[]types.ServiceTarget]),
	}

	if len(cfg.Workspaces) > 0 {
		d.staticWorkspaces = cfg.Workspaces
	}

	return d
}

// Targets returns the current set of discovered service targets.
func (d *Discovery) Targets() []types.ServiceTarget {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]types.ServiceTarget, len(d.targets))
	copy(out, d.targets)
	return out
}

// Refresh re-discovers Railway resources, using cached data when TTLs haven't expired.
func (d *Discovery) Refresh(ctx context.Context) error {
	start := time.Now()
	workspaces, err := d.resolveWorkspaces(ctx)
	if err != nil {
		return err
	}

	d.logger.Info("discovering Railway resources", "workspaces", len(workspaces))

	var allTargets []types.ServiceTarget
	for _, ws := range workspaces {
		d.logger.Debug("discovering workspace", "workspace", ws.Name, "id", ws.ID)
		targets, err := d.discoverWorkspace(ctx, ws)
		if err != nil {
			d.logger.Error("failed to discover workspace", "workspace", ws.Name, "error", err)
			continue
		}
		allTargets = append(allTargets, targets...)
	}

	d.mu.Lock()
	d.targets = allTargets
	d.mu.Unlock()

	projSet := make(map[string]bool)
	envSet := make(map[string]bool)
	for _, t := range allTargets {
		projSet[t.ProjectID] = true
		envSet[t.EnvironmentID] = true
	}
	d.logger.Info("discovery complete",
		"targets", len(allTargets),
		"projects", len(projSet),
		"environments", len(envSet),
		"workspaces", len(workspaces),
		"duration", time.Since(start),
	)
	return nil
}

// discoverFromResponse converts a DiscoverAll API response into ServiceTargets,
// applying project/service/environment filters. It also checks pageInfo for
// overflow and logs warnings.
func (d *Discovery) discoverFromResponse(resp *railway.DiscoverAllResponse, ws Workspace) []types.ServiceTarget {
	if resp.Projects.PageInfo.HasNextPage {
		d.logger.Warn("project list exceeded page limit for workspace; some projects may be missing",
			"workspace", ws.Name, "count", len(resp.Projects.Edges))
	}

	var targets []types.ServiceTarget

	for _, pe := range resp.Projects.Edges {
		p := pe.Node
		if !d.matchFilter(p.Name, p.Id, d.compiledProjects) {
			continue
		}

		if p.Environments.PageInfo.HasNextPage {
			d.logger.Warn("environment list exceeded page limit for project; some environments may be missing",
				"project", p.Name, "count", len(p.Environments.Edges))
		}

		for _, ee := range p.Environments.Edges {
			e := ee.Node
			if !d.matchFilter(e.Name, e.Id, d.compiledEnvironments) {
				continue
			}

			if e.ServiceInstances.PageInfo.HasNextPage {
				d.logger.Warn("service instance list exceeded page limit; some services may be missing",
					"project", p.Name, "environment", e.Name,
					"count", len(e.ServiceInstances.Edges))
			}

			for _, si := range e.ServiceInstances.Edges {
				inst := si.Node
				if !d.matchFilter(inst.ServiceName, inst.ServiceId, d.compiledServices) {
					continue
				}

				target := types.ServiceTarget{
					ProjectID:       p.Id,
					ProjectName:     p.Name,
					ServiceID:       inst.ServiceId,
					ServiceName:     inst.ServiceName,
					EnvironmentID:   inst.EnvironmentId,
					EnvironmentName: e.Name,
				}

				if inst.LatestDeployment != nil {
					target.DeploymentID = inst.LatestDeployment.Id
				}

				if inst.Region != nil {
					target.Region = *inst.Region
				}

				targets = append(targets, target)
			}
		}
	}

	return targets
}

// discoverWorkspace fetches all targets for a workspace using a single nested query.
// Uses in-memory and bbolt caches with jittered TTLs.
func (d *Discovery) discoverWorkspace(ctx context.Context, ws Workspace) ([]types.ServiceTarget, error) {
	// Check in-memory cache
	d.wsTargetMu.Lock()
	if cached, ok := d.wsTargetCache[ws.ID]; ok && !cached.expiredAt(d.clock.Now()) {
		d.wsTargetMu.Unlock()
		d.logger.Debug("using cached workspace discovery", "workspace", ws.Name,
			"ttl", time.Until(cached.expiresAt).Round(time.Second))
		return cached.data, nil
	}
	d.wsTargetMu.Unlock()

	// Check bbolt persistent cache
	if d.store != nil {
		raw, err := d.store.GetDiscoveryCache(ws.ID)
		if err != nil {
			d.logger.Warn("failed to read workspace discovery cache from store", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
		} else if raw != nil {
			var persisted PersistedWorkspaceDiscovery
			if err := json.Unmarshal(raw, &persisted); err != nil {
				d.logger.Warn("failed to unmarshal workspace discovery cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
			} else if d.clock.Now().Before(persisted.ExpiresAt) {
				d.logger.Debug("loaded workspace discovery from persistent cache", "workspace", ws.Name,
					"targets", len(persisted.Targets), "ttl", time.Until(persisted.ExpiresAt).Round(time.Second))
				d.wsTargetMu.Lock()
				d.wsTargetCache[ws.ID] = &cachedEntry[[]types.ServiceTarget]{
					data:      persisted.Targets,
					expiresAt: persisted.ExpiresAt,
				}
				d.wsTargetMu.Unlock()
				return persisted.Targets, nil
			}
		}
	}

	d.logger.Info("discovering workspace (cache miss)", "workspace", ws.Name)

	isEphemeral := false
	resp, err := d.client.DiscoverAll(ctx, &ws.ID, &isEphemeral)
	if err != nil {
		// Return stale cache on error if available
		d.wsTargetMu.Lock()
		if cached, ok := d.wsTargetCache[ws.ID]; ok {
			d.wsTargetMu.Unlock()
			d.logger.Warn("failed to refresh workspace discovery, using stale cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
			return cached.data, nil
		}
		d.wsTargetMu.Unlock()
		return nil, err
	}

	targets := d.discoverFromResponse(resp, ws)

	// Cache with jittered TTL. Re-check under lock in case a concurrent caller
	// already populated the cache while the API call was in flight.
	ttl := jitteredTTL(d.workspaceTTL, d.jitter)
	expiresAt := d.clock.Now().Add(ttl)
	d.wsTargetMu.Lock()
	if existing, ok := d.wsTargetCache[ws.ID]; ok && !existing.expiredAt(d.clock.Now()) {
		d.wsTargetMu.Unlock()
		return existing.data, nil
	}
	d.wsTargetCache[ws.ID] = &cachedEntry[[]types.ServiceTarget]{
		data:      targets,
		expiresAt: expiresAt,
	}
	d.wsTargetMu.Unlock()
	d.logger.Debug("cached workspace discovery", "workspace", ws.Name, "targets", len(targets), "ttl", ttl.Round(time.Second))

	// Persist to bbolt
	if d.store != nil {
		persisted := PersistedWorkspaceDiscovery{
			Targets:   targets,
			ExpiresAt: expiresAt,
		}
		data, err := json.Marshal(persisted)
		if err != nil {
			d.logger.Warn("failed to marshal workspace discovery cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
		} else if err := d.store.SetDiscoveryCache(ws.ID, data); err != nil {
			d.logger.Warn("failed to persist workspace discovery cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
		}
	}

	return targets, nil
}

// resolveWorkspaces returns the workspace list, using cache or static config.
func (d *Discovery) resolveWorkspaces(ctx context.Context) ([]Workspace, error) {
	// Static workspaces from config: always use those, no API call
	if len(d.staticWorkspaces) > 0 {
		return d.staticWorkspaces, nil
	}

	d.wsMu.Lock()
	defer d.wsMu.Unlock()

	if d.wsCache != nil && !d.wsCache.expiredAt(d.clock.Now()) {
		d.logger.Debug("using cached workspace list", "ttl", time.Until(d.wsCache.expiresAt).Round(time.Second))
		return d.wsCache.data, nil
	}

	me, err := d.client.Me(ctx)
	if err != nil {
		// If cache exists but expired, return stale data on error
		if d.wsCache != nil {
			d.logger.Warn("failed to refresh workspaces, using stale cache", "error", err)
			return d.wsCache.data, nil
		}
		return nil, err
	}

	var workspaces []Workspace
	for _, ws := range me.Me.Workspaces {
		workspaces = append(workspaces, Workspace{ID: ws.Id, Name: ws.Name})
	}

	ttl := jitteredTTL(d.workspaceTTL, d.jitter)
	d.wsCache = &cachedEntry[[]Workspace]{
		data:      workspaces,
		expiresAt: d.clock.Now().Add(ttl),
	}
	d.logger.Debug("cached workspace list", "count", len(workspaces), "ttl", ttl.Round(time.Second))

	return workspaces, nil
}

// matchFilter returns true if the name or ID matches the filter list.
// Empty filter list means match all. Entries can be exact IDs, names, or regex patterns.
func (d *Discovery) matchFilter(name, id string, filters []compiledFilter) bool {
	if len(filters) == 0 {
		return true
	}
	for _, f := range filters {
		if f.raw == name || f.raw == id {
			return true
		}
		if f.re != nil && f.re.MatchString(name) {
			return true
		}
	}
	return false
}

// DiscoveryGeneratorConfig configures a DiscoveryGenerator.
type DiscoveryGeneratorConfig struct {
	Discovery types.TargetProvider
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
	discovery types.TargetProvider
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
