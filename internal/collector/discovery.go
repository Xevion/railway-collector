package collector

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"regexp"
	"sync"
	"time"

	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/railway"
)

// ServiceTarget represents a discovered service to collect metrics/logs from.
type ServiceTarget struct {
	ProjectID       string
	ProjectName     string
	ServiceID       string
	ServiceName     string
	EnvironmentID   string
	EnvironmentName string
	// Latest active deployment ID (if any)
	DeploymentID string
	Region       string
}

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

func (c *cachedEntry[T]) expired() bool {
	return time.Now().After(c.expiresAt)
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
		ttl = time.Minute // floor
	}
	return ttl
}

// projectCache holds the discovered services/envs/deployments for a single project.
type projectCache struct {
	targets   []ServiceTarget
	expiresAt time.Time
}

// Discovery handles enumerating Railway resources and building collection targets.
type Discovery struct {
	client  *railway.Client
	filters config.FiltersConfig
	logger  *slog.Logger

	compiledProjects     []compiledFilter
	compiledServices     []compiledFilter
	compiledEnvironments []compiledFilter

	// Cache TTL configuration
	workspaceTTL time.Duration // base TTL for workspace list
	projectTTL   time.Duration // base TTL for per-project discovery
	jitter       time.Duration // ± jitter applied to each TTL

	mu      sync.RWMutex
	targets []ServiceTarget

	// Workspace cache: refreshed at workspaceTTL ± jitter
	wsMu    sync.Mutex
	wsCache *cachedEntry[[]Workspace]
	// Explicit workspace list (from config); if set, skip Me() discovery
	staticWorkspaces []Workspace

	// Per-project cache: each project refreshed independently at projectTTL ± jitter
	projMu    sync.Mutex
	projCache map[string]*projectCache // keyed by projectID
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
	Client       *railway.Client
	Filters      config.FiltersConfig
	Workspaces   []Workspace   // static workspace list (from config or Me response)
	WorkspaceTTL time.Duration // base TTL for workspace discovery (default 1h)
	ProjectTTL   time.Duration // base TTL for per-project discovery (default 1h)
	Jitter       time.Duration // ± jitter for TTLs (default 15m)
	Logger       *slog.Logger
}

func NewDiscovery(cfg DiscoveryConfig) *Discovery {
	wsTTL := cfg.WorkspaceTTL
	if wsTTL == 0 {
		wsTTL = time.Hour
	}
	pTTL := cfg.ProjectTTL
	if pTTL == 0 {
		pTTL = time.Hour
	}
	jitter := cfg.Jitter
	if jitter == 0 {
		jitter = 15 * time.Minute
	}

	d := &Discovery{
		client:               cfg.Client,
		filters:              cfg.Filters,
		logger:               cfg.Logger,
		compiledProjects:     compileFilters(cfg.Filters.Projects, cfg.Logger),
		compiledServices:     compileFilters(cfg.Filters.Services, cfg.Logger),
		compiledEnvironments: compileFilters(cfg.Filters.Environments, cfg.Logger),
		workspaceTTL:         wsTTL,
		projectTTL:           pTTL,
		jitter:               jitter,
		projCache:            make(map[string]*projectCache),
	}

	if len(cfg.Workspaces) > 0 {
		d.staticWorkspaces = cfg.Workspaces
	}

	return d
}

// Targets returns the current set of discovered service targets.
func (d *Discovery) Targets() []ServiceTarget {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]ServiceTarget, len(d.targets))
	copy(out, d.targets)
	return out
}

// Refresh re-discovers Railway resources, using cached data when TTLs haven't expired.
// Workspace list and per-project details are cached independently with jittered TTLs.
func (d *Discovery) Refresh(ctx context.Context) error {
	workspaces, err := d.resolveWorkspaces(ctx)
	if err != nil {
		return err
	}

	d.logger.Info("discovering Railway resources", "workspaces", len(workspaces))

	var allTargets []ServiceTarget

	for _, ws := range workspaces {
		wsID := ws.ID
		d.logger.Debug("discovering projects in workspace", "workspace", ws.Name, "id", ws.ID)

		resp, err := d.client.GetProjects(ctx, &wsID)
		if err != nil {
			d.logger.Error("failed to get projects for workspace", "workspace", ws.Name, "error", err)
			continue
		}

		if n := len(resp.Projects.Edges); n >= 100 {
			d.logger.Warn("project list hit page limit for workspace; some projects may be missing", "workspace", ws.Name, "count", n)
		}

		for _, pe := range resp.Projects.Edges {
			p := pe.Node
			if !d.matchFilter(p.Name, p.Id, d.compiledProjects) {
				continue
			}

			targets := d.discoverProject(ctx, p, ws)
			allTargets = append(allTargets, targets...)
		}
	}

	d.mu.Lock()
	d.targets = allTargets
	d.mu.Unlock()

	d.logger.Info("discovery complete", "targets", len(allTargets))
	return nil
}

// resolveWorkspaces returns the workspace list, using cache or static config.
func (d *Discovery) resolveWorkspaces(ctx context.Context) ([]Workspace, error) {
	// Static workspaces from config: always use those, no API call
	if len(d.staticWorkspaces) > 0 {
		return d.staticWorkspaces, nil
	}

	d.wsMu.Lock()
	defer d.wsMu.Unlock()

	if d.wsCache != nil && !d.wsCache.expired() {
		d.logger.Debug("using cached workspace list", "expires_in", time.Until(d.wsCache.expiresAt).Round(time.Second))
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
		expiresAt: time.Now().Add(ttl),
	}
	d.logger.Debug("cached workspace list", "count", len(workspaces), "ttl", ttl.Round(time.Second))

	return workspaces, nil
}

// discoverProject returns targets for a single project, using per-project cache.
func (d *Discovery) discoverProject(ctx context.Context, p railway.ProjectsProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProject, ws Workspace) []ServiceTarget {
	d.projMu.Lock()
	cached, ok := d.projCache[p.Id]
	d.projMu.Unlock()

	if ok && time.Now().Before(cached.expiresAt) {
		d.logger.Debug("using cached project discovery", "project", p.Name,
			"targets", len(cached.targets),
			"expires_in", time.Until(cached.expiresAt).Round(time.Second))
		return cached.targets
	}

	d.logger.Debug("discovering project", "project", p.Name, "id", p.Id)

	var targets []ServiceTarget

	if n := len(p.Environments.Edges); n >= 100 {
		d.logger.Warn("environment list hit page limit for project; some environments may be missing", "project", p.Name, "count", n)
	}
	for _, ee := range p.Environments.Edges {
		e := ee.Node
		if !d.matchFilter(e.Name, e.Id, d.compiledEnvironments) {
			continue
		}

		if n := len(p.Services.Edges); n >= 100 {
			d.logger.Warn("service list hit page limit for project; some services may be missing", "project", p.Name, "count", n)
		}
		for _, se := range p.Services.Edges {
			s := se.Node
			if !d.matchFilter(s.Name, s.Id, d.compiledServices) {
				continue
			}

			target := ServiceTarget{
				ProjectID:       p.Id,
				ProjectName:     p.Name,
				ServiceID:       s.Id,
				ServiceName:     s.Name,
				EnvironmentID:   e.Id,
				EnvironmentName: e.Name,
			}

			// Try to get the latest deployment for log collection
			first := 1
			deplResp, err := d.client.GetDeployments(ctx, p.Id, e.Id, s.Id, &first, nil)
			if err != nil {
				d.logger.Warn("failed to get deployments", "service", s.Name, "error", err)
			} else if len(deplResp.Deployments.Edges) > 0 {
				target.DeploymentID = deplResp.Deployments.Edges[0].Node.Id
			}

			// Get region from service instance
			siResp, err := d.client.GetServiceInstance(ctx, e.Id, s.Id)
			if err != nil {
				d.logger.Debug("failed to get service instance", "service", s.Name, "error", err)
			} else if siResp.ServiceInstance.Region != nil {
				target.Region = *siResp.ServiceInstance.Region
			}

			targets = append(targets, target)
		}
	}

	// Cache with jittered TTL
	ttl := jitteredTTL(d.projectTTL, d.jitter)
	d.projMu.Lock()
	d.projCache[p.Id] = &projectCache{
		targets:   targets,
		expiresAt: time.Now().Add(ttl),
	}
	d.projMu.Unlock()

	d.logger.Debug("cached project discovery", "project", p.Name, "targets", len(targets), "ttl", ttl.Round(time.Second))

	return targets
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
