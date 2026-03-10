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
	"github.com/xevion/railway-collector/internal/config"
	"golang.org/x/sync/errgroup"
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
		ttl = time.Minute // minimum 1 minute to avoid pathologically short TTLs
	}
	return ttl
}

// projectCache holds the discovered services/envs/deployments for a single project.
type projectCache struct {
	targets   []ServiceTarget
	expiresAt time.Time
}

// persistedProjectCache is the JSON-serialized form of a per-project discovery cache entry.
type persistedProjectCache struct {
	Targets   []ServiceTarget `json:"targets"`
	ExpiresAt time.Time       `json:"expires_at"`
}

// cachedProject is a simplified representation of a project from the GetProjects response,
// suitable for JSON serialization and used as the common type for the discovery loop.
type cachedProject struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Environments []cachedEnvEdge `json:"environments"`
	Services     []cachedSvcEdge `json:"services"`
}

type cachedEnvEdge struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type cachedSvcEdge struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// persistedProjectList is the JSON-serialized form of a workspace's project list cache.
type persistedProjectList struct {
	Projects  []cachedProject `json:"projects"`
	ExpiresAt time.Time       `json:"expires_at"`
}

// Discovery handles enumerating Railway resources and building collection targets.
type Discovery struct {
	client  RailwayAPI
	store   StateStore
	clock   clockwork.Clock
	filters config.FiltersConfig
	logger  *slog.Logger

	compiledProjects     []compiledFilter
	compiledServices     []compiledFilter
	compiledEnvironments []compiledFilter

	// Cache TTL configuration
	workspaceTTL   time.Duration // base TTL for workspace list
	projectTTL     time.Duration // base TTL for per-project discovery
	projectListTTL time.Duration // base TTL for project list (GetProjects) cache
	jitter         time.Duration // ± jitter applied to each TTL

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

	// Project list cache: per-workspace, refreshed at projectListTTL ± jitter
	projListMu    sync.Mutex
	projListCache map[string]*cachedEntry[[]cachedProject] // keyed by workspaceID
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
	Client         RailwayAPI
	Store          StateStore // persistent store for caching discovery data across restarts
	Clock          clockwork.Clock
	Filters        config.FiltersConfig
	Workspaces     []Workspace   // static workspace list (from config or Me response)
	WorkspaceTTL   time.Duration // base TTL for workspace discovery (default 1h)
	ProjectTTL     time.Duration // base TTL for per-project discovery (default 1h)
	ProjectListTTL time.Duration // base TTL for project list cache (default 4h)
	Jitter         time.Duration // ± jitter for TTLs (default 15m)
	Logger         *slog.Logger
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
	plTTL := cfg.ProjectListTTL
	if plTTL == 0 {
		plTTL = 4 * time.Hour
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
		projectTTL:           pTTL,
		projectListTTL:       plTTL,
		jitter:               jitter,
		projCache:            make(map[string]*projectCache),
		projListCache:        make(map[string]*cachedEntry[[]cachedProject]),
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
	start := time.Now()
	workspaces, err := d.resolveWorkspaces(ctx)
	if err != nil {
		return err
	}

	d.logger.Info("discovering Railway resources", "workspaces", len(workspaces))

	var allTargets []ServiceTarget

	for _, ws := range workspaces {
		d.logger.Debug("discovering projects in workspace", "workspace", ws.Name, "id", ws.ID)

		projects, err := d.resolveProjectList(ctx, ws)
		if err != nil {
			d.logger.Error("failed to get projects for workspace", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
			continue
		}

		var (
			mu      sync.Mutex
			results []ServiceTarget
		)

		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(4)

		for _, p := range projects {
			if !d.matchFilter(p.Name, p.ID, d.compiledProjects) {
				continue
			}

			g.Go(func() error {
				targets := d.discoverCachedProject(gCtx, p, ws)
				mu.Lock()
				results = append(results, targets...)
				mu.Unlock()
				return nil
			})
		}
		_ = g.Wait()
		allTargets = append(allTargets, results...)
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

// resolveProjectList returns the project list for a workspace, using in-memory cache,
// bbolt persistent cache, or the API (in that priority order).
func (d *Discovery) resolveProjectList(ctx context.Context, ws Workspace) ([]cachedProject, error) {
	d.projListMu.Lock()
	defer d.projListMu.Unlock()

	// Check in-memory cache first
	if cached, ok := d.projListCache[ws.ID]; ok && !cached.expired() {
		d.logger.Debug("using cached project list", "workspace", ws.Name,
			"ttl", time.Until(cached.expiresAt).Round(time.Second))
		return cached.data, nil
	}

	// Check bbolt persistent cache
	if d.store != nil {
		raw, err := d.store.GetProjectListCache(ws.ID)
		if err != nil {
			d.logger.Warn("failed to read project list cache from store", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
		} else if raw != nil {
			var persisted persistedProjectList
			if err := json.Unmarshal(raw, &persisted); err != nil {
				d.logger.Warn("failed to unmarshal project list cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
			} else if d.clock.Now().Before(persisted.ExpiresAt) {
				d.logger.Debug("loaded project list from persistent cache", "workspace", ws.Name,
					"projects", len(persisted.Projects), "ttl", time.Until(persisted.ExpiresAt).Round(time.Second))
				// Populate in-memory cache
				d.projListCache[ws.ID] = &cachedEntry[[]cachedProject]{
					data:      persisted.Projects,
					expiresAt: persisted.ExpiresAt,
				}
				return persisted.Projects, nil
			}
		}
	}

	// Fetch from API
	wsID := ws.ID
	resp, err := d.client.GetProjects(ctx, &wsID)
	if err != nil {
		// If in-memory cache exists but expired, return stale data on error
		if cached, ok := d.projListCache[ws.ID]; ok {
			d.logger.Warn("failed to refresh project list, using stale cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
			return cached.data, nil
		}
		return nil, err
	}

	if n := len(resp.Projects.Edges); n >= 100 {
		d.logger.Warn("project list hit page limit for workspace; some projects may be missing", "workspace", ws.Name, "count", n)
	}

	// Convert API response to cachedProject slice
	projects := make([]cachedProject, 0, len(resp.Projects.Edges))
	for _, pe := range resp.Projects.Edges {
		p := pe.Node
		envs := make([]cachedEnvEdge, 0, len(p.Environments.Edges))
		for _, ee := range p.Environments.Edges {
			envs = append(envs, cachedEnvEdge{ID: ee.Node.Id, Name: ee.Node.Name})
		}
		svcs := make([]cachedSvcEdge, 0, len(p.Services.Edges))
		for _, se := range p.Services.Edges {
			svcs = append(svcs, cachedSvcEdge{ID: se.Node.Id, Name: se.Node.Name})
		}
		projects = append(projects, cachedProject{
			ID:           p.Id,
			Name:         p.Name,
			Environments: envs,
			Services:     svcs,
		})
	}

	// Cache with jittered TTL
	ttl := jitteredTTL(d.projectListTTL, d.jitter)
	expiresAt := d.clock.Now().Add(ttl)
	d.projListCache[ws.ID] = &cachedEntry[[]cachedProject]{
		data:      projects,
		expiresAt: expiresAt,
	}
	d.logger.Debug("cached project list", "workspace", ws.Name, "projects", len(projects), "ttl", ttl.Round(time.Second))

	// Persist to bbolt
	if d.store != nil {
		persisted := persistedProjectList{
			Projects:  projects,
			ExpiresAt: expiresAt,
		}
		data, err := json.Marshal(persisted)
		if err != nil {
			d.logger.Warn("failed to marshal project list cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
		} else if err := d.store.SetProjectListCache(ws.ID, data); err != nil {
			d.logger.Warn("failed to persist project list cache", "workspace", ws.Name, "workspace_id", ws.ID, "error", err)
		}
	}

	return projects, nil
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

// discoverCachedProject returns targets for a single project, using in-memory cache,
// bbolt persistent cache, or API discovery (in that priority order).
func (d *Discovery) discoverCachedProject(ctx context.Context, p cachedProject, ws Workspace) []ServiceTarget {
	d.projMu.Lock()
	if cached, ok := d.projCache[p.ID]; ok && d.clock.Now().Before(cached.expiresAt) {
		targets := cached.targets
		ttl := time.Until(cached.expiresAt).Round(time.Second)
		d.projMu.Unlock()
		d.logger.Debug("using cached project discovery", "project", p.Name,
			"targets", len(targets), "ttl", ttl)
		return targets
	}
	d.projMu.Unlock()

	// Check bbolt persistent cache before making API calls
	if d.store != nil {
		raw, err := d.store.GetDiscoveryCache(p.ID)
		if err != nil {
			d.logger.Warn("failed to read discovery cache from store", "project", p.Name, "project_id", p.ID, "error", err)
		} else if raw != nil {
			var persisted persistedProjectCache
			if err := json.Unmarshal(raw, &persisted); err != nil {
				d.logger.Warn("failed to unmarshal discovery cache", "project", p.Name, "project_id", p.ID, "error", err)
			} else if d.clock.Now().Before(persisted.ExpiresAt) {
				d.logger.Debug("loaded project from persistent cache", "project", p.Name,
					"targets", len(persisted.Targets), "ttl", time.Until(persisted.ExpiresAt).Round(time.Second))
				// Populate in-memory cache
				d.projMu.Lock()
				d.projCache[p.ID] = &projectCache{
					targets:   persisted.Targets,
					expiresAt: persisted.ExpiresAt,
				}
				d.projMu.Unlock()
				return persisted.Targets
			}
		}
	}

	d.logger.Info("discovering project (cache miss)", "project", p.Name, "id", p.ID)

	var targets []ServiceTarget

	if n := len(p.Environments); n >= 100 {
		d.logger.Warn("environment list hit page limit for project; some environments may be missing", "project", p.Name, "count", n)
	}
	for _, e := range p.Environments {
		if !d.matchFilter(e.Name, e.ID, d.compiledEnvironments) {
			continue
		}

		if n := len(p.Services); n >= 100 {
			d.logger.Warn("service list hit page limit for project; some services may be missing", "project", p.Name, "count", n)
		}
		for _, s := range p.Services {
			if !d.matchFilter(s.Name, s.ID, d.compiledServices) {
				continue
			}

			target := ServiceTarget{
				ProjectID:       p.ID,
				ProjectName:     p.Name,
				ServiceID:       s.ID,
				ServiceName:     s.Name,
				EnvironmentID:   e.ID,
				EnvironmentName: e.Name,
			}

			// Try to get the latest deployment for log collection
			first := 1
			deplResp, err := d.client.GetDeployments(ctx, p.ID, e.ID, s.ID, &first, nil)
			if err != nil {
				d.logger.Warn("failed to get deployments", "service", s.Name, "service_id", s.ID, "project", p.Name, "project_id", p.ID, "environment", e.Name, "environment_id", e.ID, "error", err)
			} else if len(deplResp.Deployments.Edges) > 0 {
				target.DeploymentID = deplResp.Deployments.Edges[0].Node.Id
			}

			// Get region from service instance
			siResp, err := d.client.GetServiceInstance(ctx, e.ID, s.ID)
			if err != nil {
				d.logger.Warn("failed to get service instance", "service", s.Name, "service_id", s.ID, "project", p.Name, "project_id", p.ID, "environment", e.Name, "environment_id", e.ID, "error", err)
			} else if siResp.ServiceInstance.Region != nil {
				target.Region = *siResp.ServiceInstance.Region
			}

			targets = append(targets, target)
		}
	}

	// Cache with jittered TTL
	ttl := jitteredTTL(d.projectTTL, d.jitter)
	expiresAt := d.clock.Now().Add(ttl)
	d.projMu.Lock()
	d.projCache[p.ID] = &projectCache{
		targets:   targets,
		expiresAt: expiresAt,
	}
	d.projMu.Unlock()

	d.logger.Debug("cached project discovery", "project", p.Name, "targets", len(targets), "ttl", ttl.Round(time.Second))

	// Persist to bbolt
	if d.store != nil {
		persisted := persistedProjectCache{
			Targets:   targets,
			ExpiresAt: expiresAt,
		}
		data, err := json.Marshal(persisted)
		if err != nil {
			d.logger.Warn("failed to marshal discovery cache", "project", p.Name, "project_id", p.ID, "error", err)
		} else if err := d.store.SetDiscoveryCache(p.ID, data); err != nil {
			d.logger.Warn("failed to persist discovery cache", "project", p.Name, "project_id", p.ID, "error", err)
		}
	}

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
