package collector

import (
	"context"
	"log/slog"
	"regexp"
	"sync"

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

// Discovery handles enumerating Railway resources and building collection targets.
type Discovery struct {
	client      *railway.Client
	filters     config.FiltersConfig
	workspaceID string
	logger      *slog.Logger

	compiledProjects     []compiledFilter
	compiledServices     []compiledFilter
	compiledEnvironments []compiledFilter

	mu      sync.RWMutex
	targets []ServiceTarget
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

func NewDiscovery(client *railway.Client, filters config.FiltersConfig, workspaceID string, logger *slog.Logger) *Discovery {
	return &Discovery{
		client:               client,
		filters:              filters,
		workspaceID:          workspaceID,
		logger:               logger,
		compiledProjects:     compileFilters(filters.Projects, logger),
		compiledServices:     compileFilters(filters.Services, logger),
		compiledEnvironments: compileFilters(filters.Environments, logger),
	}
}

// Targets returns the current set of discovered service targets.
func (d *Discovery) Targets() []ServiceTarget {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]ServiceTarget, len(d.targets))
	copy(out, d.targets)
	return out
}

// Refresh re-discovers all Railway resources and updates targets.
func (d *Discovery) Refresh(ctx context.Context) error {
	d.logger.Info("discovering Railway resources")

	var wsFilter *string
	if d.workspaceID != "" {
		wsFilter = &d.workspaceID
	}
	resp, err := d.client.GetProjects(ctx, wsFilter)
	if err != nil {
		return err
	}

	var targets []ServiceTarget

	if n := len(resp.Projects.Edges); n >= 100 {
		d.logger.Warn("project list returned 100 entries (the default page size); there may be more projects not yet discovered — pagination is not implemented", "count", n)
	}

	for _, pe := range resp.Projects.Edges {
		p := pe.Node
		if !d.matchFilter(p.Name, p.Id, d.compiledProjects) {
			continue
		}
		d.logger.Debug("discovered project", "project", p.Name, "id", p.Id)

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
	}

	d.mu.Lock()
	d.targets = targets
	d.mu.Unlock()

	d.logger.Info("discovery complete", "targets", len(targets))
	return nil
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
