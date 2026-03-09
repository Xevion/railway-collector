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

// Discovery handles enumerating Railway resources and building collection targets.
type Discovery struct {
	client  *railway.Client
	filters config.FiltersConfig
	logger  *slog.Logger

	mu      sync.RWMutex
	targets []ServiceTarget
}

func NewDiscovery(client *railway.Client, filters config.FiltersConfig, logger *slog.Logger) *Discovery {
	return &Discovery{
		client:  client,
		filters: filters,
		logger:  logger,
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

	resp, err := d.client.GetProjects(ctx, nil)
	if err != nil {
		return err
	}

	var targets []ServiceTarget

	for _, pe := range resp.Projects.Edges {
		p := pe.Node
		if !d.matchFilter(p.Name, p.Id, d.filters.Projects) {
			continue
		}
		d.logger.Debug("discovered project", "project", p.Name, "id", p.Id)

		for _, ee := range p.Environments.Edges {
			e := ee.Node
			if !d.matchFilter(e.Name, e.Id, d.filters.Environments) {
				continue
			}

			for _, se := range p.Services.Edges {
				s := se.Node
				if !d.matchFilter(s.Name, s.Id, d.filters.Services) {
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
func (d *Discovery) matchFilter(name, id string, filters []string) bool {
	if len(filters) == 0 {
		return true
	}
	for _, f := range filters {
		if f == name || f == id {
			return true
		}
		if re, err := regexp.Compile(f); err == nil {
			if re.MatchString(name) {
				return true
			}
		}
	}
	return false
}
