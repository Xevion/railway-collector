package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/xevion/railway-collector/internal/collector"
)

// DiscoveryCmd shows cached discovery data.
type DiscoveryCmd struct {
	Workspace string `help:"Show full cached JSON for a specific workspace ID."`
	Verbose   bool   `help:"Show all services and deployments per workspace."`
}

type discoverySummaryJSON struct {
	WorkspaceID string `json:"workspace_id"`
	Targets     int    `json:"targets"`
	ExpiresAt   string `json:"expires_at"`
	Expired     bool   `json:"expired"`
	CacheAge    string `json:"cache_age,omitempty"`
}

type discoveryDetailJSON struct {
	ProjectID     string `json:"project_id"`
	ServiceID     string `json:"service_id"`
	ServiceName   string `json:"service_name"`
	EnvironmentID string `json:"environment_id"`
	EnvName       string `json:"environment_name"`
	DeploymentID  string `json:"deployment_id"`
	Region        string `json:"region"`
}

func (cmd *DiscoveryCmd) Run(c *CLI) error {
	reader, err := openReader(c.State, c.Config)
	if err != nil {
		return err
	}
	defer reader.Close()

	entries, err := reader.DiscoveryEntries()
	if err != nil {
		return fmt.Errorf("reading discovery cache: %w", err)
	}

	if len(entries) == 0 {
		fmt.Println("No discovery cache entries found.")
		return nil
	}

	f := formatter(c.JSON)
	now := time.Now()

	// Single workspace detail mode
	if cmd.Workspace != "" {
		for _, e := range entries {
			if e.Key != cmd.Workspace {
				continue
			}
			var cached collector.PersistedWorkspaceDiscovery
			if err := json.Unmarshal(e.Value, &cached); err != nil {
				return fmt.Errorf("parsing discovery cache for %s: %w", e.Key, err)
			}

			if c.JSON {
				var details []discoveryDetailJSON
				for _, t := range cached.Targets {
					details = append(details, discoveryDetailJSON{
						ProjectID:     t.ProjectID,
						ServiceID:     t.ServiceID,
						ServiceName:   t.ServiceName,
						EnvironmentID: t.EnvironmentID,
						EnvName:       t.EnvironmentName,
						DeploymentID:  t.DeploymentID,
						Region:        t.Region,
					})
				}
				return f.WriteJSON(details)
			}

			headers := []string{"Service", "Service ID", "Environment", "Deployment ID", "Region"}
			var rows [][]string
			for _, t := range cached.Targets {
				rows = append(rows, []string{
					t.ServiceName,
					t.ServiceID,
					t.EnvironmentName,
					truncate(t.DeploymentID, 12),
					t.Region,
				})
			}
			f.WriteTable(headers, rows)
			return nil
		}
		fmt.Printf("No discovery cache found for workspace %s.\n", cmd.Workspace)
		return nil
	}

	// Summary mode
	type entry struct {
		workspaceID string
		cache       collector.PersistedWorkspaceDiscovery
	}

	var parsed []entry
	for _, e := range entries {
		var cached collector.PersistedWorkspaceDiscovery
		if err := json.Unmarshal(e.Value, &cached); err != nil {
			continue
		}
		parsed = append(parsed, entry{e.Key, cached})
	}

	if c.JSON {
		var jsonOut []discoverySummaryJSON
		for _, p := range parsed {
			jsonOut = append(jsonOut, discoverySummaryJSON{
				WorkspaceID: p.workspaceID,
				Targets:     len(p.cache.Targets),
				ExpiresAt:   p.cache.ExpiresAt.Format(time.RFC3339),
				Expired:     now.After(p.cache.ExpiresAt),
			})
		}
		return f.WriteJSON(jsonOut)
	}

	if cmd.Verbose {
		headers := []string{"Workspace ID", "Service", "Environment", "Deployment", "Region", "Expires"}
		var rows [][]string
		for _, p := range parsed {
			for _, t := range p.cache.Targets {
				marker := ""
				if now.After(p.cache.ExpiresAt) {
					marker = " <- expired"
				}
				rows = append(rows, []string{
					truncate(p.workspaceID, 12),
					t.ServiceName,
					t.EnvironmentName,
					truncate(t.DeploymentID, 12),
					t.Region,
					humanize.RelTime(p.cache.ExpiresAt, now, "ago", "from now") + marker,
				})
			}
		}
		f.WriteTable(headers, rows)
		return nil
	}

	// Default summary
	headers := []string{"Workspace ID", "Targets", "Expires", ""}
	var rows [][]string
	for _, p := range parsed {
		marker := ""
		if now.After(p.cache.ExpiresAt) {
			marker = "<- expired"
		}
		workspaceName := p.workspaceID
		rows = append(rows, []string{
			workspaceName,
			fmt.Sprintf("%d", len(p.cache.Targets)),
			humanize.RelTime(p.cache.ExpiresAt, now, "ago", "from now"),
			marker,
		})
	}
	f.WriteTable(headers, rows)
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
