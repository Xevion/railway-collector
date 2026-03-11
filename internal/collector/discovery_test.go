package collector_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/mocks"
	"github.com/xevion/railway-collector/internal/config"
	"github.com/xevion/railway-collector/internal/railway"
	"github.com/xevion/railway-collector/internal/state"
)

// buildDiscoverAllResponse constructs a DiscoverAllResponse for testing.
// It returns a response with 2 projects, each with 1 environment and 2 service instances.
// The first service instance in each env has a LatestDeployment and a Region.
// The second has neither.
func buildDiscoverAllResponse() *railway.DiscoverAllResponse {
	// Type aliases for brevity
	type Project = railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProject
	type Env = railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdgeNodeEnvironment
	type ServiceInstance = railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdgeNodeEnvironmentServiceInstancesEnvironmentServiceInstancesConnectionEdgesEnvironmentServiceInstancesConnectionEdgeNodeServiceInstance
	type Deployment = railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdgeNodeEnvironmentServiceInstancesEnvironmentServiceInstancesConnectionEdgesEnvironmentServiceInstancesConnectionEdgeNodeServiceInstanceLatestDeployment

	makeEnv := func(envID, envName, proj1ServiceID, proj1ServiceName, proj2ServiceID, proj2ServiceName string) railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdgeNodeEnvironment {
		region := "us-west2"
		return Env{
			Id:          envID,
			Name:        envName,
			IsEphemeral: false,
			ServiceInstances: railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdgeNodeEnvironmentServiceInstancesEnvironmentServiceInstancesConnection{
				Edges: []railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdgeNodeEnvironmentServiceInstancesEnvironmentServiceInstancesConnectionEdgesEnvironmentServiceInstancesConnectionEdge{
					{
						Node: ServiceInstance{
							ServiceId:     proj1ServiceID,
							ServiceName:   proj1ServiceName,
							EnvironmentId: envID,
							Region:        &region,
							LatestDeployment: &Deployment{
								Id:     proj1ServiceID + "-deploy",
								Status: railway.DeploymentStatus("SUCCESS"),
							},
						},
					},
					{
						Node: ServiceInstance{
							ServiceId:        proj2ServiceID,
							ServiceName:      proj2ServiceName,
							EnvironmentId:    envID,
							Region:           nil,
							LatestDeployment: nil,
						},
					},
				},
				PageInfo: railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdgeNodeEnvironmentServiceInstancesEnvironmentServiceInstancesConnectionPageInfo{
					HasNextPage: false,
				},
			},
		}
	}

	makeProject := func(projectID, projectName, envID, envName, svc1ID, svc1Name, svc2ID, svc2Name string) railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdge {
		return railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdge{
			Node: Project{
				Id:   projectID,
				Name: projectName,
				Environments: railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnection{
					Edges: []railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionEdgesProjectEnvironmentsConnectionEdge{
						{Node: makeEnv(envID, envName, svc1ID, svc1Name, svc2ID, svc2Name)},
					},
					PageInfo: railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdgeNodeProjectEnvironmentsProjectEnvironmentsConnectionPageInfo{
						HasNextPage: false,
					},
				},
			},
		}
	}

	return &railway.DiscoverAllResponse{
		Projects: railway.DiscoverAllProjectsQueryProjectsConnection{
			Edges: []railway.DiscoverAllProjectsQueryProjectsConnectionEdgesQueryProjectsConnectionEdge{
				makeProject("proj-1", "project-one", "env-1", "production", "svc-1a", "service-alpha", "svc-1b", "service-beta"),
				makeProject("proj-2", "project-two", "env-2", "staging", "svc-2a", "service-gamma", "svc-2b", "service-delta"),
			},
			PageInfo: railway.DiscoverAllProjectsQueryProjectsConnectionPageInfo{
				HasNextPage: false,
			},
		},
	}
}

func TestDiscovery_Refresh_HappyPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockRailwayAPI(ctrl)
	clk := clockwork.NewFakeClock()
	ws := collector.Workspace{ID: "ws-1", Name: "test-workspace"}

	resp := buildDiscoverAllResponse()

	mockAPI.EXPECT().
		DiscoverAll(gomock.Any(), gomock.Eq(&ws.ID), gomock.Any()).
		Return(resp, nil).
		Times(1)

	d := collector.NewDiscovery(collector.DiscoveryConfig{
		Client:       mockAPI,
		Store:        nil,
		Clock:        clk,
		Filters:      config.FiltersConfig{},
		Workspaces:   []collector.Workspace{ws},
		WorkspaceTTL: time.Hour,
		Jitter:       0,
		Logger:       discardLogger,
	})

	err := d.Refresh(context.Background())
	require.NoError(t, err)

	targets := d.Targets()
	assert.Len(t, targets, 4, "expected 4 total targets (2 projects × 1 env × 2 services)")

	// Build a lookup map by service ID for easier assertions
	byService := make(map[string]collector.ServiceTarget)
	for _, t := range targets {
		byService[t.ServiceID] = t
	}

	// Service with deployment and region
	svc1a, ok := byService["svc-1a"]
	require.True(t, ok, "expected svc-1a in targets")
	assert.Equal(t, "proj-1", svc1a.ProjectID)
	assert.Equal(t, "env-1", svc1a.EnvironmentID)
	assert.NotEmpty(t, svc1a.DeploymentID, "expected non-empty DeploymentID for svc-1a")
	assert.NotEmpty(t, svc1a.Region, "expected non-empty Region for svc-1a")

	// Service without deployment or region
	svc1b, ok := byService["svc-1b"]
	require.True(t, ok, "expected svc-1b in targets")
	assert.Equal(t, "proj-1", svc1b.ProjectID)
	assert.Equal(t, "env-1", svc1b.EnvironmentID)
	assert.Empty(t, svc1b.DeploymentID, "expected empty DeploymentID for svc-1b")
	assert.Empty(t, svc1b.Region, "expected empty Region for svc-1b")

	// Spot check second project
	svc2a, ok := byService["svc-2a"]
	require.True(t, ok, "expected svc-2a in targets")
	assert.Equal(t, "proj-2", svc2a.ProjectID)
	assert.Equal(t, "env-2", svc2a.EnvironmentID)
	assert.NotEmpty(t, svc2a.DeploymentID)
	assert.NotEmpty(t, svc2a.Region)

	svc2b, ok := byService["svc-2b"]
	require.True(t, ok, "expected svc-2b in targets")
	assert.Equal(t, "proj-2", svc2b.ProjectID)
	assert.Equal(t, "env-2", svc2b.EnvironmentID)
	assert.Empty(t, svc2b.DeploymentID)
	assert.Empty(t, svc2b.Region)
}

func TestDiscovery_Refresh_ProjectFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockRailwayAPI(ctrl)
	clk := clockwork.NewFakeClock()
	ws := collector.Workspace{ID: "ws-1", Name: "test-workspace"}

	resp := buildDiscoverAllResponse()

	mockAPI.EXPECT().
		DiscoverAll(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(resp, nil).
		Times(1)

	// Filter to only project-one by name
	d := collector.NewDiscovery(collector.DiscoveryConfig{
		Client: mockAPI,
		Store:  nil,
		Clock:  clk,
		Filters: config.FiltersConfig{
			Projects: []string{"project-one"},
		},
		Workspaces:   []collector.Workspace{ws},
		WorkspaceTTL: time.Hour,
		Jitter:       0,
		Logger:       discardLogger,
	})

	err := d.Refresh(context.Background())
	require.NoError(t, err)

	targets := d.Targets()
	assert.Len(t, targets, 2, "expected 2 targets (only project-one's services)")

	for _, target := range targets {
		assert.Equal(t, "proj-1", target.ProjectID, "all targets should belong to proj-1")
	}
}

func TestDiscovery_Refresh_CacheHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockRailwayAPI(ctrl)
	clk := clockwork.NewFakeClock()
	ws := collector.Workspace{ID: "ws-1", Name: "test-workspace"}

	resp := buildDiscoverAllResponse()

	// Expect DiscoverAll to be called exactly once across both Refresh calls.
	mockAPI.EXPECT().
		DiscoverAll(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(resp, nil).
		Times(1)

	d := collector.NewDiscovery(collector.DiscoveryConfig{
		Client:       mockAPI,
		Store:        nil,
		Clock:        clk,
		Filters:      config.FiltersConfig{},
		Workspaces:   []collector.Workspace{ws},
		WorkspaceTTL: time.Hour,
		Jitter:       0,
		Logger:       discardLogger,
	})

	err := d.Refresh(context.Background())
	require.NoError(t, err)

	// Second call should use in-memory cache (clock not advanced).
	err = d.Refresh(context.Background())
	require.NoError(t, err)

	// Gomock will verify Times(1) was satisfied on ctrl.Finish().
	targets := d.Targets()
	assert.Len(t, targets, 4, "targets should still be 4 after cached second Refresh")
}

func TestDiscovery_Refresh_PageInfoOverflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockRailwayAPI(ctrl)
	clk := clockwork.NewFakeClock()
	ws := collector.Workspace{ID: "ws-1", Name: "test-workspace"}

	resp := buildDiscoverAllResponse()
	// Simulate the projects page being truncated.
	resp.Projects.PageInfo.HasNextPage = true

	mockAPI.EXPECT().
		DiscoverAll(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(resp, nil).
		Times(1)

	d := collector.NewDiscovery(collector.DiscoveryConfig{
		Client:       mockAPI,
		Store:        nil,
		Clock:        clk,
		Filters:      config.FiltersConfig{},
		Workspaces:   []collector.Workspace{ws},
		WorkspaceTTL: time.Hour,
		Jitter:       0,
		Logger:       discardLogger,
	})

	// Must not panic even when HasNextPage is true.
	require.NotPanics(t, func() {
		err := d.Refresh(context.Background())
		require.NoError(t, err)
	})
}

func TestDiscovery_Refresh_StaleOnError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockRailwayAPI(ctrl)
	clk := clockwork.NewFakeClock()
	ws := collector.Workspace{ID: "ws-1", Name: "test-workspace"}

	resp := buildDiscoverAllResponse()

	gomock.InOrder(
		mockAPI.EXPECT().
			DiscoverAll(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(resp, nil),
		mockAPI.EXPECT().
			DiscoverAll(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, errors.New("api unavailable")),
	)

	d := collector.NewDiscovery(collector.DiscoveryConfig{
		Client:       mockAPI,
		Store:        nil,
		Clock:        clk,
		Filters:      config.FiltersConfig{},
		Workspaces:   []collector.Workspace{ws},
		WorkspaceTTL: time.Hour,
		Jitter:       0,
		Logger:       discardLogger,
	})

	// First refresh populates cache.
	err := d.Refresh(context.Background())
	require.NoError(t, err)
	assert.Len(t, d.Targets(), 4)

	// Advance clock past TTL so the cache appears expired.
	clk.Advance(2 * time.Hour)

	// Second refresh: cache is expired, API fails, stale fallback used.
	err = d.Refresh(context.Background())
	require.NoError(t, err, "Refresh should not error when stale cache is available")

	targets := d.Targets()
	assert.Len(t, targets, 4, "stale targets should still be returned after API error")
}

func TestDiscovery_Refresh_PersistentCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockAPI := mocks.NewMockRailwayAPI(ctrl)
	clk := clockwork.NewFakeClock()
	ws := collector.Workspace{ID: "ws-1", Name: "test-workspace"}

	resp := buildDiscoverAllResponse()

	// Expect exactly one API call (from the first Discovery instance only).
	mockAPI.EXPECT().
		DiscoverAll(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(resp, nil).
		Times(1)

	dbPath := filepath.Join(t.TempDir(), "state.db")
	store1, err := state.Open(dbPath)
	require.NoError(t, err)
	defer store1.Close()

	cfg := collector.DiscoveryConfig{
		Client:       mockAPI,
		Store:        store1,
		Clock:        clk,
		Filters:      config.FiltersConfig{},
		Workspaces:   []collector.Workspace{ws},
		WorkspaceTTL: time.Hour,
		Jitter:       0,
		Logger:       discardLogger,
	}

	// First instance: populates bbolt.
	d1 := collector.NewDiscovery(cfg)
	err = d1.Refresh(context.Background())
	require.NoError(t, err)
	assert.Len(t, d1.Targets(), 4)
	store1.Close()

	// Second instance: opens same DB, fresh in-memory state.
	store2, err := state.Open(dbPath)
	require.NoError(t, err)
	defer store2.Close()

	cfg.Store = store2
	d2 := collector.NewDiscovery(cfg)
	err = d2.Refresh(context.Background())
	require.NoError(t, err, "second Refresh should succeed using bbolt cache")

	targets := d2.Targets()
	assert.Len(t, targets, 4, "second instance should load 4 targets from bbolt")
}
