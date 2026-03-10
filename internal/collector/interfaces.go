package collector

import (
	"context"
	"time"

	"github.com/xevion/railway-collector/internal/railway"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mocks.go -package=mocks

// RailwayAPI abstracts the Railway GraphQL client for testing.
type RailwayAPI interface {
	Me(ctx context.Context) (*railway.MeResponse, error)
	GetProjects(ctx context.Context, workspaceID *string) (*railway.ProjectsResponse, error)
	GetDeployments(ctx context.Context, projectID, envID, serviceID string, first *int, after *string) (*railway.DeploymentsResponse, error)
	GetServiceInstance(ctx context.Context, envID, serviceID string) (*railway.ServiceInstanceQueryResponse, error)
	GetMetrics(ctx context.Context, projectID, serviceID, envID *string, startDate string, endDate *string, measurements []railway.MetricMeasurement, groupBy []railway.MetricTag, sampleRate, avgWindow *int) (*railway.MetricsResponse, error)
	GetEnvironmentLogs(ctx context.Context, environmentID string, filter *string, afterDate, beforeDate *string, afterLimit, beforeLimit *int, anchorDate *string) (*railway.EnvironmentLogsQueryResponse, error)
	GetDeploymentLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*railway.DeploymentLogsQueryResponse, error)
	GetBuildLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*railway.BuildLogsQueryResponse, error)
	GetHttpLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*railway.HttpLogsQueryResponse, error)
	IsRateLimited() (bool, time.Duration)
	RateLimitInfo() (remaining int, resetAt time.Time)
}

// StateStore abstracts persistent cursor and cache storage for testing.
type StateStore interface {
	GetLogCursor(deploymentID, logType string) time.Time
	SetLogCursor(deploymentID, logType string, ts time.Time) error
	GetMetricCursor(projectID string) time.Time
	SetMetricCursor(projectID string, ts time.Time) error
	GetDiscoveryCache(projectID string) ([]byte, error)
	SetDiscoveryCache(projectID string, data []byte) error
	ListDiscoveryCache() (map[string][]byte, error)
	DeleteDiscoveryCache(projectID string) error
	GetProjectListCache(workspaceID string) ([]byte, error)
	SetProjectListCache(workspaceID string, data []byte) error
	GetCoverage(key string) ([]byte, error)
	SetCoverage(key string, data []byte) error
	ListCoverage() (map[string][]byte, error)
	Close() error
}

// TargetProvider abstracts discovery for testing.
type TargetProvider interface {
	Targets() []ServiceTarget
	Refresh(ctx context.Context) error
}

// Collector is implemented by MetricsCollector and LogsCollector.
type Collector interface {
	Collect(ctx context.Context) error
}
