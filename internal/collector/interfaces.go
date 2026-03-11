package collector

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xevion/railway-collector/internal/railway"
)

// TaskType identifies the category of a scheduled work item for credit allocation.
type TaskType int

const (
	TaskTypeMetrics TaskType = iota
	TaskTypeLogs
	TaskTypeDiscovery
)

// String returns a human-readable name for the task type.
func (t TaskType) String() string {
	switch t {
	case TaskTypeMetrics:
		return "metrics"
	case TaskTypeLogs:
		return "logs"
	case TaskTypeDiscovery:
		return "discovery"
	default:
		return "unknown"
	}
}

// QueryKind identifies the GraphQL root field for a work item.
// Items with the same (QueryKind, BatchKey) can be merged into one aliased query.
type QueryKind string

const (
	QueryMetrics         QueryKind = "metrics"
	QueryEnvironmentLogs QueryKind = "environmentLogs"
	QueryBuildLogs       QueryKind = "buildLogs"
	QueryHttpLogs        QueryKind = "httpLogs"
	QueryDiscovery       QueryKind = "discovery"
)

// WorkItem describes a unit of work that a TaskGenerator needs executed.
// The scheduler groups compatible items (same Kind + BatchKey) into
// batched GraphQL requests using aliases.
type WorkItem struct {
	// ID uniquely identifies this work item for correlating results.
	ID string

	// Kind is the GraphQL root field type.
	Kind QueryKind

	// TaskType is the credit allocation category.
	TaskType TaskType

	// AliasKey is the entity ID used as the alias key in a batched query
	// (e.g., projectID for metrics, environmentID for env logs).
	AliasKey string

	// BatchKey is an opaque string computed by the generator.
	// Items with the same (Kind, BatchKey) can be merged into one request.
	BatchKey string

	// Params are the query parameters for the query builder.
	Params map[string]any
}

//go:generate mockgen -source=interfaces.go -destination=mocks/mocks.go -package=mocks

// RailwayAPI abstracts the Railway GraphQL client for testing.
type RailwayAPI interface {
	Me(ctx context.Context) (*railway.MeResponse, error)
	GetProjects(ctx context.Context, workspaceID *string) (*railway.ProjectsResponse, error)
	GetDeployments(ctx context.Context, projectID, envID, serviceID string, first *int, after *string) (*railway.DeploymentsResponse, error)
	GetServiceInstance(ctx context.Context, envID, serviceID string) (*railway.ServiceInstanceQueryResponse, error)
	RawQuery(ctx context.Context, operationName, query string, variables map[string]any) (*railway.RawQueryResponse, error)
	IsRateLimited() (bool, time.Duration)
	RateLimitInfo() (remaining int, resetAt time.Time)
}

// StateStore abstracts persistent cursor and cache storage for testing.
type StateStore interface {
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

// TaskGenerator produces work items describing what data it needs.
// The scheduler polls generators, batches compatible items, executes
// the queries, and delivers results back.
type TaskGenerator interface {
	// Poll returns pending work items, or nil if idle.
	Poll(now time.Time) []WorkItem

	// Deliver sends results back for a previously emitted work item.
	// data is the per-alias JSON response; err is non-nil on query failure.
	Deliver(ctx context.Context, item WorkItem, data json.RawMessage, err error)

	// Type returns the task category for credit allocation.
	Type() TaskType

	// NextPoll returns the earliest time this generator will produce work.
	// Returns the zero time if the generator has no scheduled poll.
	NextPoll() time.Time
}
