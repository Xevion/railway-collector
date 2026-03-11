package collector

import (
	"context"
	"encoding/json"
	"time"

	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/railway"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/mocks.go -package=mocks

// RailwayAPI abstracts the Railway GraphQL client for testing.
type RailwayAPI interface {
	Me(ctx context.Context) (*railway.MeResponse, error)
	DiscoverAll(ctx context.Context, workspaceID *string, isEphemeral *bool) (*railway.DiscoverAllResponse, error)
	RawQuery(ctx context.Context, operationName, query string, variables map[string]any) (*railway.RawQueryResponse, error)
	IsRateLimited() (bool, time.Duration)
	RateLimitInfo() (remaining int, resetAt time.Time)
}

// StateStore abstracts persistent cursor and cache storage for testing.
type StateStore interface {
	GetDiscoveryCache(key string) ([]byte, error)
	SetDiscoveryCache(key string, data []byte) error
	ListDiscoveryCache() (map[string][]byte, error)
	DeleteDiscoveryCache(key string) error
	GetCoverage(key string) ([]byte, error)
	SetCoverage(key string, data []byte) error
	ListCoverage() (map[string][]byte, error)
	Close() error
}

// TargetProvider abstracts discovery for testing.
type TargetProvider interface {
	Targets() []types.ServiceTarget
	Refresh(ctx context.Context) error
}

// TaskGenerator produces work items describing what data it needs.
// The scheduler polls generators, batches compatible items, executes
// the queries, and delivers results back.
type TaskGenerator interface {
	// Poll returns pending work items, or nil if idle.
	Poll(now time.Time) []types.WorkItem

	// Deliver sends results back for a previously emitted work item.
	// data is the per-alias JSON response; err is non-nil on query failure.
	Deliver(ctx context.Context, item types.WorkItem, data json.RawMessage, err error)

	// Type returns the task category for credit allocation.
	Type() types.TaskType

	// NextPoll returns the earliest time this generator will produce work.
	// Returns the zero time if the generator has no scheduled poll.
	NextPoll() time.Time
}
