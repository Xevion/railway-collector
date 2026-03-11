package types

// TaskType identifies the category of a scheduled work item for credit allocation.
type TaskType int

const (
	TaskTypeMetrics TaskType = iota
	TaskTypeLogs
	TaskTypeDiscovery
	TaskTypeUsage
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
	case TaskTypeUsage:
		return "usage"
	default:
		return "unknown"
	}
}

// QueryKind identifies the GraphQL root field for a work item.
// Items with the same (QueryKind, BatchKey) can be merged into one aliased query.
type QueryKind string

const (
	QueryMetrics                    QueryKind = "metrics"
	QueryServiceMetrics             QueryKind = "serviceMetrics"
	QueryReplicaMetrics             QueryKind = "replicaMetrics"
	QueryHttpDurationMetrics        QueryKind = "httpDurationMetrics"
	QueryHttpMetricsGroupedByStatus QueryKind = "httpMetricsGroupedByStatus"
	QueryEnvironmentLogs            QueryKind = "environmentLogs"
	QueryBuildLogs                  QueryKind = "buildLogs"
	QueryHttpLogs                   QueryKind = "httpLogs"
	QueryDiscovery                  QueryKind = "discovery"
	QueryUsage                      QueryKind = "usage"
	QueryEstimatedUsage             QueryKind = "estimatedUsage"
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

// CompositeKey returns the service:environment composite key used for
// per-service coverage tracking and work item aliasing.
func (t ServiceTarget) CompositeKey() string {
	return t.ServiceID + ":" + t.EnvironmentID
}
