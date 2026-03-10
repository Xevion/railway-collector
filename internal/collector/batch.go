package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/xevion/railway-collector/internal/railway"
)

// Batch is a group of WorkItems with the same (Kind, BatchKey) that can be
// merged into a single aliased GraphQL request.
type Batch struct {
	Kind     QueryKind
	BatchKey string
	TaskType TaskType
	Items    []WorkItem
}

// GroupByCompatibility groups work items into batches by (Kind, BatchKey).
// Items with the same Kind and BatchKey can be merged into one aliased query.
// Returns batches sorted by priority (Metrics > Logs > Discovery > Backfill).
func GroupByCompatibility(items []WorkItem) []Batch {
	type batchKey struct {
		kind     QueryKind
		batchKey string
	}

	groups := make(map[batchKey][]WorkItem)
	taskTypes := make(map[batchKey]TaskType)
	var order []batchKey

	for _, item := range items {
		key := batchKey{kind: item.Kind, batchKey: item.BatchKey}
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], item)
		taskTypes[key] = item.TaskType
	}

	batches := make([]Batch, 0, len(groups))
	for _, key := range order {
		batches = append(batches, Batch{
			Kind:     key.kind,
			BatchKey: key.batchKey,
			TaskType: taskTypes[key],
			Items:    groups[key],
		})
	}

	// Sort by task type priority: Metrics(0) < Logs(1) < Discovery(2) < Backfill(3)
	sort.Slice(batches, func(i, j int) bool {
		return batches[i].TaskType < batches[j].TaskType
	})

	return batches
}

// OperationNameForBatch returns the GraphQL operation name for a batch.
// Used for the Cloudflare query parameter and logging.
func OperationNameForBatch(kind QueryKind) string {
	switch kind {
	case QueryMetrics:
		return "BatchMetrics"
	case QueryEnvironmentLogs:
		return "BatchEnvironmentLogs"
	case QueryBuildLogs, QueryHttpLogs:
		return "BatchDeploymentLogs"
	case QueryDiscovery:
		return "Discovery"
	default:
		return "BatchQuery"
	}
}

// BuildQueryFromBatch constructs a GraphQL query string and variables from a batch
// of compatible work items. For items with per-item parameters (e.g. different
// startDate cursors), uses the earliest/most inclusive value.
func BuildQueryFromBatch(batch Batch) (operationName, query string, variables map[string]any, err error) {
	operationName = OperationNameForBatch(batch.Kind)

	switch batch.Kind {
	case QueryMetrics:
		return buildMetricsBatch(operationName, batch)
	case QueryEnvironmentLogs:
		return buildEnvLogsBatch(operationName, batch)
	case QueryBuildLogs:
		return buildBuildLogsBatch(operationName, batch)
	case QueryHttpLogs:
		return buildHttpLogsBatch(operationName, batch)
	case QueryDiscovery:
		// Discovery doesn't use raw queries -- handled specially by the scheduler.
		return operationName, "", nil, nil
	default:
		return "", "", nil, fmt.Errorf("unknown query kind: %s", batch.Kind)
	}
}

func buildMetricsBatch(opName string, batch Batch) (string, string, map[string]any, error) {
	if len(batch.Items) == 0 {
		return opName, "", nil, fmt.Errorf("empty metrics batch")
	}

	// Extract shared params from the first item (all items share the same BatchKey
	// so these params are identical).
	first := batch.Items[0].Params
	measurements, _ := first["measurements"].([]railway.MetricMeasurement)
	groupBy, _ := first["groupBy"].([]railway.MetricTag)
	sampleRate, _ := first["sampleRateSeconds"].(int)
	avgWindow, _ := first["averagingWindowSeconds"].(int)

	// Build per-project items with their own startDate/endDate.
	var batchItems []railway.MetricsBatchItem
	for _, item := range batch.Items {
		sd, _ := item.Params["startDate"].(string)
		bi := railway.MetricsBatchItem{
			ProjectID: item.AliasKey,
			StartDate: sd,
		}
		if ed, ok := item.Params["endDate"].(string); ok {
			bi.EndDate = &ed
		}
		batchItems = append(batchItems, bi)
	}

	var sampleRatePtr *int
	if sampleRate > 0 {
		sampleRatePtr = &sampleRate
	}
	var avgWindowPtr *int
	if avgWindow > 0 {
		avgWindowPtr = &avgWindow
	}

	query, vars := railway.BuildBatchMetricsQuery(
		batchItems, measurements, groupBy,
		sampleRatePtr, avgWindowPtr,
	)
	return opName, query, vars, nil
}

func buildEnvLogsBatch(opName string, batch Batch) (string, string, map[string]any, error) {
	if len(batch.Items) == 0 {
		return opName, "", nil, fmt.Errorf("empty env logs batch")
	}

	// Use earliest afterDate across all items.
	var earliestAfterDate *string
	for _, item := range batch.Items {
		if ad, ok := item.Params["afterDate"].(string); ok {
			if earliestAfterDate == nil || ad < *earliestAfterDate {
				earliestAfterDate = &ad
			}
		}
	}

	// Use the latest beforeDate across all items (backfill items set this;
	// realtime items omit it, leaving the query open-ended).
	var latestBeforeDate *string
	for _, item := range batch.Items {
		if bd, ok := item.Params["beforeDate"].(string); ok {
			if latestBeforeDate == nil || bd > *latestBeforeDate {
				latestBeforeDate = &bd
			}
		}
	}

	// afterLimit is shared (same BatchKey).
	var afterLimit *int
	if al, ok := batch.Items[0].Params["afterLimit"].(int); ok {
		afterLimit = &al
	}

	var envIDs []string
	for _, item := range batch.Items {
		envIDs = append(envIDs, item.AliasKey)
	}

	query, vars := railway.BuildBatchEnvironmentLogsQuery(
		envIDs, earliestAfterDate, latestBeforeDate, afterLimit, nil,
	)
	return opName, query, vars, nil
}

func buildBuildLogsBatch(opName string, batch Batch) (string, string, map[string]any, error) {
	if len(batch.Items) == 0 {
		return opName, "", nil, fmt.Errorf("empty build logs batch")
	}

	// Use earliest startDate.
	var earliestStart *string
	for _, item := range batch.Items {
		if sd, ok := item.Params["startDate"].(string); ok {
			if earliestStart == nil || sd < *earliestStart {
				earliestStart = &sd
			}
		}
	}

	var limit *int
	if l, ok := batch.Items[0].Params["limit"].(int); ok {
		limit = &l
	}

	var requests []railway.DeploymentLogRequest
	for _, item := range batch.Items {
		requests = append(requests, railway.DeploymentLogRequest{
			DeploymentID:     item.AliasKey,
			IncludeBuildLogs: true,
		})
	}

	query, vars := railway.BuildBatchDeploymentLogsQuery(requests, limit, earliestStart, nil)
	return opName, query, vars, nil
}

func buildHttpLogsBatch(opName string, batch Batch) (string, string, map[string]any, error) {
	if len(batch.Items) == 0 {
		return opName, "", nil, fmt.Errorf("empty http logs batch")
	}

	var earliestStart *string
	for _, item := range batch.Items {
		if sd, ok := item.Params["startDate"].(string); ok {
			if earliestStart == nil || sd < *earliestStart {
				earliestStart = &sd
			}
		}
	}

	var limit *int
	if l, ok := batch.Items[0].Params["limit"].(int); ok {
		limit = &l
	}

	var requests []railway.DeploymentLogRequest
	for _, item := range batch.Items {
		requests = append(requests, railway.DeploymentLogRequest{
			DeploymentID:    item.AliasKey,
			IncludeHttpLogs: true,
		})
	}

	query, vars := railway.BuildBatchDeploymentLogsQuery(requests, limit, earliestStart, nil)
	return opName, query, vars, nil
}

// DispatchResults delivers per-alias results from a RawQueryResponse back to
// the originating generators. Each generator receives its alias's data slice.
//
// generatorMap maps work item IDs to their originating TaskGenerator.
// For full HTTP failures, err is delivered to all items.
func DispatchResults(
	ctx context.Context,
	batch Batch,
	resp *railway.RawQueryResponse,
	queryErr error,
	generatorMap map[string]TaskGenerator,
) {
	for _, item := range batch.Items {
		gen, ok := generatorMap[item.ID]
		if !ok {
			continue
		}

		if queryErr != nil {
			gen.Deliver(ctx, item, nil, queryErr)
			continue
		}

		// Look up this item's alias in the response.
		alias := railway.SanitizeAlias(item.AliasKey)

		// For deployment logs with suffixed aliases, try the specific suffix.
		switch item.Kind {
		case QueryBuildLogs:
			alias += "_build"
		case QueryHttpLogs:
			alias += "_http"
		}

		data, exists := resp.Data[alias]
		if !exists {
			// Check if there's a GraphQL error for this alias.
			var aliasErr error
			for _, gqlErr := range resp.Errors {
				if len(gqlErr.Path) > 0 && gqlErr.Path[0] == alias {
					aliasErr = fmt.Errorf("GraphQL error: %s", gqlErr.Message)
					break
				}
			}
			if aliasErr != nil {
				gen.Deliver(ctx, item, nil, aliasErr)
			} else {
				gen.Deliver(ctx, item, nil, fmt.Errorf("alias %q not found in response", alias))
			}
			continue
		}

		// Check for per-alias GraphQL errors (data may be null with an error).
		if string(data) == "null" {
			var aliasErr error
			for _, gqlErr := range resp.Errors {
				if len(gqlErr.Path) > 0 && gqlErr.Path[0] == alias {
					aliasErr = fmt.Errorf("GraphQL error: %s", gqlErr.Message)
					break
				}
			}
			if aliasErr != nil {
				gen.Deliver(ctx, item, nil, aliasErr)
				continue
			}
		}

		gen.Deliver(ctx, item, json.RawMessage(data), nil)
	}
}
