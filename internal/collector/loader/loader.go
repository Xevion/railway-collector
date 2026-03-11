package loader

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/railway"
)

// Breadth costs per query type, determined by Railway API error messages.
const (
	BreadthMetrics                    = 12
	BreadthEnvironmentLogs            = 13
	BreadthBuildLogs                  = 13
	BreadthHttpLogs                   = 21
	BreadthHttpDurationMetrics        = 5
	BreadthHttpMetricsGroupedByStatus = 5
	BreadthReplicaMetrics             = 12 // assumed same as BreadthMetrics; same selection set shape
	BreadthUsage                      = 12
	BreadthEstimatedUsage             = 5

	// MaxBreadth is the per-request breadth cap imposed by the Railway API.
	MaxBreadth = 500

	// MaxAliasesPerRequest caps aliases per request to avoid "Problem processing
	// request" errors from the API's undocumented computation limits.
	MaxAliasesPerRequest = 20

	// MaxPointsPerRequest caps the estimated total data points in a single
	// request. The Railway API computation limit is around 45-60k points;
	// we target ~30k to stay safely under.
	MaxPointsPerRequest = 30000
)

// AliasFragment is a self-contained GraphQL alias with fully-namespaced
// variables. Fragments from different query types can be packed into a
// single request without variable collisions.
type AliasFragment struct {
	// Alias is the GraphQL alias name (e.g. "p_abc123").
	Alias string
	// Field is the GraphQL root field (e.g. "metrics", "environmentLogs").
	Field string
	// Args is the argument string (e.g. `projectId: "...", startDate: $startDate_p_abc123`).
	Args string
	// Selection is the field body / selection set.
	Selection string
	// VarDecls are the variable declarations (e.g. "$startDate_p_abc123: DateTime!").
	VarDecls []string
	// VarValues maps variable names to their values.
	VarValues map[string]any
	// Breadth is the breadth cost for this alias.
	Breadth int
	// EstimatedPoints is the estimated number of data points this alias will
	// return, based on time range, sample rate, and measurement count.
	EstimatedPoints int
	// Item is the original types.WorkItem that produced this fragment.
	Item types.WorkItem
}

// Request is a packed set of alias fragments that fit within the breadth budget.
type Request struct {
	Fragments       []AliasFragment
	Breadth         int
	EstimatedPoints int
}

// AssembleQuery builds a GraphQL query string and variables map from a Request.
func (r *Request) AssembleQuery() (query string, variables map[string]any) {
	variables = make(map[string]any)
	var varDecls []string
	var aliases []string

	for _, f := range r.Fragments {
		for _, decl := range f.VarDecls {
			varDecls = append(varDecls, decl)
		}
		for k, v := range f.VarValues {
			variables[k] = v
		}
		aliases = append(aliases, fmt.Sprintf("    %s: %s(%s) %s", f.Alias, f.Field, f.Args, f.Selection))
	}

	query = fmt.Sprintf("query Batch(%s) {\n%s\n}", strings.Join(varDecls, ", "), strings.Join(aliases, "\n"))
	return query, variables
}

// Items returns all WorkItems in this request.
func (r *Request) Items() []types.WorkItem {
	items := make([]types.WorkItem, len(r.Fragments))
	for i, f := range r.Fragments {
		items[i] = f.Item
	}
	return items
}

// Pack groups alias fragments into requests, each fitting within MaxBreadth.
// Fragments are packed in the order given (caller should sort by priority).
func Pack(fragments []AliasFragment) []Request {
	if len(fragments) == 0 {
		return nil
	}

	var requests []Request
	current := Request{}

	for _, f := range fragments {
		if f.Breadth > MaxBreadth {
			requests = append(requests, Request{
				Fragments:       []AliasFragment{f},
				Breadth:         f.Breadth,
				EstimatedPoints: f.EstimatedPoints,
			})
			continue
		}

		wouldExceed := current.Breadth+f.Breadth > MaxBreadth ||
			len(current.Fragments) >= MaxAliasesPerRequest ||
			(current.EstimatedPoints+f.EstimatedPoints > MaxPointsPerRequest && len(current.Fragments) > 0)

		if wouldExceed {
			if len(current.Fragments) > 0 {
				requests = append(requests, current)
			}
			current = Request{}
		}

		current.Fragments = append(current.Fragments, f)
		current.Breadth += f.Breadth
		current.EstimatedPoints += f.EstimatedPoints
	}

	if len(current.Fragments) > 0 {
		requests = append(requests, current)
	}

	return requests
}

// SortByPriority sorts fragments by types.TaskType priority (Metrics first, then Logs,
// then Discovery). Within the same type, order is preserved.
func SortByPriority(fragments []AliasFragment) {
	sort.SliceStable(fragments, func(i, j int) bool {
		return fragments[i].Item.TaskType < fragments[j].Item.TaskType
	})
}

// DispatchRequestResults delivers per-alias results from a RawQueryResponse
// back to the originating generators. Uses fragment aliases directly.
func DispatchRequestResults(
	ctx context.Context,
	req Request,
	resp *railway.RawQueryResponse,
	queryErr error,
	generatorMap map[string]types.TaskGenerator,
) {
	// Check for request-level errors (empty data with errors = total failure).
	if queryErr == nil && resp != nil && len(resp.Data) == 0 && len(resp.Errors) > 0 {
		msgs := make([]string, len(resp.Errors))
		for i, e := range resp.Errors {
			msgs[i] = e.Message
		}
		queryErr = fmt.Errorf("GraphQL request failed: %s", strings.Join(msgs, "; "))
	}

	// Check for breadth-exceeded errors (indicates a packer bug)
	if resp != nil {
		for _, gqlErr := range resp.Errors {
			if strings.Contains(strings.ToLower(gqlErr.Message), "breadth") {
				slog.Error("breadth limit exceeded (packer bug)",
					"message", gqlErr.Message,
					"computed_breadth", req.Breadth,
					"max_breadth", MaxBreadth,
					"alias_count", len(req.Fragments),
				)
			}
		}
	}

	for _, frag := range req.Fragments {
		gen, ok := generatorMap[frag.Item.ID]
		if !ok {
			continue
		}

		if queryErr != nil {
			gen.Deliver(ctx, frag.Item, nil, queryErr)
			continue
		}

		alias := frag.Alias

		data, exists := resp.Data[alias]
		if !exists {
			var aliasErr error
			for _, gqlErr := range resp.Errors {
				if len(gqlErr.Path) > 0 && gqlErr.Path[0] == alias {
					aliasErr = fmt.Errorf("GraphQL error: %s", gqlErr.Message)
					break
				}
			}
			if aliasErr != nil {
				gen.Deliver(ctx, frag.Item, nil, aliasErr)
			} else {
				gen.Deliver(ctx, frag.Item, nil, fmt.Errorf("alias %q not found in response", alias))
			}
			continue
		}

		if string(data) == "null" {
			var aliasErr error
			for _, gqlErr := range resp.Errors {
				if len(gqlErr.Path) > 0 && gqlErr.Path[0] == alias {
					aliasErr = fmt.Errorf("GraphQL error: %s", gqlErr.Message)
					break
				}
			}
			if aliasErr != nil {
				gen.Deliver(ctx, frag.Item, nil, aliasErr)
				continue
			}
		}

		gen.Deliver(ctx, frag.Item, json.RawMessage(data), nil)
	}
}
