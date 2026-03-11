package loader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/xevion/railway-collector/internal/collector/loader"
	"github.com/xevion/railway-collector/internal/collector/mocks"
	"github.com/xevion/railway-collector/internal/collector/types"
	"github.com/xevion/railway-collector/internal/railway"
)

// frag builds an AliasFragment with the given alias, breadth, and estimated points.
func frag(alias string, breadth, points int) loader.AliasFragment {
	return loader.AliasFragment{
		Alias:           alias,
		Field:           "metrics",
		Args:            fmt.Sprintf(`projectId: "%s"`, alias),
		Selection:       "{ cpu memory }",
		VarDecls:        []string{fmt.Sprintf("$start_%s: DateTime!", alias)},
		VarValues:       map[string]any{fmt.Sprintf("start_%s", alias): "2024-01-01"},
		Breadth:         breadth,
		EstimatedPoints: points,
		Item: types.WorkItem{
			ID:       alias,
			Kind:     types.QueryMetrics,
			TaskType: types.TaskTypeMetrics,
			AliasKey: alias,
		},
	}
}

// nFrags creates n fragments with sequential aliases and the given breadth/points.
func nFrags(n, breadth, points int) []loader.AliasFragment {
	frags := make([]loader.AliasFragment, n)
	for i := range n {
		frags[i] = frag(fmt.Sprintf("p%d", i), breadth, points)
	}
	return frags
}

func TestPack(t *testing.T) {
	tests := []struct {
		name        string
		fragments   []loader.AliasFragment
		wantReqs    int
		wantCounts  []int // fragment count per request
		wantBreadth []int // breadth per request (nil to skip check)
		wantPoints  []int // estimated points per request (nil to skip check)
	}{
		{
			name:      "nil input",
			fragments: nil,
			wantReqs:  0,
		},
		{
			name:      "empty input",
			fragments: []loader.AliasFragment{},
			wantReqs:  0,
		},
		{
			name:        "single fragment",
			fragments:   nFrags(1, 12, 100),
			wantReqs:    1,
			wantCounts:  []int{1},
			wantBreadth: []int{12},
			wantPoints:  []int{100},
		},
		{
			name:        "breadth exact fit (500/500)",
			fragments:   nFrags(5, 100, 10),
			wantReqs:    1,
			wantCounts:  []int{5},
			wantBreadth: []int{500},
		},
		{
			name:       "breadth overflow splits",
			fragments:  nFrags(6, 100, 10),
			wantReqs:   2,
			wantCounts: []int{5, 1},
		},
		{
			name:       "alias count exact fit (20)",
			fragments:  nFrags(20, 1, 1),
			wantReqs:   1,
			wantCounts: []int{20},
		},
		{
			name:       "alias count overflow splits",
			fragments:  nFrags(21, 1, 1),
			wantReqs:   2,
			wantCounts: []int{20, 1},
		},
		{
			name: "estimated points exact fit (30000)",
			fragments: []loader.AliasFragment{
				frag("a", 10, 15000),
				frag("b", 10, 15000),
			},
			wantReqs:   1,
			wantCounts: []int{2},
		},
		{
			name: "estimated points overflow splits",
			fragments: []loader.AliasFragment{
				frag("a", 10, 20000),
				frag("b", 10, 20000),
			},
			wantReqs:   2,
			wantCounts: []int{1, 1},
		},
		{
			name: "first fragment always fits even if over points limit",
			fragments: []loader.AliasFragment{
				frag("huge", 10, 50000),
			},
			wantReqs:   1,
			wantCounts: []int{1},
			wantPoints: []int{50000},
		},
		{
			name: "oversized breadth gets own request",
			fragments: []loader.AliasFragment{
				frag("big", loader.MaxBreadth+1, 100),
				frag("small", 10, 100),
			},
			wantReqs:    2,
			wantCounts:  []int{1, 1},
			wantBreadth: []int{loader.MaxBreadth + 1, 10},
		},
		{
			name: "oversized between normals - normals pack together",
			fragments: []loader.AliasFragment{
				frag("a", 10, 10),
				frag("big", loader.MaxBreadth+100, 10),
				frag("b", 10, 10),
			},
			wantReqs:    2,
			wantCounts:  []int{1, 2},
			wantBreadth: []int{loader.MaxBreadth + 100, 20},
		},
		{
			name: "accumulates breadth and points",
			fragments: []loader.AliasFragment{
				frag("a", 100, 5000),
				frag("b", 200, 8000),
			},
			wantReqs:    1,
			wantCounts:  []int{2},
			wantBreadth: []int{300},
			wantPoints:  []int{13000},
		},
		{
			name: "fragment at exactly MaxBreadth is packed normally",
			fragments: []loader.AliasFragment{
				frag("exact", loader.MaxBreadth, 100),
			},
			wantReqs:    1,
			wantCounts:  []int{1},
			wantBreadth: []int{loader.MaxBreadth},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqs := loader.Pack(tt.fragments)

			if tt.wantReqs == 0 {
				assert.Nil(t, reqs)
				return
			}

			require.Len(t, reqs, tt.wantReqs)
			for i, wantCount := range tt.wantCounts {
				assert.Len(t, reqs[i].Fragments, wantCount, "request %d fragment count", i)
			}
			for i, wantB := range tt.wantBreadth {
				assert.Equal(t, wantB, reqs[i].Breadth, "request %d breadth", i)
			}
			for i, wantP := range tt.wantPoints {
				assert.Equal(t, wantP, reqs[i].EstimatedPoints, "request %d estimated points", i)
			}
		})
	}
}

func TestAssembleQuery(t *testing.T) {
	t.Run("single fragment", func(t *testing.T) {
		req := &loader.Request{Fragments: []loader.AliasFragment{frag("p_abc", 12, 100)}}
		query, vars := req.AssembleQuery()

		assert.Contains(t, query, "query Batch(")
		assert.Contains(t, query, "$start_p_abc: DateTime!")
		assert.Contains(t, query, `p_abc: metrics(projectId: "p_abc")`)
		assert.Contains(t, query, "{ cpu memory }")
		assert.Equal(t, "2024-01-01", vars["start_p_abc"])
	})

	t.Run("multiple fragments merge vars", func(t *testing.T) {
		req := &loader.Request{Fragments: []loader.AliasFragment{frag("a", 12, 100), frag("b", 12, 100)}}
		query, vars := req.AssembleQuery()

		assert.Contains(t, query, "$start_a: DateTime!")
		assert.Contains(t, query, "$start_b: DateTime!")
		assert.Len(t, vars, 2)
	})

	t.Run("empty request", func(t *testing.T) {
		req := &loader.Request{}
		query, vars := req.AssembleQuery()
		assert.Contains(t, query, "query Batch()")
		assert.Empty(t, vars)
	})
}

func TestItems(t *testing.T) {
	req := &loader.Request{Fragments: []loader.AliasFragment{frag("a", 10, 10), frag("b", 10, 10)}}
	items := req.Items()
	require.Len(t, items, 2)
	assert.Equal(t, "a", items[0].ID)
	assert.Equal(t, "b", items[1].ID)
}

func TestSortByPriority(t *testing.T) {
	frags := []loader.AliasFragment{
		{Item: types.WorkItem{ID: "log1", TaskType: types.TaskTypeLogs}},
		{Item: types.WorkItem{ID: "disc1", TaskType: types.TaskTypeDiscovery}},
		{Item: types.WorkItem{ID: "met1", TaskType: types.TaskTypeMetrics}},
		{Item: types.WorkItem{ID: "use1", TaskType: types.TaskTypeUsage}},
		{Item: types.WorkItem{ID: "met2", TaskType: types.TaskTypeMetrics}},
	}

	loader.SortByPriority(frags)

	// Metrics (0) < Logs (1) < Discovery (2) < Usage (3)
	assert.Equal(t, types.TaskTypeMetrics, frags[0].Item.TaskType)
	assert.Equal(t, types.TaskTypeMetrics, frags[1].Item.TaskType)
	assert.Equal(t, types.TaskTypeLogs, frags[2].Item.TaskType)
	assert.Equal(t, types.TaskTypeDiscovery, frags[3].Item.TaskType)
	assert.Equal(t, types.TaskTypeUsage, frags[4].Item.TaskType)

	// Stable sort preserves relative order within same type.
	assert.Equal(t, "met1", frags[0].Item.ID)
	assert.Equal(t, "met2", frags[1].Item.ID)
}

func TestDispatchRequestResults(t *testing.T) {
	tests := []struct {
		name     string
		req      loader.Request
		resp     *railway.RawQueryResponse
		queryErr error
		check    func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator)
	}{
		{
			name: "happy path delivers data",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{"a1": json.RawMessage(`{"cpu":42}`)},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`{"cpu":42}`), nil)
				genMap["a1"] = gen
			},
		},
		{
			name: "query error propagated to all fragments",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			queryErr: fmt.Errorf("network timeout"),
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, data json.RawMessage, err error) {
						assert.Nil(t, data)
						assert.ErrorContains(t, err, "network timeout")
					})
				genMap["a1"] = gen
			},
		},
		{
			name: "empty data with errors becomes request-level failure",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data:   map[string]json.RawMessage{},
				Errors: []railway.GraphQLError{{Message: "Unauthorized"}, {Message: "Rate limited"}},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, _ json.RawMessage, err error) {
						assert.ErrorContains(t, err, "Unauthorized")
						assert.ErrorContains(t, err, "Rate limited")
					})
				genMap["a1"] = gen
			},
		},
		{
			name: "missing alias with matching error",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data:   map[string]json.RawMessage{"other": json.RawMessage(`{}`)},
				Errors: []railway.GraphQLError{{Message: "field not found", Path: []string{"a1"}}},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, _ json.RawMessage, err error) {
						assert.ErrorContains(t, err, "field not found")
					})
				genMap["a1"] = gen
			},
		},
		{
			name: "missing alias no matching error falls through to not-found",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data:   map[string]json.RawMessage{"other": json.RawMessage(`{}`)},
				Errors: []railway.GraphQLError{{Message: "unrelated", Path: []string{"other"}}},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, _ json.RawMessage, err error) {
						assert.ErrorContains(t, err, `not found in response`)
					})
				genMap["a1"] = gen
			},
		},
		{
			name: "missing alias error with empty path doesn't match",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data:   map[string]json.RawMessage{"other": json.RawMessage(`{}`)},
				Errors: []railway.GraphQLError{{Message: "vague error", Path: []string{}}},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, _ json.RawMessage, err error) {
						assert.ErrorContains(t, err, "not found in response")
					})
				genMap["a1"] = gen
			},
		},
		{
			name: "null data with matching error",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data:   map[string]json.RawMessage{"a1": json.RawMessage(`null`)},
				Errors: []railway.GraphQLError{{Message: "project gone", Path: []string{"a1"}}},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, data json.RawMessage, err error) {
						assert.Nil(t, data)
						assert.ErrorContains(t, err, "project gone")
					})
				genMap["a1"] = gen
			},
		},
		{
			name: "null data without error delivers as-is",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{"a1": json.RawMessage(`null`)},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`null`), nil)
				genMap["a1"] = gen
			},
		},
		{
			name: "unknown generator skipped",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{"a1": json.RawMessage(`{}`)},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				// Don't register "a1" - it should be silently skipped.
			},
		},
		{
			name: "breadth error logged without affecting delivery",
			req: loader.Request{
				Fragments: []loader.AliasFragment{
					{Alias: "a1", Item: types.WorkItem{ID: "gen1"}},
				},
				Breadth: 600,
			},
			resp: &railway.RawQueryResponse{
				Data:   map[string]json.RawMessage{"a1": json.RawMessage(`{}`)},
				Errors: []railway.GraphQLError{{Message: "Query breadth limit exceeded"}},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`{}`), nil)
				genMap["gen1"] = gen
			},
		},
		{
			name: "nil response with query error",
			req: loader.Request{Fragments: []loader.AliasFragment{
				frag("a1", 0, 0),
			}},
			queryErr: fmt.Errorf("connection refused"),
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, _ json.RawMessage, err error) {
						assert.ErrorContains(t, err, "connection refused")
					})
				genMap["a1"] = gen
			},
		},
		{
			name: "null data error path uses first match only",
			req: loader.Request{
				Fragments: []loader.AliasFragment{
					{Alias: "a1", Item: types.WorkItem{ID: "gen1"}},
				},
			},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{"a1": json.RawMessage(`null`)},
				Errors: []railway.GraphQLError{
					{Message: "first error", Path: []string{"a1"}},
					{Message: "second error", Path: []string{"a1"}},
				},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any()).
					Do(func(_ context.Context, _ types.WorkItem, _ json.RawMessage, err error) {
						assert.ErrorContains(t, err, "first error")
						assert.NotContains(t, err.Error(), "second error")
					})
				genMap["gen1"] = gen
			},
		},
		{
			// Ensures an unknown generator is skipped without aborting the loop;
			// the next fragment's generator must still receive its delivery.
			name: "skipped generator does not stop others",
			req: loader.Request{Fragments: []loader.AliasFragment{
				{Alias: "a1", Item: types.WorkItem{ID: "unknown"}},
				{Alias: "a2", Item: types.WorkItem{ID: "known"}},
			}},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{
					"a1": json.RawMessage(`{}`),
					"a2": json.RawMessage(`{"ok":true}`),
				},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen := mocks.NewMockTaskGenerator(ctrl)
				gen.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`{"ok":true}`), nil)
				genMap["known"] = gen
			},
		},
		{
			// Ensures query errors are delivered to every fragment, not just the first.
			name: "query error continues to all fragments",
			req: loader.Request{Fragments: []loader.AliasFragment{
				{Alias: "a1", Item: types.WorkItem{ID: "g1"}},
				{Alias: "a2", Item: types.WorkItem{ID: "g2"}},
			}},
			queryErr: fmt.Errorf("fail"),
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen1 := mocks.NewMockTaskGenerator(ctrl)
				gen2 := mocks.NewMockTaskGenerator(ctrl)
				gen1.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any())
				gen2.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any())
				genMap["g1"] = gen1
				genMap["g2"] = gen2
			},
		},
		{
			// Ensures a missing alias error doesn't abort the loop; subsequent
			// fragments with present data must still be delivered successfully.
			name: "missing alias continues to next",
			req: loader.Request{Fragments: []loader.AliasFragment{
				{Alias: "missing", Item: types.WorkItem{ID: "g1"}},
				{Alias: "present", Item: types.WorkItem{ID: "g2"}},
			}},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{
					"present": json.RawMessage(`{"ok":true}`),
				},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen1 := mocks.NewMockTaskGenerator(ctrl)
				gen2 := mocks.NewMockTaskGenerator(ctrl)
				gen1.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any())
				gen2.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`{"ok":true}`), nil)
				genMap["g1"] = gen1
				genMap["g2"] = gen2
			},
		},
		{
			// Ensures null data with a matching error doesn't abort the loop;
			// the next fragment must still be delivered.
			name: "null data continues to next",
			req: loader.Request{Fragments: []loader.AliasFragment{
				{Alias: "nulled", Item: types.WorkItem{ID: "g1"}},
				{Alias: "ok", Item: types.WorkItem{ID: "g2"}},
			}},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{
					"nulled": json.RawMessage(`null`),
					"ok":     json.RawMessage(`{"ok":true}`),
				},
				Errors: []railway.GraphQLError{{Message: "gone", Path: []string{"nulled"}}},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen1 := mocks.NewMockTaskGenerator(ctrl)
				gen2 := mocks.NewMockTaskGenerator(ctrl)
				gen1.EXPECT().Deliver(gomock.Any(), gomock.Any(), nil, gomock.Any())
				gen2.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`{"ok":true}`), nil)
				genMap["g1"] = gen1
				genMap["g2"] = gen2
			},
		},
		{
			name: "multiple fragments with different generators",
			req: loader.Request{Fragments: []loader.AliasFragment{
				{Alias: "a1", Item: types.WorkItem{ID: "g1"}},
				{Alias: "a2", Item: types.WorkItem{ID: "g2"}},
			}},
			resp: &railway.RawQueryResponse{
				Data: map[string]json.RawMessage{
					"a1": json.RawMessage(`{"val":1}`),
					"a2": json.RawMessage(`{"val":2}`),
				},
			},
			check: func(t *testing.T, ctrl *gomock.Controller, genMap map[string]types.TaskGenerator) {
				gen1 := mocks.NewMockTaskGenerator(ctrl)
				gen2 := mocks.NewMockTaskGenerator(ctrl)
				gen1.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`{"val":1}`), nil)
				gen2.EXPECT().Deliver(gomock.Any(), gomock.Any(), json.RawMessage(`{"val":2}`), nil)
				genMap["g1"] = gen1
				genMap["g2"] = gen2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			genMap := make(map[string]types.TaskGenerator)
			tt.check(t, ctrl, genMap)

			loader.DispatchRequestResults(context.Background(), tt.req, tt.resp, tt.queryErr, genMap)
		})
	}
}
