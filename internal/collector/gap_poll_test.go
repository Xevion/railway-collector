package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/collector/coverage"
	"github.com/xevion/railway-collector/internal/collector/types"
)

// fakeTargetProvider is a minimal TargetProvider for internal tests.
type fakeTargetProvider struct {
	targets []types.ServiceTarget
}

func (f *fakeTargetProvider) Targets() []types.ServiceTarget        { return f.targets }
func (f *fakeTargetProvider) Refresh(_ context.Context) error { return nil }

// fakeStateStore is a minimal StateStore for internal tests.
type fakeStateStore struct {
	coverage map[string][]byte
}

func (f *fakeStateStore) GetCoverage(key string) ([]byte, error) {
	if f.coverage == nil {
		return nil, nil
	}
	return f.coverage[key], nil
}
func (f *fakeStateStore) SetCoverage(string, []byte) error               { return nil }
func (f *fakeStateStore) ListCoverage() (map[string][]byte, error)       { return nil, nil }
func (f *fakeStateStore) GetDiscoveryCache(string) ([]byte, error)       { return nil, nil }
func (f *fakeStateStore) SetDiscoveryCache(string, []byte) error         { return nil }
func (f *fakeStateStore) ListDiscoveryCache() (map[string][]byte, error) { return nil, nil }
func (f *fakeStateStore) DeleteDiscoveryCache(string) error              { return nil }
func (f *fakeStateStore) Close() error                                   { return nil }

// simpleBuildItems returns a buildItems callback that creates one types.WorkItem per
// chunk with a predictable ID format for assertions.
func simpleBuildItems(kind types.QueryKind) func(pollEntity, coverage.TimeRange, bool) []types.WorkItem {
	return func(e pollEntity, chunk coverage.TimeRange, isLiveEdge bool) []types.WorkItem {
		id := fmt.Sprintf("test:%s:%s", e.Key, chunk.Start.Format(time.RFC3339))
		if isLiveEdge {
			id += ":live"
		}
		return []types.WorkItem{{
			ID:       id,
			Kind:     kind,
			TaskType: types.TaskTypeMetrics,
			AliasKey: e.Key,
			Params: map[string]any{
				"startDate": chunk.Start.Format(time.RFC3339),
				"endDate":   chunk.End.Format(time.RFC3339),
			},
		}}
	}
}

func TestPollCoverageGaps_ReturnsNilBeforeNextPoll(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	nextPoll := now.Add(5 * time.Minute) // in the future

	items := pollCoverageGaps(now, gapPollParams{
		store:           &fakeStateStore{},
		discovery:       &fakeTargetProvider{targets: []types.ServiceTarget{{ProjectID: "p1"}}},
		logger:          slog.Default(),
		metricRetention: 1 * time.Hour,
		chunkSize:       6 * time.Hour,
		maxItemsPerPoll: 10,
		nextPoll:        nextPoll,
		entities: func(targets []types.ServiceTarget) []pollEntity {
			t.Fatal("entities should not be called when before nextPoll")
			return nil
		},
		buildItems:   simpleBuildItems(types.QueryMetrics),
		itemsPerEmit: 1,
		logPrefix:    "test",
	})

	assert.Nil(t, items)
}

func TestPollCoverageGaps_ReturnsNilWhenNoTargets(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	items := pollCoverageGaps(now, gapPollParams{
		store:           &fakeStateStore{},
		discovery:       &fakeTargetProvider{targets: nil},
		logger:          slog.Default(),
		metricRetention: 1 * time.Hour,
		chunkSize:       6 * time.Hour,
		maxItemsPerPoll: 10,
		nextPoll:        time.Time{},
		entities: func(targets []types.ServiceTarget) []pollEntity {
			return nil // no targets → no entities
		},
		buildItems:   simpleBuildItems(types.QueryMetrics),
		itemsPerEmit: 1,
		logPrefix:    "test",
	})

	assert.Nil(t, items)
}

func TestPollCoverageGaps_EmitsItemsForGaps(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	items := pollCoverageGaps(now, gapPollParams{
		store: &fakeStateStore{},
		discovery: &fakeTargetProvider{targets: []types.ServiceTarget{
			{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
		}},
		logger:          slog.Default(),
		metricRetention: 1 * time.Hour, // gap from 11:00 to 12:00
		chunkSize:       6 * time.Hour, // 1h gap < 6h chunk, so single item
		maxItemsPerPoll: 10,
		nextPoll:        time.Time{},
		entities: func(targets []types.ServiceTarget) []pollEntity {
			return []pollEntity{{
				Key:          "proj-1",
				CoverageType: coverage.CoverageTypeMetric,
				LogAttrs:     []any{"project_id", "proj-1"},
			}}
		},
		buildItems:   simpleBuildItems(types.QueryMetrics),
		itemsPerEmit: 1,
		logPrefix:    "metric",
	})

	require.NotEmpty(t, items)
	// The 1h gap ends at now → live edge → live item
	assert.True(t, strings.Contains(items[len(items)-1].ID, ":live"))
}

func TestPollCoverageGaps_RespectsBudget(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	maxItems := 2
	items := pollCoverageGaps(now, gapPollParams{
		store: &fakeStateStore{},
		discovery: &fakeTargetProvider{targets: []types.ServiceTarget{
			{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
			{ProjectID: "proj-2", ServiceID: "svc-2", EnvironmentID: "env-2"},
			{ProjectID: "proj-3", ServiceID: "svc-3", EnvironmentID: "env-3"},
		}},
		logger:          slog.Default(),
		metricRetention: 30 * time.Minute,
		chunkSize:       6 * time.Hour,
		maxItemsPerPoll: maxItems,
		nextPoll:        time.Time{},
		entities: func(targets []types.ServiceTarget) []pollEntity {
			var entities []pollEntity
			for _, tgt := range targets {
				entities = append(entities, pollEntity{
					Key:          tgt.ProjectID,
					CoverageType: coverage.CoverageTypeMetric,
					LogAttrs:     []any{"project_id", tgt.ProjectID},
				})
			}
			return entities
		},
		buildItems:   simpleBuildItems(types.QueryMetrics),
		itemsPerEmit: 1,
		logPrefix:    "metric",
	})

	assert.LessOrEqual(t, len(items), maxItems, "should respect maxItemsPerPoll budget")
}

func TestPollCoverageGaps_BudgetAccountsForItemsPerEmit(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	// buildItems returns 2 items per call (like HTTP generator)
	pairBuilder := func(e pollEntity, chunk coverage.TimeRange, isLiveEdge bool) []types.WorkItem {
		base := fmt.Sprintf("test:%s:%s", e.Key, chunk.Start.Format(time.RFC3339))
		return []types.WorkItem{
			{ID: base + ":a", Kind: types.QueryMetrics, AliasKey: e.Key},
			{ID: base + ":b", Kind: types.QueryMetrics, AliasKey: e.Key},
		}
	}

	items := pollCoverageGaps(now, gapPollParams{
		store: &fakeStateStore{},
		discovery: &fakeTargetProvider{targets: []types.ServiceTarget{
			{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
			{ProjectID: "proj-2", ServiceID: "svc-2", EnvironmentID: "env-2"},
		}},
		logger:          slog.Default(),
		metricRetention: 30 * time.Minute,
		chunkSize:       6 * time.Hour,
		maxItemsPerPoll: 3, // room for one pair (2) but not two (4)
		nextPoll:        time.Time{},
		entities: func(targets []types.ServiceTarget) []pollEntity {
			var entities []pollEntity
			for _, tgt := range targets {
				entities = append(entities, pollEntity{
					Key:          tgt.ProjectID,
					CoverageType: coverage.CoverageTypeMetric,
				})
			}
			return entities
		},
		buildItems:   pairBuilder,
		itemsPerEmit: 2,
		logPrefix:    "test",
	})

	// With budget 3 and itemsPerEmit 2, only one entity's pair fits
	assert.Equal(t, 2, len(items), "should emit exactly one pair (budget 3, cost 2 each)")
}

func TestPollCoverageGaps_LiveEdgeSplitsOversizedGap(t *testing.T) {
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

	items := pollCoverageGaps(now, gapPollParams{
		store: &fakeStateStore{},
		discovery: &fakeTargetProvider{targets: []types.ServiceTarget{
			{ProjectID: "proj-1", ServiceID: "svc-1", EnvironmentID: "env-1"},
		}},
		logger:          slog.Default(),
		metricRetention: 24 * time.Hour, // 24h gap, much larger than chunkSize
		chunkSize:       6 * time.Hour,
		maxItemsPerPoll: 50,
		nextPoll:        time.Time{},
		entities: func(targets []types.ServiceTarget) []pollEntity {
			return []pollEntity{{
				Key:          "proj-1",
				CoverageType: coverage.CoverageTypeMetric,
				LogAttrs:     []any{"project_id", "proj-1"},
			}}
		},
		buildItems:   simpleBuildItems(types.QueryMetrics),
		itemsPerEmit: 1,
		logPrefix:    "metric",
	})

	require.NotEmpty(t, items)

	// Should have chunked older items plus one live edge item
	liveCount := 0
	nonLiveCount := 0
	for _, item := range items {
		if strings.Contains(item.ID, ":live") {
			liveCount++
		} else {
			nonLiveCount++
		}
	}
	assert.Equal(t, 1, liveCount, "should have exactly one live-edge item")
	assert.Greater(t, nonLiveCount, 0, "should have chunked older items")
}
