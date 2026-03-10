package collector_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xevion/railway-collector/internal/collector"
	"github.com/xevion/railway-collector/internal/collector/mocks"
	"go.uber.org/mock/gomock"
)

func TestDiscoveryGenerator_Type(t *testing.T) {
	gen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Logger: slog.Default(),
	})
	assert.Equal(t, collector.TaskTypeDiscovery, gen.Type())
}

func TestDiscoveryGenerator_Poll_EmitsOneItem(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)

	gen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: targets,
		Interval:  60 * time.Second,
		Logger:    slog.Default(),
	})

	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)
	items := gen.Poll(now)
	require.Len(t, items, 1)

	assert.Equal(t, "discovery", items[0].ID)
	assert.Equal(t, collector.QueryDiscovery, items[0].Kind)
	assert.Equal(t, collector.TaskTypeDiscovery, items[0].TaskType)
}

func TestDiscoveryGenerator_Poll_RespectsInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)

	gen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: targets,
		Interval:  60 * time.Second,
		Logger:    slog.Default(),
	})

	now := time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC)

	items := gen.Poll(now)
	require.Len(t, items, 1)

	// Immediate poll returns nil
	items = gen.Poll(now.Add(1 * time.Second))
	assert.Nil(t, items)

	// After interval, returns item
	items = gen.Poll(now.Add(60 * time.Second))
	require.Len(t, items, 1)
}

func TestDiscoveryGenerator_Deliver_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)

	targets.EXPECT().Targets().Return([]collector.ServiceTarget{
		{ProjectID: "proj-1"},
		{ProjectID: "proj-2"},
	})

	gen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: targets,
		Interval:  60 * time.Second,
		Logger:    slog.Default(),
	})

	item := collector.WorkItem{ID: "discovery", Kind: collector.QueryDiscovery}

	// Should not panic, should log target count
	gen.Deliver(context.Background(), item, nil, nil)
}

func TestDiscoveryGenerator_Deliver_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)

	gen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: targets,
		Interval:  60 * time.Second,
		Logger:    slog.Default(),
	})

	item := collector.WorkItem{ID: "discovery", Kind: collector.QueryDiscovery}

	// Should not panic
	gen.Deliver(context.Background(), item, nil, assert.AnError)
}

func TestDiscoveryGenerator_Refresh(t *testing.T) {
	ctrl := gomock.NewController(t)
	targets := mocks.NewMockTargetProvider(ctrl)

	targets.EXPECT().Refresh(gomock.Any()).Return(nil)

	gen := collector.NewDiscoveryGenerator(collector.DiscoveryGeneratorConfig{
		Discovery: targets,
		Interval:  60 * time.Second,
		Logger:    slog.Default(),
	})

	err := gen.Refresh(context.Background())
	require.NoError(t, err)
}
