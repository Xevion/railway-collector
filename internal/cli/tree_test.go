package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, ""},
		{-1 * time.Hour, ""},
		{45 * time.Minute, "45m"},
		{1 * time.Minute, "1m"},
		{2*time.Hour + 30*time.Minute, "2h 30m"},
		{5 * time.Hour, "5h"},
		{24 * time.Hour, "1d"},
		{25 * time.Hour, "1d 1h"},
		{76*time.Hour + 21*time.Hour, "4d 1h"},
		{88*24*time.Hour + 21*time.Hour, "88d 21h"},
		{100 * 24 * time.Hour, "100d"},
		{134*24*time.Hour + 12*time.Hour, "134d"},
	}
	for _, tt := range tests {
		t.Run(tt.input.String(), func(t *testing.T) {
			assert.Equal(t, tt.want, FormatDuration(tt.input))
		})
	}
}

func TestTreeBuilder_BasicTree(t *testing.T) {
	tb := NewTreeBuilder()
	tb.Add([]string{"banner", "metric"}, &NodeStats{Coverage: 99.6, GapCount: 1, LargestGap: 10 * time.Hour})
	tb.Add([]string{"banner", "banner", "build"}, &NodeStats{Coverage: 0.0, GapCount: 2})
	tb.Add([]string{"banner", "banner", "http"}, &NodeStats{Coverage: 0.2, GapCount: 2})
	tb.Add([]string{"banner", "banner", "env"}, &NodeStats{Coverage: 13.7, GapCount: 2})
	tb.Add([]string{"banner", "Postgres", "env"}, &NodeStats{Coverage: 23.7, GapCount: 2})
	tb.Add([]string{"banner", "backup", "build"}, &NodeStats{Coverage: 0.0, GapCount: 2})

	tree := tb.Build()
	require.Len(t, tree.Children, 1, "expected one project")

	banner := tree.Children[0]
	assert.Equal(t, "banner", banner.Label)
	require.NotNil(t, banner.Stats, "project should have aggregated stats")
}

func TestTreeBuilder_CollapseSingleChild(t *testing.T) {
	tb := NewTreeBuilder()
	tb.Add([]string{"proj", "Postgres", "env"}, &NodeStats{Coverage: 23.7, GapCount: 2})

	tree := tb.Build()
	require.Len(t, tree.Children, 1)

	// "proj" -> "Postgres" -> "env" all collapse into one node
	collapsed := tree.Children[0]
	assert.Contains(t, collapsed.Label, "\u00bb")
	assert.Contains(t, collapsed.Label, "proj")
	assert.Contains(t, collapsed.Label, "Postgres")
	assert.Contains(t, collapsed.Label, "env")
	assert.Equal(t, 23.7, collapsed.Stats.Coverage)
}

func TestTreeBuilder_CollapsePreservesMultiChild(t *testing.T) {
	tb := NewTreeBuilder()
	tb.Add([]string{"proj", "svc", "build"}, &NodeStats{Coverage: 10})
	tb.Add([]string{"proj", "svc", "http"}, &NodeStats{Coverage: 20})
	tb.Add([]string{"proj", "Postgres", "env"}, &NodeStats{Coverage: 30})

	tree := tb.Build()
	require.Len(t, tree.Children, 1)

	proj := tree.Children[0]
	assert.Equal(t, "proj", proj.Label)
	require.Len(t, proj.Children, 2) // "Postgres >> env" and "svc"

	// Postgres has only "env", so it collapses
	var pgNode *TreeNode
	for _, c := range proj.Children {
		if strings.Contains(c.Label, "Postgres") {
			pgNode = c
		}
	}
	require.NotNil(t, pgNode)
	assert.Contains(t, pgNode.Label, "\u00bb")
	assert.Equal(t, 30.0, pgNode.Stats.Coverage)
}

func TestTreeBuilder_AggregateStats(t *testing.T) {
	tb := NewTreeBuilder()
	tb.Add([]string{"proj", "a"}, &NodeStats{
		Coverage: 80.0, GapCount: 1, LargestGap: 5 * time.Hour,
		Collected: 80 * time.Hour, Total: 100 * time.Hour,
	})
	tb.Add([]string{"proj", "b"}, &NodeStats{
		Coverage: 40.0, GapCount: 3, LargestGap: 20 * time.Hour,
		Collected: 40 * time.Hour, Total: 100 * time.Hour,
	})

	tree := tb.Build()
	proj := tree.Children[0]

	require.NotNil(t, proj.Stats)
	assert.Equal(t, 4, proj.Stats.GapCount)
	assert.Equal(t, 20*time.Hour, proj.Stats.LargestGap)
	assert.InDelta(t, 60.0, proj.Stats.Coverage, 0.1) // 120/200 = 60%
}

func TestTreeBuilder_Sort(t *testing.T) {
	tb := NewTreeBuilder()
	tb.Add([]string{"zebra", "a"}, &NodeStats{Coverage: 1})
	tb.Add([]string{"zebra", "b"}, &NodeStats{Coverage: 1})
	tb.Add([]string{"alpha", "a"}, &NodeStats{Coverage: 2})
	tb.Add([]string{"alpha", "b"}, &NodeStats{Coverage: 2})
	tb.Add([]string{"middle", "a"}, &NodeStats{Coverage: 3})
	tb.Add([]string{"middle", "b"}, &NodeStats{Coverage: 3})

	tree := tb.Build()
	require.Len(t, tree.Children, 3)
	assert.Equal(t, "alpha", tree.Children[0].Label)
	assert.Equal(t, "middle", tree.Children[1].Label)
	assert.Equal(t, "zebra", tree.Children[2].Label)
}

func TestRenderTree_NoColor(t *testing.T) {
	// Render to a buffer (not a terminal), so colors are disabled.
	tb := NewTreeBuilder()
	tb.Add([]string{"proj", "metric"}, &NodeStats{
		Coverage: 99.6, GapCount: 1, LargestGap: 10 * time.Hour,
		Collected: 99 * time.Hour, Total: 100 * time.Hour,
	})
	tb.Add([]string{"proj", "svc", "env"}, &NodeStats{
		Coverage: 50.0, GapCount: 2, LargestGap: 48 * time.Hour,
		Collected: 50 * time.Hour, Total: 100 * time.Hour,
	})

	tree := tb.Build()
	var buf bytes.Buffer
	RenderTree(&buf, tree, "Test Report")

	out := buf.String()
	assert.Contains(t, out, "Test Report")
	assert.Contains(t, out, "PROJECT")
	assert.Contains(t, out, "COVERAGE")
	assert.Contains(t, out, "GAPS")
	assert.Contains(t, out, "MAX GAP")
	assert.Contains(t, out, "proj")
	assert.Contains(t, out, "metric")
	assert.Contains(t, out, "99.6%")
	assert.Contains(t, out, "50.0%")
	assert.Contains(t, out, "2d")
}
