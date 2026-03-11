package cli

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mattn/go-runewidth"
)

// ANSI color codes
const (
	ansiReset     = "\033[0m"
	ansiBold      = "\033[1m"
	ansiDim       = "\033[2m"
	ansiRed       = "\033[31m"
	ansiYellow    = "\033[33m"
	ansiGreen     = "\033[32m"
	ansiDimYellow = "\033[2;33m"
)

// NodeStats holds statistics for a tree node.
type NodeStats struct {
	Coverage   float64
	GapCount   int
	LargestGap time.Duration
	Collected  time.Duration // used for parent aggregation
	Total      time.Duration // used for parent aggregation
}

// TreeNode represents a node in the display tree.
type TreeNode struct {
	Label    string
	Stats    *NodeStats
	Children []*TreeNode
}

// TreeBuilder constructs a tree from path segments.
type TreeBuilder struct {
	root *TreeNode
}

// NewTreeBuilder creates a new tree builder.
func NewTreeBuilder() *TreeBuilder {
	return &TreeBuilder{root: &TreeNode{}}
}

// Add inserts a node at the given path, creating intermediate nodes as needed.
// The final segment receives the given stats.
func (tb *TreeBuilder) Add(path []string, stats *NodeStats) {
	node := tb.root
	for i, seg := range path {
		var found *TreeNode
		for _, c := range node.Children {
			if c.Label == seg {
				found = c
				break
			}
		}
		if found == nil {
			found = &TreeNode{Label: seg}
			node.Children = append(node.Children, found)
		}
		if i == len(path)-1 {
			found.Stats = stats
		}
		node = found
	}
}

// Build collapses single-child nodes, aggregates parent stats, and sorts.
func (tb *TreeBuilder) Build() *TreeNode {
	collapseChildren(tb.root)
	aggregateStats(tb.root)
	sortChildren(tb.root)
	return tb.root
}

// collapseChildren merges a node with its only child when the node has no stats
// of its own (e.g. "Postgres" with only child "env" becomes "Postgres >> env").
func collapseChildren(node *TreeNode) {
	for _, c := range node.Children {
		collapseChildren(c)
	}
	// Only collapse non-root nodes (Label != "")
	for len(node.Children) == 1 && node.Stats == nil && node.Label != "" {
		child := node.Children[0]
		node.Label += " \u00bb " + child.Label
		node.Stats = child.Stats
		node.Children = child.Children
	}
}

// aggregateStats computes parent stats from children, bottom-up.
func aggregateStats(node *TreeNode) {
	for _, c := range node.Children {
		aggregateStats(c)
	}
	if node.Stats != nil || len(node.Children) == 0 {
		return
	}
	agg := &NodeStats{}
	for _, c := range node.Children {
		if c.Stats == nil {
			continue
		}
		agg.Collected += c.Stats.Collected
		agg.Total += c.Stats.Total
		agg.GapCount += c.Stats.GapCount
		if c.Stats.LargestGap > agg.LargestGap {
			agg.LargestGap = c.Stats.LargestGap
		}
	}
	if agg.Total > 0 {
		agg.Coverage = float64(agg.Collected) / float64(agg.Total) * 100
	}
	node.Stats = agg
}

func sortChildren(node *TreeNode) {
	sort.Slice(node.Children, func(i, j int) bool {
		return node.Children[i].Label < node.Children[j].Label
	})
	for _, c := range node.Children {
		sortChildren(c)
	}
}

// FormatDuration formats a duration in human-friendly days/hours/minutes.
func FormatDuration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	totalMinutes := int(d.Minutes())
	totalHours := totalMinutes / 60
	minutes := totalMinutes % 60
	days := totalHours / 24
	hours := totalHours % 24

	switch {
	case totalHours == 0:
		return fmt.Sprintf("%dm", minutes)
	case days == 0:
		if minutes == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		return fmt.Sprintf("%dh %dm", hours, minutes)
	case days >= 100:
		return fmt.Sprintf("%dd", days)
	default:
		if hours == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd %dh", days, hours)
	}
}

// treeLine holds pre-computed rendering data for a single output line.
type treeLine struct {
	prefix    string
	label     string
	coverage  string
	gaps      string
	maxGap    string
	pct       float64
	isProject bool
	hasStats  bool
}

// RenderTree writes the tree to w with optional ANSI colors.
func RenderTree(w io.Writer, root *TreeNode, title string) {
	lines := buildLines(root)
	if len(lines) == 0 {
		return
	}

	color := detectColor(w)

	// Calculate column widths
	maxName := len("PROJECT")
	maxCov := len("COVERAGE")
	maxGaps := len("GAPS")
	maxDur := len("MAX GAP")

	for _, l := range lines {
		nw := runewidth.StringWidth(l.prefix) + runewidth.StringWidth(l.label)
		if nw > maxName {
			maxName = nw
		}
		if len(l.coverage) > maxCov {
			maxCov = len(l.coverage)
		}
		if len(l.gaps) > maxGaps {
			maxGaps = len(l.gaps)
		}
		if len(l.maxGap) > maxDur {
			maxDur = len(l.maxGap)
		}
	}
	maxName++ // right padding

	totalWidth := maxName + 2 + maxCov + 3 + maxGaps + 3 + maxDur

	// Title
	if title != "" {
		if color {
			fmt.Fprintf(w, "%s%s%s\n\n", ansiBold, title, ansiReset)
		} else {
			fmt.Fprintln(w, title)
			fmt.Fprintln(w)
		}
	}

	// Header
	hdr := fmt.Sprintf(" %-*s  %*s   %*s   %*s",
		maxName-1, "PROJECT", maxCov, "COVERAGE", maxGaps, "GAPS", maxDur, "MAX GAP")
	if color {
		fmt.Fprintf(w, "%s%s%s\n", ansiDim, hdr, ansiReset)
		fmt.Fprintf(w, "%s %s%s\n", ansiDim, strings.Repeat("\u2500", totalWidth), ansiReset)
	} else {
		fmt.Fprintln(w, hdr)
		fmt.Fprintf(w, " %s\n", strings.Repeat("-", totalWidth))
	}

	// Data lines
	for i, l := range lines {
		if l.isProject && i > 0 {
			fmt.Fprintln(w)
		}

		nameWidth := runewidth.StringWidth(l.prefix) + runewidth.StringWidth(l.label)
		pad := maxName - nameWidth

		// Tree prefix (dim)
		if l.prefix != "" {
			if color {
				fmt.Fprintf(w, "%s%s%s", ansiDim, l.prefix, ansiReset)
			} else {
				fmt.Fprint(w, l.prefix)
			}
		}

		// Label (bold for project-level)
		if l.isProject && color {
			fmt.Fprintf(w, "%s%s%s", ansiBold, l.label, ansiReset)
		} else {
			fmt.Fprint(w, l.label)
		}

		fmt.Fprint(w, strings.Repeat(" ", pad))

		if !l.hasStats {
			covPad := maxCov - 1
			if color {
				fmt.Fprintf(w, "  %*s%s\u2014%s", covPad, "", ansiDim, ansiReset)
			} else {
				fmt.Fprintf(w, "  %*s-", covPad, "")
			}
		} else {
			// Coverage percentage
			if color {
				fmt.Fprintf(w, "  %s%*s%s", coverageColor(l.pct), maxCov, l.coverage, ansiReset)
			} else {
				fmt.Fprintf(w, "  %*s", maxCov, l.coverage)
			}

			// Gap count
			fmt.Fprintf(w, "   %*s", maxGaps, l.gaps)

			// Largest gap duration
			if l.maxGap != "" {
				if color {
					fmt.Fprintf(w, "   %s%*s%s", ansiDimYellow, maxDur, l.maxGap, ansiReset)
				} else {
					fmt.Fprintf(w, "   %*s", maxDur, l.maxGap)
				}
			}
		}

		fmt.Fprintln(w)
	}
}

func buildLines(root *TreeNode) []treeLine {
	var lines []treeLine
	for _, project := range root.Children {
		lines = append(lines, formatNode(project, " ", true))
		collectChildren(project, " ", &lines)
	}
	return lines
}

func collectChildren(node *TreeNode, parentPrefix string, lines *[]treeLine) {
	for i, child := range node.Children {
		isLast := i == len(node.Children)-1
		var connector, continuation string
		if isLast {
			connector = "\u2514 "
			continuation = "  "
		} else {
			connector = "\u251c "
			continuation = "\u2502 "
		}
		prefix := parentPrefix + connector
		*lines = append(*lines, formatNode(child, prefix, false))
		collectChildren(child, parentPrefix+continuation, lines)
	}
}

func formatNode(node *TreeNode, prefix string, isProject bool) treeLine {
	l := treeLine{
		prefix:    prefix,
		label:     node.Label,
		isProject: isProject,
		hasStats:  node.Stats != nil,
	}
	if node.Stats != nil {
		l.pct = node.Stats.Coverage
		l.coverage = fmt.Sprintf("%.1f%%", node.Stats.Coverage)
		if node.Stats.GapCount > 0 {
			word := "gaps"
			if node.Stats.GapCount == 1 {
				word = "gap"
			}
			l.gaps = fmt.Sprintf("%d %s", node.Stats.GapCount, word)
		}
		if node.Stats.LargestGap > 0 {
			l.maxGap = FormatDuration(node.Stats.LargestGap)
		}
	}
	return l
}

func coverageColor(pct float64) string {
	switch {
	case pct >= 75:
		return ansiGreen
	case pct >= 25:
		return ansiYellow
	default:
		return ansiRed
	}
}

func detectColor(w io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}
