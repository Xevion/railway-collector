package collector

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xevion/railway-collector/internal/config"
)

var testDiscardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// newTestDiscovery constructs a minimal Discovery for unit tests that don't
// need a live API client or persistent store.
func newTestDiscovery(filters config.FiltersConfig) *Discovery {
	return NewDiscovery(DiscoveryConfig{
		Filters: filters,
		Logger:  testDiscardLogger,
	})
}

// ---- jitteredTTL ----

// Test_jitteredTTL_NoJitter verifies that zero jitter returns the exact base duration.
func Test_jitteredTTL_NoJitter(t *testing.T) {
	base := 30 * time.Minute
	got := jitteredTTL(base, 0)
	assert.Equal(t, base, got)
}

// Test_jitteredTTL_WithJitter verifies that the returned duration stays within
// [base-jitter, base+jitter] across 100 iterations.
func Test_jitteredTTL_WithJitter(t *testing.T) {
	base := 10 * time.Minute
	jitter := 2 * time.Minute
	lo := base - jitter
	hi := base + jitter

	for i := 0; i < 100; i++ {
		got := jitteredTTL(base, jitter)
		// Also account for the 1-minute minimum floor.
		if got < time.Minute {
			t.Fatalf("iteration %d: got %v which is below the 1-minute minimum", i, got)
		}
		assert.GreaterOrEqualf(t, got, lo, "iteration %d: %v < lower bound %v", i, got, lo)
		assert.LessOrEqualf(t, got, hi, "iteration %d: %v > upper bound %v", i, got, hi)
	}
}

// Test_jitteredTTL_MinimumOneMinute verifies that even when base-jitter would
// produce a sub-minute value, the result is clamped to at least 1 minute.
func Test_jitteredTTL_MinimumOneMinute(t *testing.T) {
	base := 30 * time.Second
	jitter := 30 * time.Second

	for i := 0; i < 100; i++ {
		got := jitteredTTL(base, jitter)
		assert.GreaterOrEqualf(t, got, time.Minute, "iteration %d: got %v which is below 1 minute minimum", i, got)
	}
}

// Test_jitteredTTL_NegativeJitter verifies that a negative jitter value is
// treated as zero and the exact base is returned.
func Test_jitteredTTL_NegativeJitter(t *testing.T) {
	base := 5 * time.Minute
	got := jitteredTTL(base, -10*time.Second)
	assert.Equal(t, base, got)
}

// ---- matchFilter ----

// Test_matchFilter consolidates all matchFilter cases into a single table-driven test.
// Each row exercises a distinct matching scenario; the filter list is compiled from
// patterns and ids fields combined, and the result is compared against want.
func Test_matchFilter(t *testing.T) {
	d := newTestDiscovery(config.FiltersConfig{})

	tests := []struct {
		name       string
		patterns   []string
		targetName string
		targetID   string
		want       bool
	}{
		{
			name:       "EmptyFilters/nil",
			patterns:   nil,
			targetName: "any-name",
			targetID:   "any-id",
			want:       true,
		},
		{
			name:       "EmptyFilters/empty slice",
			patterns:   []string{},
			targetName: "any-name",
			targetID:   "any-id",
			want:       true,
		},
		{
			name:       "ExactNameMatch/matching name",
			patterns:   []string{"web-frontend"},
			targetName: "web-frontend",
			targetID:   "svc-123",
			want:       true,
		},
		{
			name:       "ExactNameMatch/non-matching name",
			patterns:   []string{"web-frontend"},
			targetName: "api-gateway",
			targetID:   "svc-456",
			want:       false,
		},
		{
			name:       "ExactIDMatch/matching id",
			patterns:   []string{"svc-abc"},
			targetName: "some-service",
			targetID:   "svc-abc",
			want:       true,
		},
		{
			name:       "ExactIDMatch/non-matching id",
			patterns:   []string{"svc-abc"},
			targetName: "some-service",
			targetID:   "svc-xyz",
			want:       false,
		},
		{
			name:       "RegexMatch/matching prefix",
			patterns:   []string{"^web-.*"},
			targetName: "web-frontend",
			targetID:   "svc-1",
			want:       true,
		},
		{
			name:       "RegexMatch/matching prefix variant",
			patterns:   []string{"^web-.*"},
			targetName: "web-backend",
			targetID:   "svc-2",
			want:       true,
		},
		{
			name:       "RegexMatch/no match",
			patterns:   []string{"^web-.*"},
			targetName: "api-gateway",
			targetID:   "svc-3",
			want:       false,
		},
		{
			name:       "InvalidRegexFallsBackToLiteral/matching literal",
			patterns:   []string{"[invalid"},
			targetName: "[invalid",
			targetID:   "svc-1",
			want:       true,
		},
		{
			name:       "InvalidRegexFallsBackToLiteral/non-matching literal",
			patterns:   []string{"[invalid"},
			targetName: "something-else",
			targetID:   "svc-2",
			want:       false,
		},
		{
			name:       "MultipleFilters/first matches",
			patterns:   []string{"web-frontend", "api-gateway"},
			targetName: "web-frontend",
			targetID:   "svc-1",
			want:       true,
		},
		{
			name:       "MultipleFilters/second matches",
			patterns:   []string{"web-frontend", "api-gateway"},
			targetName: "api-gateway",
			targetID:   "svc-2",
			want:       true,
		},
		{
			name:       "MultipleFilters/no match",
			patterns:   []string{"web-frontend", "api-gateway"},
			targetName: "worker",
			targetID:   "svc-3",
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filters []compiledFilter
			if tt.patterns != nil {
				filters = compileFilters(tt.patterns, testDiscardLogger)
			}
			got := d.matchFilter(tt.targetName, tt.targetID, filters)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Test_matchFilter_InvalidRegex verifies that an invalid regex pattern produces
// a nil re field and is still recorded as a compiledFilter with the raw pattern.
func Test_matchFilter_InvalidRegex(t *testing.T) {
	// "[invalid" is not a valid regex; compileFilters sets re = nil, raw = "[invalid"
	filters := compileFilters([]string{"[invalid"}, testDiscardLogger)
	require.Len(t, filters, 1)
	assert.Nil(t, filters[0].re, "invalid regex should produce nil re")
	assert.Equal(t, "[invalid", filters[0].raw)
}

// ---- compileFilters ----

// Test_compileFilters_Empty verifies that a nil patterns slice produces an
// empty (zero-length) slice with no entries.
func Test_compileFilters_Empty(t *testing.T) {
	result := compileFilters(nil, testDiscardLogger)
	assert.Empty(t, result)
}

// Test_compileFilters_ValidRegex verifies that two valid patterns both produce
// non-nil compiled regexes, and that the raw field is preserved.
func Test_compileFilters_ValidRegex(t *testing.T) {
	patterns := []string{"^web-.*", "api-.*"}
	result := compileFilters(patterns, testDiscardLogger)

	require.Len(t, result, 2)

	assert.Equal(t, "^web-.*", result[0].raw)
	assert.NotNil(t, result[0].re, "valid regex should produce non-nil re")

	assert.Equal(t, "api-.*", result[1].raw)
	assert.NotNil(t, result[1].re, "valid regex should produce non-nil re")
}
