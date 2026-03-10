package collector

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCoverageKey(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{"metric key", []string{"proj-1", "metric"}, "proj-1:metric"},
		{"env log key", []string{"env-1", "log", "environment"}, "env-1:log:environment"},
		{"build log key", []string{"dep-1", "log", "build"}, "dep-1:log:build"},
		{"single part", []string{"standalone"}, "standalone"},
		{"empty parts", []string{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CoverageKey(tt.parts...))
		})
	}
}
