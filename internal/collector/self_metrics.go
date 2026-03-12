package collector

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xevion/railway-collector/internal/sink"
)

// CollectorMetrics tracks operational self-metrics for the collector.
// All methods are safe for concurrent use and nil-safe (nil receiver is a no-op).
type CollectorMetrics struct {
	mu            sync.Mutex
	counters      map[string]uint64
	floatCounters map[string]float64
	gauges        map[string]float64
	meta          map[string]selfMetricMeta
}

type selfMetricMeta struct {
	name   string
	labels map[string]string
	kind   sink.MetricKind
}

// NewCollectorMetrics creates a new CollectorMetrics instance.
func NewCollectorMetrics() *CollectorMetrics {
	return &CollectorMetrics{
		counters:      make(map[string]uint64),
		floatCounters: make(map[string]float64),
		gauges:        make(map[string]float64),
		meta:          make(map[string]selfMetricMeta),
	}
}

// selfMetricKey builds a stable composite key from name and sorted label pairs.
func selfMetricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%s", k, labels[k])
	}
	return name + "|" + strings.Join(parts, ",")
}

// IncrCounter atomically increments a named counter with labels by 1.
// Safe to call on a nil *CollectorMetrics.
func (m *CollectorMetrics) IncrCounter(name string, labels map[string]string) {
	if m == nil {
		return
	}
	key := selfMetricKey(name, labels)
	m.mu.Lock()
	m.counters[key]++
	if _, ok := m.meta[key]; !ok {
		labelsCopy := make(map[string]string, len(labels))
		for k, v := range labels {
			labelsCopy[k] = v
		}
		m.meta[key] = selfMetricMeta{name: name, labels: labelsCopy, kind: sink.MetricKindCounter}
	}
	m.mu.Unlock()
}

// InitCounter registers a named counter with labels at 0 if it does not already exist.
// Use at startup to ensure counters appear in Snapshot even before the first event.
// Safe to call on a nil *CollectorMetrics.
func (m *CollectorMetrics) InitCounter(name string, labels map[string]string) {
	if m == nil {
		return
	}
	key := selfMetricKey(name, labels)
	m.mu.Lock()
	if _, ok := m.counters[key]; !ok {
		labelsCopy := make(map[string]string, len(labels))
		for k, v := range labels {
			labelsCopy[k] = v
		}
		m.counters[key] = 0
		m.meta[key] = selfMetricMeta{name: name, labels: labelsCopy, kind: sink.MetricKindCounter}
	}
	m.mu.Unlock()
}

// AddCounter adds delta to a named float counter with labels.
// Use for values that accumulate as float64 (e.g. durations in seconds).
// Safe to call on a nil *CollectorMetrics.
func (m *CollectorMetrics) AddCounter(name string, labels map[string]string, delta float64) {
	if m == nil {
		return
	}
	key := selfMetricKey(name, labels)
	m.mu.Lock()
	m.floatCounters[key] += delta
	if _, ok := m.meta[key]; !ok {
		labelsCopy := make(map[string]string, len(labels))
		for k, v := range labels {
			labelsCopy[k] = v
		}
		m.meta[key] = selfMetricMeta{name: name, labels: labelsCopy, kind: sink.MetricKindCounter}
	}
	m.mu.Unlock()
}

// SetGauge sets a named gauge with labels to value.
// Safe to call on a nil *CollectorMetrics.
func (m *CollectorMetrics) SetGauge(name string, labels map[string]string, value float64) {
	if m == nil {
		return
	}
	key := selfMetricKey(name, labels)
	m.mu.Lock()
	m.gauges[key] = value
	if _, ok := m.meta[key]; !ok {
		labelsCopy := make(map[string]string, len(labels))
		for k, v := range labels {
			labelsCopy[k] = v
		}
		m.meta[key] = selfMetricMeta{name: name, labels: labelsCopy, kind: sink.MetricKindGauge}
	}
	m.mu.Unlock()
}

// Snapshot returns all current metrics as MetricPoints at the given timestamp.
// Safe to call on a nil *CollectorMetrics (returns nil).
func (m *CollectorMetrics) Snapshot(now time.Time) []sink.MetricPoint {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	points := make([]sink.MetricPoint, 0, len(m.counters)+len(m.floatCounters)+len(m.gauges))

	for key, val := range m.counters {
		meta := m.meta[key]
		labelsCopy := make(map[string]string, len(meta.labels))
		for k, v := range meta.labels {
			labelsCopy[k] = v
		}
		points = append(points, sink.MetricPoint{
			Name:      meta.name,
			Value:     float64(val),
			Timestamp: now,
			Labels:    labelsCopy,
			Kind:      sink.MetricKindCounter,
		})
	}

	for key, val := range m.floatCounters {
		meta := m.meta[key]
		labelsCopy := make(map[string]string, len(meta.labels))
		for k, v := range meta.labels {
			labelsCopy[k] = v
		}
		points = append(points, sink.MetricPoint{
			Name:      meta.name,
			Value:     val,
			Timestamp: now,
			Labels:    labelsCopy,
			Kind:      sink.MetricKindCounter,
		})
	}

	for key, val := range m.gauges {
		meta := m.meta[key]
		labelsCopy := make(map[string]string, len(meta.labels))
		for k, v := range meta.labels {
			labelsCopy[k] = v
		}
		points = append(points, sink.MetricPoint{
			Name:      meta.name,
			Value:     val,
			Timestamp: now,
			Labels:    labelsCopy,
			Kind:      sink.MetricKindGauge,
		})
	}

	return points
}
