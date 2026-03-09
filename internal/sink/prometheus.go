package sink

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusSink exposes Railway metrics as a Prometheus /metrics endpoint.
type PrometheusSink struct {
	registry *prometheus.Registry
	server   *http.Server
	logger   *slog.Logger

	mu     sync.RWMutex
	gauges map[string]*prometheus.GaugeVec
}

func NewPrometheusSink(listen string, logger *slog.Logger) *PrometheusSink {
	registry := prometheus.NewRegistry()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})

	server := &http.Server{
		Addr:    listen,
		Handler: mux,
	}

	s := &PrometheusSink{
		registry: registry,
		server:   server,
		logger:   logger,
		gauges:   make(map[string]*prometheus.GaugeVec),
	}

	go func() {
		logger.Info("prometheus sink listening", "addr", listen)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("prometheus server error", "error", err)
		}
	}()

	return s
}

func (s *PrometheusSink) Name() string { return "prometheus" }

func (s *PrometheusSink) WriteMetrics(_ context.Context, metrics []MetricPoint) error {
	for _, m := range metrics {
		gauge := s.getOrCreateGauge(m.Name, m.Labels)
		labelValues := s.extractLabelValues(gauge, m.Labels)
		gauge.WithLabelValues(labelValues...).Set(m.Value)
	}
	return nil
}

func (s *PrometheusSink) WriteLogs(_ context.Context, _ []LogEntry) error {
	// Prometheus doesn't handle logs — this is a no-op.
	// Logs are handled by file sink or future Loki/OTLP sinks.
	return nil
}

func (s *PrometheusSink) Close() error {
	return s.server.Close()
}

// getOrCreateGauge returns or creates a GaugeVec for the given metric name and label set.
func (s *PrometheusSink) getOrCreateGauge(name string, labels map[string]string) *prometheus.GaugeVec {
	// Build a cache key from name + sorted label keys
	labelNames := sortedKeys(labels)
	cacheKey := name + "|" + strings.Join(labelNames, ",")

	s.mu.RLock()
	if g, ok := s.gauges[cacheKey]; ok {
		s.mu.RUnlock()
		return g
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check
	if g, ok := s.gauges[cacheKey]; ok {
		return g
	}

	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: name,
		Help: fmt.Sprintf("Railway metric: %s", name),
	}, labelNames)

	if err := s.registry.Register(g); err != nil {
		s.logger.Warn("failed to register metric", "name", name, "error", err)
		return g
	}
	s.gauges[cacheKey] = g
	return g
}

func (s *PrometheusSink) extractLabelValues(gauge *prometheus.GaugeVec, labels map[string]string) []string {
	keys := sortedKeys(labels)
	values := make([]string, len(keys))
	for i, k := range keys {
		values[i] = labels[k]
	}
	return values
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
