package sink

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// OTLPSink exports metrics and logs via OTLP HTTP to backends like
// VictoriaMetrics and VictoriaLogs.
type OTLPSink struct {
	metricExporter *otlpmetrichttp.Exporter
	logExporter    *otlploghttp.Exporter
	resource       *resource.Resource
	logger         *slog.Logger
}

type OTLPConfig struct {
	MetricsEndpoint string
	LogsEndpoint    string
	Headers         map[string]string
}

func NewOTLPSink(ctx context.Context, cfg OTLPConfig, logger *slog.Logger) (*OTLPSink, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("railway-collector"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating OTLP resource: %w", err)
	}

	s := &OTLPSink{
		resource: res,
		logger:   logger,
	}

	if cfg.MetricsEndpoint != "" {
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpointURL(cfg.MetricsEndpoint),
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlpmetrichttp.WithHeaders(cfg.Headers))
		}
		exp, err := otlpmetrichttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("creating OTLP metric exporter: %w", err)
		}
		s.metricExporter = exp
		logger.Info("OTLP metrics exporter initialized", "endpoint", cfg.MetricsEndpoint)
	}

	if cfg.LogsEndpoint != "" {
		opts := []otlploghttp.Option{
			otlploghttp.WithEndpointURL(cfg.LogsEndpoint),
		}
		if len(cfg.Headers) > 0 {
			opts = append(opts, otlploghttp.WithHeaders(cfg.Headers))
		}
		exp, err := otlploghttp.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("creating OTLP log exporter: %w", err)
		}
		s.logExporter = exp
		logger.Info("OTLP logs exporter initialized", "endpoint", cfg.LogsEndpoint)
	}

	return s, nil
}

func (s *OTLPSink) Name() string { return "otlp" }

func (s *OTLPSink) WriteMetrics(ctx context.Context, metrics []MetricPoint) error {
	if s.metricExporter == nil || len(metrics) == 0 {
		return nil
	}

	// Group metrics by name to create proper OTLP metric structures
	byName := make(map[string][]MetricPoint)
	for _, m := range metrics {
		byName[m.Name] = append(byName[m.Name], m)
	}

	var scopeMetrics []metricdata.ScopeMetrics
	var otelMetrics []metricdata.Metrics

	for name, points := range byName {
		var dataPoints []metricdata.DataPoint[float64]
		for _, p := range points {
			attrs := make([]attribute.KeyValue, 0, len(p.Labels))
			for k, v := range p.Labels {
				attrs = append(attrs, attribute.String(k, v))
			}
			dataPoints = append(dataPoints, metricdata.DataPoint[float64]{
				Attributes: attribute.NewSet(attrs...),
				Time:       p.Timestamp,
				Value:      p.Value,
			})
		}

		otelMetrics = append(otelMetrics, metricdata.Metrics{
			Name: name,
			Data: metricdata.Gauge[float64]{DataPoints: dataPoints},
		})
	}

	scopeMetrics = append(scopeMetrics, metricdata.ScopeMetrics{
		Metrics: otelMetrics,
	})

	rm := &metricdata.ResourceMetrics{
		Resource:     s.resource,
		ScopeMetrics: scopeMetrics,
	}

	if err := s.metricExporter.Export(ctx, rm); err != nil {
		return fmt.Errorf("exporting OTLP metrics: %w", err)
	}
	return nil
}

func (s *OTLPSink) WriteLogs(ctx context.Context, logs []LogEntry) error {
	if s.logExporter == nil || len(logs) == 0 {
		return nil
	}

	records := make([]sdklog.Record, 0, len(logs))
	for _, l := range logs {
		var rec sdklog.Record
		rec.SetTimestamp(l.Timestamp)
		rec.SetObservedTimestamp(time.Now())
		rec.SetBody(otellog.StringValue(l.Message))
		rec.SetSeverityText(l.Severity)

		// Map severity text to OTel severity number
		switch l.Severity {
		case "debug":
			rec.SetSeverity(otellog.SeverityDebug)
		case "info":
			rec.SetSeverity(otellog.SeverityInfo)
		case "warn", "warning":
			rec.SetSeverity(otellog.SeverityWarn)
		case "error":
			rec.SetSeverity(otellog.SeverityError)
		case "fatal":
			rec.SetSeverity(otellog.SeverityFatal)
		}

		// Add labels + attributes as OTel log attributes
		attrs := make([]otellog.KeyValue, 0, len(l.Labels)+len(l.Attributes))
		for k, v := range l.Labels {
			attrs = append(attrs, otellog.String(k, v))
		}
		for k, v := range l.Attributes {
			attrs = append(attrs, otellog.String(k, v))
		}
		rec.AddAttributes(attrs...)

		records = append(records, rec)
	}

	if err := s.logExporter.Export(ctx, records); err != nil {
		return fmt.Errorf("exporting OTLP logs: %w", err)
	}
	return nil
}

func (s *OTLPSink) Close() error {
	ctx := context.Background()
	var firstErr error
	if s.metricExporter != nil {
		if err := s.metricExporter.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.logExporter != nil {
		if err := s.logExporter.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Ensure OTLPSink satisfies the Sink interface at compile time.
var _ Sink = (*OTLPSink)(nil)
