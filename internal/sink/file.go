package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// FileSink writes metrics and logs as JSON lines to a file.
type FileSink struct {
	file   *os.File
	logger *slog.Logger
	mu     sync.Mutex
}

func NewFileSink(path string, logger *slog.Logger) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening file sink %s: %w", path, err)
	}
	logger.Info("file sink opened", "path", path)
	return &FileSink{file: f, logger: logger}, nil
}

func (s *FileSink) Name() string { return "file" }

type jsonMetricLine struct {
	Type      string            `json:"type"`
	Name      string            `json:"name"`
	Value     float64           `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Labels    map[string]string `json:"labels"`
}

type jsonLogLine struct {
	Type       string            `json:"type"`
	Timestamp  time.Time         `json:"timestamp"`
	Message    string            `json:"message"`
	Severity   string            `json:"severity"`
	Labels     map[string]string `json:"labels"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (s *FileSink) WriteMetrics(_ context.Context, metrics []MetricPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	enc := json.NewEncoder(s.file)
	for _, m := range metrics {
		if err := enc.Encode(jsonMetricLine{
			Type:      "metric",
			Name:      m.Name,
			Value:     m.Value,
			Timestamp: m.Timestamp,
			Labels:    m.Labels,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileSink) WriteLogs(_ context.Context, logs []LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	enc := json.NewEncoder(s.file)
	for _, l := range logs {
		if err := enc.Encode(jsonLogLine{
			Type:       "log",
			Timestamp:  l.Timestamp,
			Message:    l.Message,
			Severity:   l.Severity,
			Labels:     l.Labels,
			Attributes: l.Attributes,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileSink) Close() error {
	return s.file.Close()
}
