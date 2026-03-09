package railway

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Khan/genqlient/graphql"
	"golang.org/x/time/rate"
)

const (
	Endpoint = "https://backboard.railway.com/graphql/v2"
)

// Client wraps the genqlient GraphQL client with rate limiting and auth.
type Client struct {
	gql     graphql.Client
	limiter *rate.Limiter
	logger  *slog.Logger
}

// rateLimitTransport injects auth headers and tracks rate limit state.
type rateLimitTransport struct {
	token     string
	inner     http.RoundTripper
	mu        sync.Mutex
	remaining int
	resetAt   time.Time
	logger    *slog.Logger
}

func (t *rateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// Track rate limit headers
	if v := resp.Header.Get("X-RateLimit-Remaining"); v != "" {
		if remaining, err := strconv.Atoi(v); err == nil {
			t.mu.Lock()
			t.remaining = remaining
			t.mu.Unlock()
			if remaining < 50 {
				t.logger.Warn("rate limit running low", "remaining", remaining)
			}
		}
	}
	if v := resp.Header.Get("X-RateLimit-Reset"); v != "" {
		if resetUnix, err := strconv.ParseInt(v, 10, 64); err == nil {
			t.mu.Lock()
			t.resetAt = time.Unix(resetUnix, 0)
			t.mu.Unlock()
		}
	}

	if resp.StatusCode == 429 {
		retryAfter := resp.Header.Get("Retry-After")
		t.logger.Warn("rate limited by Railway API", "retry_after", retryAfter)
	}

	return resp, nil
}

// NewClient creates a new Railway API client with rate limiting.
// rps controls requests per second (0 = no limit).
func NewClient(token string, rps float64, logger *slog.Logger) *Client {
	transport := &rateLimitTransport{
		token:  token,
		inner:  http.DefaultTransport,
		logger: logger,
	}
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	var limiter *rate.Limiter
	if rps > 0 {
		limiter = rate.NewLimiter(rate.Limit(rps), int(rps)+1)
	}

	return &Client{
		gql:     graphql.NewClient(Endpoint, httpClient),
		limiter: limiter,
		logger:  logger,
	}
}

func (c *Client) wait(ctx context.Context) error {
	if c.limiter != nil {
		return c.limiter.Wait(ctx)
	}
	return nil
}

// Me returns the authenticated user info.
func (c *Client) Me(ctx context.Context) (*MeResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return Me(ctx, c.gql)
}

// GetProjects returns all projects, optionally filtered by workspace.
func (c *Client) GetProjects(ctx context.Context, workspaceID *string) (*ProjectsResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return Projects(ctx, c.gql, workspaceID)
}

// GetDeployments returns deployments for a service in an environment.
func (c *Client) GetDeployments(ctx context.Context, projectID, envID, serviceID string, first *int, after *string) (*DeploymentsResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return Deployments(ctx, c.gql, projectID, envID, serviceID, first, after)
}

// GetServiceInstance returns the service instance config.
func (c *Client) GetServiceInstance(ctx context.Context, envID, serviceID string) (*ServiceInstanceQueryResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return ServiceInstanceQuery(ctx, c.gql, envID, serviceID)
}

// GetMetrics returns time-series metrics.
func (c *Client) GetMetrics(ctx context.Context, projectID, serviceID, envID *string, startDate string, endDate *string, measurements []MetricMeasurement, groupBy []MetricTag, sampleRate, avgWindow *int) (*MetricsResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return Metrics(ctx, c.gql, projectID, serviceID, envID, startDate, endDate, measurements, groupBy, sampleRate, avgWindow)
}

// GetUsage returns aggregated usage.
func (c *Client) GetUsage(ctx context.Context, projectID, workspaceID *string, measurements []MetricMeasurement, groupBy []MetricTag, startDate, endDate *string) (*UsageResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return Usage(ctx, c.gql, projectID, workspaceID, measurements, groupBy, startDate, endDate)
}

// GetEstimatedUsage returns estimated billing.
func (c *Client) GetEstimatedUsage(ctx context.Context, projectID, workspaceID *string, measurements []MetricMeasurement) (*EstimatedUsageResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return EstimatedUsage(ctx, c.gql, projectID, workspaceID, measurements)
}

// GetDeploymentLogs returns runtime logs for a deployment.
func (c *Client) GetDeploymentLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*DeploymentLogsQueryResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return DeploymentLogsQuery(ctx, c.gql, deploymentID, limit, startDate, endDate, filter)
}

// GetBuildLogs returns build logs for a deployment.
func (c *Client) GetBuildLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*BuildLogsQueryResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return BuildLogsQuery(ctx, c.gql, deploymentID, limit, startDate, endDate, filter)
}

// GetHttpLogs returns HTTP access logs for a deployment.
func (c *Client) GetHttpLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*HttpLogsQueryResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return HttpLogsQuery(ctx, c.gql, deploymentID, limit, startDate, endDate, filter)
}

// GetEnvironmentLogs returns all logs for an environment.
func (c *Client) GetEnvironmentLogs(ctx context.Context, envID string, filter, afterDate, beforeDate *string, afterLimit, beforeLimit *int, anchorDate *string) (*EnvironmentLogsQueryResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	return EnvironmentLogsQuery(ctx, c.gql, envID, filter, afterDate, beforeDate, afterLimit, beforeLimit, anchorDate)
}

// GetTcpProxies returns TCP proxies for a service.
func (c *Client) GetTcpProxies(ctx context.Context, envID, serviceID string) (*TcpProxiesQueryResponse, error) {
	if err := c.wait(ctx); err != nil {
		return nil, err
	}
	resp, err := TcpProxiesQuery(ctx, c.gql, envID, serviceID)
	if err != nil {
		return nil, fmt.Errorf("querying TCP proxies: %w", err)
	}
	return resp, nil
}
