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

	const maxRetries = 3
	for attempt := 0; resp.StatusCode == 429 && attempt < maxRetries; attempt++ {
		// Parse Retry-After header; default to 5s
		wait := 5 * time.Second
		if v := resp.Header.Get("Retry-After"); v != "" {
			if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
				wait = time.Duration(secs) * time.Second
			}
		}

		t.logger.Warn("rate limited by Railway API, retrying",
			"retry_after", wait, "attempt", attempt+1, "max_retries", maxRetries)

		resp.Body.Close()

		// Ensure we can re-read the request body
		if req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("getting request body for retry: %w", err)
			}
			req.Body = body
		} else if req.Body != nil {
			// Body was consumed and can't be re-created
			return nil, fmt.Errorf("cannot retry request: GetBody is nil and body was consumed")
		}

		time.Sleep(wait)

		resp, err = t.inner.RoundTrip(req)
		if err != nil {
			return resp, err
		}

		// Re-track rate limit headers on retry response
		if v := resp.Header.Get("X-RateLimit-Remaining"); v != "" {
			if remaining, err := strconv.Atoi(v); err == nil {
				t.mu.Lock()
				t.remaining = remaining
				t.mu.Unlock()
			}
		}
		if v := resp.Header.Get("X-RateLimit-Reset"); v != "" {
			if resetUnix, err := strconv.ParseInt(v, 10, 64); err == nil {
				t.mu.Lock()
				t.resetAt = time.Unix(resetUnix, 0)
				t.mu.Unlock()
			}
		}
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
