package railway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Khan/genqlient/graphql"
	"github.com/xevion/railway-collector/internal/logging"
	"golang.org/x/time/rate"
)

const (
	Endpoint = "https://backboard.railway.com/graphql/v2"
)

// Client wraps the genqlient GraphQL client with rate limiting and auth.
type Client struct {
	gql       graphql.Client
	limiter   *rate.Limiter
	transport *rateLimitTransport
	logger    *slog.Logger
}

// rateLimitTransport injects auth headers and tracks rate limit state.
type rateLimitTransport struct {
	token  string
	inner  http.RoundTripper
	logger *slog.Logger

	mu        sync.Mutex
	remaining int
	resetAt   time.Time
	// Set to true once we've received at least one rate limit header
	initialized bool
}

// reWaitSeconds matches "try again in 1234.567 seconds" in Railway 429 error bodies.
var reWaitSeconds = regexp.MustCompile(`try again in ([\d.]+) seconds`)

func (t *rateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+t.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.inner.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	t.trackHeaders(resp)

	// Handle 429: parse wait time, update state, return response.
	// The retry happens at the Client.wait() level, not here --
	// sleeping inside RoundTrip would exceed http.Client.Timeout.
	if resp.StatusCode == 429 {
		wait := t.parseRetryWait(resp)

		t.mu.Lock()
		newReset := time.Now().Add(wait)
		if newReset.After(t.resetAt) {
			t.resetAt = newReset
		}
		t.remaining = 0
		t.initialized = true
		t.mu.Unlock()

		t.logger.Warn("rate limited by Railway API",
			"wait", wait.Round(time.Second),
			"reset_at", newReset.Format(time.RFC3339))
	}

	return resp, nil
}

// parseRetryWait extracts the actual wait duration from a 429 response.
// Checks Retry-After header first, then parses the error body JSON for
// "try again in N seconds" messages.
func (t *rateLimitTransport) parseRetryWait(resp *http.Response) time.Duration {
	// Try Retry-After header first
	if v := resp.Header.Get("Retry-After"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}

	// Parse body for "try again in N seconds", then restore it so the
	// caller (genqlient) can still read the response.
	if resp.Body != nil {
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		// Restore body for downstream readers
		resp.Body = io.NopCloser(bytes.NewReader(body))
		if err == nil {
			if matches := reWaitSeconds.FindSubmatch(body); len(matches) > 1 {
				if secs, err := strconv.ParseFloat(string(matches[1]), 64); err == nil && secs > 0 {
					return time.Duration(math.Ceil(secs)) * time.Second
				}
			}
			// Try to parse as JSON with errors array
			var errResp struct {
				Errors []struct {
					Message string `json:"message"`
				} `json:"errors"`
			}
			if json.Unmarshal(body, &errResp) == nil {
				for _, e := range errResp.Errors {
					if matches := reWaitSeconds.FindStringSubmatch(e.Message); len(matches) > 1 {
						if secs, err := strconv.ParseFloat(matches[1], 64); err == nil && secs > 0 {
							return time.Duration(math.Ceil(secs)) * time.Second
						}
					}
				}
			}
		}
	}

	// Default: 60 seconds (conservative, not the old 5s)
	return 60 * time.Second
}

func (t *rateLimitTransport) trackHeaders(resp *http.Response) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if v := resp.Header.Get("X-RateLimit-Remaining"); v != "" {
		if remaining, err := strconv.Atoi(v); err == nil {
			t.remaining = remaining
			t.initialized = true
			if remaining < 50 && remaining > 0 {
				t.logger.Warn("rate limit running low",
					"remaining", remaining,
					"reset_at", t.resetAt.Format(time.RFC3339),
					"reset_in", time.Until(t.resetAt).Round(time.Second),
				)
			}
		}
	}
	if v := resp.Header.Get("X-RateLimit-Reset"); v != "" {
		if resetUnix, err := strconv.ParseInt(v, 10, 64); err == nil {
			t.resetAt = time.Unix(resetUnix, 0)
			t.initialized = true
		}
	}
}

// getRateLimitState returns the current rate limit state.
func (t *rateLimitTransport) getRateLimitState() (remaining int, resetAt time.Time, initialized bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.remaining, t.resetAt, t.initialized
}

// NewClient creates a new Railway API client with rate limiting.
// rps controls requests per second (0 = no limit).
func NewClient(token string, rps float64, logger *slog.Logger) *Client {
	transport := &rateLimitTransport{
		token:     token,
		inner:     http.DefaultTransport,
		logger:    logger,
		remaining: -1, // unknown until first response
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
		gql:       graphql.NewClient(Endpoint, httpClient),
		limiter:   limiter,
		transport: transport,
		logger:    logger,
	}
}

// wait blocks until it's safe to make a request. Checks both the token bucket
// (RPS smoothing) and the API's hourly rate limit (remaining/resetAt).
func (c *Client) wait(ctx context.Context) error {
	// Check API rate limit gate
	remaining, resetAt, initialized := c.transport.getRateLimitState()
	if initialized && remaining <= 0 && time.Now().Before(resetAt) {
		waitDur := time.Until(resetAt)
		c.logger.Warn("API rate limit exhausted, waiting for reset",
			"wait", waitDur.Round(time.Second),
			"reset_at", resetAt.Format(time.RFC3339))

		waitStart := time.Now()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDur):
			c.logger.Info("rate limit reset, resuming requests", "waited", time.Since(waitStart).Round(time.Second))
		}
	}

	// Token bucket for RPS smoothing
	if c.limiter != nil {
		return c.limiter.Wait(ctx)
	}
	return nil
}

// do executes fn with wait+retry and exponential backoff. Retries on 429,
// 500, 502, 503, and 504 errors up to maxAttempts times.
func do[T any](c *Client, ctx context.Context, method string, fn func() (T, error)) (T, error) {
	const maxAttempts = 3
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := c.wait(ctx); err != nil {
			var zero T
			return zero, err
		}

		callStart := time.Now()
		result, err := fn()
		callDuration := time.Since(callStart)
		if err == nil {
			c.logger.Log(ctx, logging.LevelTrace, "API request completed",
				"method", method,
				"duration", callDuration.Round(time.Millisecond),
			)
			return result, nil
		}
		lastErr = err

		// Determine if this is a retryable error
		if !c.isRetryable(err) {
			return result, c.wrapError(method, err)
		}

		// Update rate limit state from error if applicable
		c.wrapError(method, err)

		// Don't backoff after the last attempt — we're about to exit
		if attempt == maxAttempts-1 {
			break
		}

		// Calculate backoff: base * 2^attempt + jitter
		// For 429s, use the server's reset time if available
		backoff := c.retryBackoff(attempt)

		c.logger.Warn("retryable error, backing off",
			"method", method,
			"attempt", attempt+1,
			"max_attempts", maxAttempts,
			"backoff", backoff.Round(time.Millisecond),
			"error", err,
		)

		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(backoff):
		}
	}

	var zero T
	return zero, c.wrapError(method, lastErr)
}

// isRetryable returns true if the error is worth retrying.
func (c *Client) isRetryable(err error) bool {
	if err == nil {
		return false
	}
	// Check if transport detected a 429
	if limited, _ := c.IsRateLimited(); limited {
		return true
	}
	// Check error message for server errors
	msg := err.Error()
	for _, code := range []string{"429", "500", "502", "503", "504"} {
		if strings.Contains(msg, code) {
			return true
		}
	}
	if strings.Contains(msg, "Rate limit exceeded") {
		return true
	}
	return false
}

// retryBackoff calculates the backoff duration for a retry attempt.
// For 429s, respects the server's reset time. For other errors, uses
// exponential backoff: 1s * 2^attempt + random jitter up to 1s.
// Capped at 60s to avoid excessively long waits.
func (c *Client) retryBackoff(attempt int) time.Duration {
	const maxBackoff = 60 * time.Second

	// If we're rate limited, use the server's reset time
	if limited, wait := c.IsRateLimited(); limited && wait > 0 {
		if wait > maxBackoff {
			return maxBackoff
		}
		return wait
	}

	// Exponential backoff with jitter
	base := time.Second * time.Duration(1<<uint(attempt))    // 1s, 2s, 4s
	jitter := time.Duration(rand.Int64N(int64(time.Second))) // 0-1s random
	backoff := base + jitter
	if backoff > maxBackoff {
		return maxBackoff
	}
	return backoff
}

// IsRateLimited returns true if the API rate limit is exhausted, along with
// the time until reset. Used by the collection loop circuit breaker.
func (c *Client) IsRateLimited() (bool, time.Duration) {
	remaining, resetAt, initialized := c.transport.getRateLimitState()
	if initialized && remaining <= 0 && time.Now().Before(resetAt) {
		return true, time.Until(resetAt)
	}
	return false, 0
}

// RateLimitInfo returns remaining calls and reset time for budget logging.
func (c *Client) RateLimitInfo() (remaining int, resetAt time.Time) {
	r, t, _ := c.transport.getRateLimitState()
	return r, t
}

// Me returns the authenticated user info.
func (c *Client) Me(ctx context.Context) (*MeResponse, error) {
	return do(c, ctx, "Me", func() (*MeResponse, error) {
		return Me(ctx, c.gql)
	})
}

// GetProjects returns all projects, optionally filtered by workspace.
func (c *Client) GetProjects(ctx context.Context, workspaceID *string) (*ProjectsResponse, error) {
	return do(c, ctx, "GetProjects", func() (*ProjectsResponse, error) {
		return Projects(ctx, c.gql, workspaceID)
	})
}

// GetDeployments returns deployments for a service in an environment.
func (c *Client) GetDeployments(ctx context.Context, projectID, envID, serviceID string, first *int, after *string) (*DeploymentsResponse, error) {
	return do(c, ctx, "GetDeployments", func() (*DeploymentsResponse, error) {
		return Deployments(ctx, c.gql, projectID, envID, serviceID, first, after)
	})
}

// GetServiceInstance returns the service instance config.
func (c *Client) GetServiceInstance(ctx context.Context, envID, serviceID string) (*ServiceInstanceQueryResponse, error) {
	return do(c, ctx, "GetServiceInstance", func() (*ServiceInstanceQueryResponse, error) {
		return ServiceInstanceQuery(ctx, c.gql, envID, serviceID)
	})
}

// GetMetrics returns time-series metrics.
func (c *Client) GetMetrics(ctx context.Context, projectID, serviceID, envID *string, startDate string, endDate *string, measurements []MetricMeasurement, groupBy []MetricTag, sampleRate, avgWindow *int) (*MetricsResponse, error) {
	return do(c, ctx, "GetMetrics", func() (*MetricsResponse, error) {
		return Metrics(ctx, c.gql, projectID, serviceID, envID, startDate, endDate, measurements, groupBy, sampleRate, avgWindow)
	})
}

// GetDeploymentLogs returns runtime logs for a deployment.
func (c *Client) GetDeploymentLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*DeploymentLogsQueryResponse, error) {
	return do(c, ctx, "GetDeploymentLogs", func() (*DeploymentLogsQueryResponse, error) {
		return DeploymentLogsQuery(ctx, c.gql, deploymentID, limit, startDate, endDate, filter)
	})
}

// GetBuildLogs returns build logs for a deployment.
func (c *Client) GetBuildLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*BuildLogsQueryResponse, error) {
	return do(c, ctx, "GetBuildLogs", func() (*BuildLogsQueryResponse, error) {
		return BuildLogsQuery(ctx, c.gql, deploymentID, limit, startDate, endDate, filter)
	})
}

// GetHttpLogs returns HTTP access logs for a deployment.
func (c *Client) GetHttpLogs(ctx context.Context, deploymentID string, limit *int, startDate, endDate, filter *string) (*HttpLogsQueryResponse, error) {
	return do(c, ctx, "GetHttpLogs", func() (*HttpLogsQueryResponse, error) {
		return HttpLogsQuery(ctx, c.gql, deploymentID, limit, startDate, endDate, filter)
	})
}

// GetEnvironmentLogs returns runtime logs for an entire environment (all services).
func (c *Client) GetEnvironmentLogs(ctx context.Context, environmentID string, filter *string, afterDate, beforeDate *string, afterLimit, beforeLimit *int, anchorDate *string) (*EnvironmentLogsQueryResponse, error) {
	return do(c, ctx, "GetEnvironmentLogs", func() (*EnvironmentLogsQueryResponse, error) {
		return EnvironmentLogsQuery(ctx, c.gql, environmentID, filter, afterDate, beforeDate, afterLimit, beforeLimit, anchorDate)
	})
}

// wrapError checks if a GraphQL error contains a 429 status and updates rate
// limit state accordingly. genqlient returns errors for non-null data+errors
// responses, so we need to detect 429s in the error message too.
func (c *Client) wrapError(method string, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "429") || strings.Contains(msg, "Rate limit exceeded") {
		if matches := reWaitSeconds.FindStringSubmatch(msg); len(matches) > 1 {
			if secs, parseErr := strconv.ParseFloat(matches[1], 64); parseErr == nil && secs > 0 {
				wait := time.Duration(math.Ceil(secs)) * time.Second
				c.transport.mu.Lock()
				newReset := time.Now().Add(wait)
				if newReset.After(c.transport.resetAt) {
					c.transport.resetAt = newReset
				}
				c.transport.remaining = 0
				c.transport.initialized = true
				c.transport.mu.Unlock()

				c.logger.Warn("rate limit detected in error response",
					"method", method, "wait", wait)
			}
		}
	}
	return err
}
