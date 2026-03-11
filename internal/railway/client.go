package railway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	Endpoint = "https://backboard.railway.com/graphql/internal"
)

// Client wraps the genqlient GraphQL client with rate limiting and auth.
type Client struct {
	gql        graphql.Client
	httpClient *http.Client
	limiter    *rate.Limiter
	transport  *rateLimitTransport
	logger     *slog.Logger
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

	// Append operationName as a URL query parameter to avoid Cloudflare's
	// undocumented ~10 RPS rate limit on bare POST requests without query strings.
	if req.Body != nil {
		bodyBytes, err := io.ReadAll(req.Body)
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		if err == nil {
			var payload struct {
				OperationName string `json:"operationName"`
			}
			if json.Unmarshal(bodyBytes, &payload) == nil && payload.OperationName != "" {
				q := req.URL.Query()
				q.Set("operationName", payload.OperationName)
				req.URL.RawQuery = q.Encode()
			}
		}
	}

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
		gql:        graphql.NewClient(Endpoint, httpClient),
		httpClient: httpClient,
		limiter:    limiter,
		transport:  transport,
		logger:     logger,
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

		// Don't backoff after the last attempt -- we're about to exit
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

// DiscoverAll returns all projects with nested environments, service instances,
// and latest deployments for a workspace in a single query.
func (c *Client) DiscoverAll(ctx context.Context, workspaceID *string, isEphemeral *bool) (*DiscoverAllResponse, error) {
	return do(c, ctx, "DiscoverAll", func() (*DiscoverAllResponse, error) {
		return DiscoverAll(ctx, c.gql, workspaceID, isEphemeral)
	})
}

// RawQueryResponse holds the parsed result of a raw GraphQL query.
// Data contains per-alias results keyed by alias name.
// Errors contains any GraphQL-level errors from the response.
type RawQueryResponse struct {
	Data   map[string]json.RawMessage `json:"data"`
	Errors []GraphQLError             `json:"errors"`
}

// GraphQLError represents a single error from a GraphQL response.
type GraphQLError struct {
	Message string   `json:"message"`
	Path    []string `json:"path"`
}

// RawQuery executes an arbitrary GraphQL query string with variables and returns
// the raw JSON response keyed by alias. This is used for batched aliased queries
// that genqlient cannot express (dynamic number of aliases at runtime).
// The operationName is used for logging and the Cloudflare query parameter.
func (c *Client) RawQuery(ctx context.Context, operationName, query string, variables map[string]any) (*RawQueryResponse, error) {
	return do(c, ctx, operationName, func() (*RawQueryResponse, error) {
		payload := map[string]any{
			"query":         query,
			"operationName": operationName,
		}
		if len(variables) > 0 {
			payload["variables"] = variables
		}

		body, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshaling query payload: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, Endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("creating request: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("executing request: %w", err)
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading response body: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
		}

		var result RawQueryResponse
		if err := json.Unmarshal(respBody, &result); err != nil {
			return nil, fmt.Errorf("unmarshaling response: %w", err)
		}

		return &result, nil
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
