package collector

import "context"

type contextKey string

const triggerContextKey contextKey = "trigger"

// withTrigger returns a new context that carries the given trigger label.
// The trigger describes why a collection was initiated (e.g. "startup", "scheduled").
func withTrigger(ctx context.Context, trigger string) context.Context {
	return context.WithValue(ctx, triggerContextKey, trigger)
}

// triggerFromCtx extracts the trigger label from ctx, or returns an empty string if not set.
func triggerFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(triggerContextKey).(string); ok {
		return v
	}
	return ""
}
