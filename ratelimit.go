// Package ratelimit provides composable HTTP rate limiters.
//
// It ships two algorithms — token bucket and sliding-window log — each in two
// flavours: an in-process version (fast, per-instance) and a Redis-backed
// version (coordinated across instances). The in-process limiters guard shared
// state with a mutex; the Redis limiters run their read-modify-write as a single
// atomic Lua script so concurrent requests across replicas cannot race.
//
// See the Limiter interface: everything in this package is built around it, and
// the net/http Middleware works with any implementation.
package ratelimit

import (
	"context"
	"time"
)

// Decision is the outcome of a single Allow check.
type Decision struct {
	Allowed    bool          // whether the request may proceed
	Limit      int           // bucket capacity / window limit, for the X-RateLimit-Limit header
	Remaining  int           // approximate requests left before the next denial
	RetryAfter time.Duration // how long until a denied request would be allowed (0 when allowed)
	ResetAfter time.Duration // until the bucket refills / the window empties
}

// Limiter decides whether a request identified by key may proceed.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}
