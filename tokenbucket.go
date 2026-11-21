package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// TokenBucket is an in-process token-bucket limiter.
//
// Each key owns a bucket of up to burst tokens that refills at ratePerSec
// tokens per second. A request costs one token; when the bucket is empty the
// request is denied. This allows short bursts up to burst while holding the
// long-run average at ratePerSec.
//
// State lives in this process only: three replicas behind a load balancer each
// keep their own buckets, so the effective global limit is 3x. Use a Redis*
// limiter when the limit must hold across instances.
type TokenBucket struct {
	ratePerSec float64
	burst      float64

	mu      sync.Mutex
	buckets map[string]*bucketState
	now     func() time.Time // injectable clock for tests
}

type bucketState struct {
	tokens float64
	last   time.Time
}

// NewTokenBucket returns a limiter that refills ratePerSec tokens per second up
// to a capacity of burst.
func NewTokenBucket(ratePerSec float64, burst int) *TokenBucket {
	return &TokenBucket{
		ratePerSec: ratePerSec,
		burst:      float64(burst),
		buckets:    make(map[string]*bucketState),
		now:        time.Now,
	}
}

// Allow consumes one token for key if available.
func (tb *TokenBucket) Allow(_ context.Context, key string) (Decision, error) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := tb.now()
	b, ok := tb.buckets[key]
	if !ok {
		b = &bucketState{tokens: tb.burst, last: now}
		tb.buckets[key] = b
	}

	// Refill for the time elapsed since we last touched this bucket.
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(tb.burst, b.tokens+elapsed*tb.ratePerSec)
		b.last = now
	}

	d := Decision{Limit: int(tb.burst)}
	if b.tokens >= 1 {
		b.tokens--
		d.Allowed = true
	} else if tb.ratePerSec > 0 {
		d.RetryAfter = secondsToDuration((1 - b.tokens) / tb.ratePerSec)
	}
	d.Remaining = int(b.tokens)
	if tb.ratePerSec > 0 {
		d.ResetAfter = secondsToDuration((tb.burst - b.tokens) / tb.ratePerSec)
	}
	return d, nil
}

func secondsToDuration(s float64) time.Duration {
	if math.IsInf(s, 0) || s < 0 {
		return 0
	}
	return time.Duration(s * float64(time.Second))
}
