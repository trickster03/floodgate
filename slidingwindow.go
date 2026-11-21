package ratelimit

import (
	"context"
	"sync"
	"time"
)

// SlidingWindow is an in-process sliding-window-log limiter.
//
// It keeps the timestamps of recent requests per key and allows a new one only
// while fewer than limit of them fall inside the trailing window. Unlike a fixed
// window it has no boundary burst (a client cannot fire 2x the limit around the
// window edge), at the cost of storing one timestamp per in-window request.
type SlidingWindow struct {
	limit  int
	window time.Duration

	mu   sync.Mutex
	hits map[string][]time.Time
	now  func() time.Time // injectable clock for tests
}

// NewSlidingWindow allows at most limit requests per key within any trailing
// window of the given duration.
func NewSlidingWindow(limit int, window time.Duration) *SlidingWindow {
	return &SlidingWindow{
		limit:  limit,
		window: window,
		hits:   make(map[string][]time.Time),
		now:    time.Now,
	}
}

// Allow records a request for key if the window has room.
func (sw *SlidingWindow) Allow(_ context.Context, key string) (Decision, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := sw.now()
	cutoff := now.Add(-sw.window)

	// Drop timestamps that have aged out of the window.
	ts := sw.hits[key]
	drop := 0
	for drop < len(ts) && !ts[drop].After(cutoff) {
		drop++
	}
	ts = ts[drop:]

	d := Decision{Limit: sw.limit}
	if len(ts) < sw.limit {
		ts = append(ts, now)
		d.Allowed = true
		d.Remaining = sw.limit - len(ts)
		d.ResetAfter = ts[0].Add(sw.window).Sub(now) // when the oldest expires
	} else {
		// Full: the next slot frees when the oldest in-window request expires.
		d.RetryAfter = ts[0].Add(sw.window).Sub(now)
		d.ResetAfter = d.RetryAfter
	}

	if len(ts) == 0 {
		delete(sw.hits, key)
	} else {
		sw.hits[key] = ts
	}
	return d, nil
}
