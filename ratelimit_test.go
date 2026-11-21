package ratelimit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketBurstThenRefill(t *testing.T) {
	tb := NewTokenBucket(1, 5) // 1 token/s, burst 5
	base := time.Now()
	cur := base
	tb.now = func() time.Time { return cur }
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if d, _ := tb.Allow(ctx, "k"); !d.Allowed {
			t.Fatalf("burst request %d should be allowed", i+1)
		}
	}
	d, _ := tb.Allow(ctx, "k")
	if d.Allowed {
		t.Fatal("6th request should be denied")
	}
	if d.RetryAfter <= 0 || d.RetryAfter > time.Second {
		t.Fatalf("RetryAfter should be ~1s, got %v", d.RetryAfter)
	}

	cur = base.Add(time.Second) // exactly one token refills
	if d, _ := tb.Allow(ctx, "k"); !d.Allowed {
		t.Fatal("request after 1s refill should be allowed")
	}
	if d, _ := tb.Allow(ctx, "k"); d.Allowed {
		t.Fatal("only one token should have refilled")
	}
}

func TestTokenBucketConcurrent(t *testing.T) {
	const burst = 100
	tb := NewTokenBucket(0.001, burst) // refill negligible over the test
	allowed := countConcurrentAllowed(500, func() bool {
		d, _ := tb.Allow(context.Background(), "shared")
		return d.Allowed
	})
	if allowed != burst {
		t.Fatalf("expected exactly %d allowed, got %d", burst, allowed)
	}
}

func TestSlidingWindowBoundary(t *testing.T) {
	sw := NewSlidingWindow(3, time.Second)
	base := time.Now()
	cur := base
	sw.now = func() time.Time { return cur }
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if d, _ := sw.Allow(ctx, "k"); !d.Allowed {
			t.Fatalf("request %d should be allowed", i+1)
		}
	}
	if d, _ := sw.Allow(ctx, "k"); d.Allowed {
		t.Fatal("4th request in window should be denied")
	}
	cur = base.Add(time.Second + time.Millisecond) // whole window slides past
	if d, _ := sw.Allow(ctx, "k"); !d.Allowed {
		t.Fatal("request after the window should be allowed")
	}
}

func TestSlidingWindowConcurrent(t *testing.T) {
	const limit = 50
	sw := NewSlidingWindow(limit, time.Hour) // nothing expires mid-test
	allowed := countConcurrentAllowed(300, func() bool {
		d, _ := sw.Allow(context.Background(), "shared")
		return d.Allowed
	})
	if allowed != limit {
		t.Fatalf("expected exactly %d allowed, got %d", limit, allowed)
	}
}

// countConcurrentAllowed fires n calls at once and returns how many returned true.
func countConcurrentAllowed(n int, call func() bool) int64 {
	var allowed int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all goroutines together to maximise contention
			if call() {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	close(start)
	wg.Wait()
	return allowed
}

func BenchmarkTokenBucket(b *testing.B) {
	tb := NewTokenBucket(1e9, 1<<30) // effectively unlimited: measure the limiter path, not denials
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = tb.Allow(ctx, "bench")
		}
	})
}
