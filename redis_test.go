package ratelimit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis-backed tests run only when REDIS_ADDR is set, e.g.
//
//	REDIS_ADDR=localhost:6379 go test -race ./...
func testRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("set REDIS_ADDR to run Redis-backed tests")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	return rdb
}

func TestRedisTokenBucketConcurrent(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	const burst = 100
	key := "tb-" + time.Now().Format("150405.000000")
	t.Cleanup(func() { rdb.Del(ctx, "rl:tb:"+key) })

	lim := NewRedisTokenBucket(rdb, 0.001, burst, "rl:tb:")
	allowed := countConcurrentAllowed(500, func() bool {
		d, err := lim.Allow(ctx, key)
		return err == nil && d.Allowed
	})
	if allowed != burst {
		t.Fatalf("expected exactly %d allowed across the shared Redis bucket, got %d", burst, allowed)
	}
}

func TestRedisSlidingWindowConcurrent(t *testing.T) {
	rdb := testRedis(t)
	ctx := context.Background()
	const limit = 50
	key := "sw-" + time.Now().Format("150405.000000")
	t.Cleanup(func() { rdb.Del(ctx, "rl:sw:"+key) })

	lim := NewRedisSlidingWindow(rdb, limit, time.Hour, "rl:sw:")
	allowed := countConcurrentAllowed(300, func() bool {
		d, err := lim.Allow(ctx, key)
		return err == nil && d.Allowed
	})
	if allowed != limit {
		t.Fatalf("expected exactly %d allowed across the shared window, got %d", limit, allowed)
	}
}
