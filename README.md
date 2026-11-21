# go-rate-limiter

A small, composable HTTP rate limiter for Go. Two algorithms — **token bucket** and **sliding-window log** — each available **in-process** (fast, per-instance) or **Redis-backed** (one limit enforced across every replica). Drops into any `net/http` service as middleware.

```go
limiter := ratelimit.NewTokenBucket(5, 10) // 5 req/s, bursts up to 10
http.Handle("/", ratelimit.Middleware(limiter, ratelimit.KeyByIP)(handler))
```

## Why both in-process *and* distributed?

This is the whole point of the project, and the thing worth understanding:

- An **in-process** limiter keeps its counters in the Go process's memory. It's fast (no network hop) but each replica counts on its own — run three behind a load balancer with a 100 req/s limit and a client actually gets **~300 req/s**.
- A **distributed** limiter keeps its state in Redis, shared by every replica, so the limit holds **globally** no matter how many instances you run. The cost is a Redis round-trip per request and the need to make the read-modify-write **atomic** — which here is done with a Lua script, because Redis runs a script to completion before any other command, so concurrent requests (on one instance or across many) can't race.

Same interface either way — swap `NewTokenBucket` for `NewRedisTokenBucket` and nothing else changes.

## Algorithms

| Algorithm | Burst behaviour | Memory per client | Use when |
|---|---|---|---|
| Token bucket | Allows controlled bursts up to capacity | 2 fields | Bursty traffic with an average-rate cap |
| Sliding window (log) | No boundary burst | 1 entry per in-window request | You need exact request tracking |

(Fixed-window and leaky-bucket are easy variants of the same code; this repo ships the two that matter most in practice.)

## Quick start

```bash
# in-process token bucket
go run ./cmd/demo -algo tokenbucket -rate 5 -burst 10

# distributed sliding window, state in Redis
go run ./cmd/demo -algo sliding -limit 10 -window 1s -redis localhost:6379

# limit per API key instead of per IP
go run ./cmd/demo -algo tokenbucket -rate 5 -burst 10 -header X-API-Key
```

Hammer it and watch the burst drain:

```
$ for i in $(seq 1 15); do curl -s -o /dev/null -w "%{http_code} " localhost:8080/; done
200 200 200 200 200 200 200 200 200 200 429 429 429 429 429

$ curl -s -D - -o /dev/null localhost:8080/   # on a rejected request
HTTP/1.1 429 Too Many Requests
Retry-After: 1
X-RateLimit-Limit: 10
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 2
```

## Library use

```go
import ratelimit "github.com/trickster03/go-rate-limiter"

// in-process
l := ratelimit.NewTokenBucket(100, 200)                 // 100 req/s, burst 200

// distributed (shared across replicas)
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
l := ratelimit.NewRedisTokenBucket(rdb, 100, 200, "rl:api:")

// wrap any handler
h = ratelimit.Middleware(l, ratelimit.KeyByHeader("X-API-Key"))(h)
```

Every limiter satisfies one interface:

```go
type Limiter interface {
	Allow(ctx context.Context, key string) (Decision, error)
}
```

## Design notes

- **Atomicity** — the Redis limiters do their whole refill-and-consume (or window-prune-and-count) inside one Lua script, so there is no check-then-set race across instances. The in-process limiters use a mutex.
- **Fail-open** — if the limiter errors (e.g. Redis is unreachable), the middleware serves the request rather than dropping it, so a limiter outage doesn't take the service down. Switch to fail-closed where correctness beats availability.
- **Standard headers** — `X-RateLimit-Limit/Remaining/Reset` on every response, `Retry-After` on a 429.
- **Keying** — limit per IP (`KeyByIP`, honours `X-Forwarded-For`) or per header such as an API key (`KeyByHeader`), which falls back to IP so unauthenticated callers are still limited.

## Benchmark

In-process token bucket, `go test -bench`, Apple Silicon:

```
BenchmarkTokenBucket-8   5388832   204.8 ns/op   0 B/op   0 allocs/op
```

~205 ns and zero allocations per decision — the limiter is not your bottleneck.

## Tests

```bash
go test -race ./...                              # in-process limiters
REDIS_ADDR=localhost:6379 go test -race ./...    # also runs the Redis-backed tests
```

The concurrency tests fire 500 requests at once at a bucket of 100 and assert that **exactly 100** are allowed — the check that proves the limit holds under contention, in-process and across shared Redis alike.

## Layout

```
ratelimit.go       Limiter interface + Decision
tokenbucket.go     in-process token bucket
slidingwindow.go   in-process sliding-window log
redis.go           Redis-backed token bucket + sliding window (Lua)
middleware.go      net/http middleware, IP / header keying
cmd/demo           runnable demo server
```

## Roadmap

- Prometheus metrics endpoint (allowed / denied counters per key)
- Two-tier limiter: fast local pre-check + Redis for the global truth, to cut the round-trip
- Load-test harness comparing in-process vs Redis at N replicas
