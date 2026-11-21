package ratelimit

import (
	"context"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// The limiter state lives in Redis so every replica sees one shared counter and
// the configured limit holds globally. Each check is a single EVALSHA: Redis
// runs a script to completion before any other command, so the read-modify-write
// is atomic and concurrent requests — on this instance or another — cannot race.

// tokenBucketScript refills and consumes a bucket stored as a hash {tokens, ts}.
// KEYS[1]=key  ARGV: rate(/s), burst, now(ms), cost
// returns: {allowed, remaining, retry_after_ms, reset_after_ms}
var tokenBucketScript = redis.NewScript(`
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now   = tonumber(ARGV[3])
local cost  = tonumber(ARGV[4])

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = burst
  ts = now
end

local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(burst, tokens + elapsed * rate)

local allowed = 0
local retry = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
elseif rate > 0 then
  retry = math.ceil(((cost - tokens) / rate) * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
local reset = 0
if rate > 0 then
  reset = math.ceil(((burst - tokens) / rate) * 1000)
  redis.call('PEXPIRE', key, reset + 1000)
end
return {allowed, math.floor(tokens), retry, reset}
`)

// slidingWindowScript keeps request timestamps in a sorted set and counts the
// ones still inside the window.
// KEYS[1]=key  ARGV: now(ms), window(ms), limit, member
// returns: {allowed, remaining, retry_after_ms}
var slidingWindowScript = redis.NewScript(`
local key    = KEYS[1]
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)

local allowed = 0
local retry = 0
if count < limit then
  redis.call('ZADD', key, now, member)
  allowed = 1
  count = count + 1
else
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  if oldest[2] ~= nil then
    retry = (tonumber(oldest[2]) + window) - now
    if retry < 0 then retry = 0 end
  end
end
redis.call('PEXPIRE', key, window)

local remaining = limit - count
if remaining < 0 then remaining = 0 end
return {allowed, remaining, retry}
`)

// RedisTokenBucket is a token-bucket limiter whose state lives in Redis.
type RedisTokenBucket struct {
	rdb    redis.Scripter
	rate   float64
	burst  int
	prefix string
}

// NewRedisTokenBucket builds a Redis-backed token bucket. keyPrefix is prepended
// to every key so several limiters can share one Redis without colliding.
func NewRedisTokenBucket(rdb redis.Scripter, ratePerSec float64, burst int, keyPrefix string) *RedisTokenBucket {
	return &RedisTokenBucket{rdb: rdb, rate: ratePerSec, burst: burst, prefix: keyPrefix}
}

// Allow consumes one token for key.
func (r *RedisTokenBucket) Allow(ctx context.Context, key string) (Decision, error) {
	now := time.Now().UnixMilli()
	res, err := tokenBucketScript.Run(ctx, r.rdb, []string{r.prefix + key},
		r.rate, r.burst, now, 1).Int64Slice()
	if err != nil {
		return Decision{}, err
	}
	return Decision{
		Allowed:    res[0] == 1,
		Limit:      r.burst,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
		ResetAfter: time.Duration(res[3]) * time.Millisecond,
	}, nil
}

// RedisSlidingWindow is a sliding-window-log limiter backed by Redis.
type RedisSlidingWindow struct {
	rdb    redis.Scripter
	limit  int
	window time.Duration
	prefix string
	seq    atomic.Uint64 // makes sorted-set members unique within a process
}

// NewRedisSlidingWindow allows at most limit requests per key within the window.
func NewRedisSlidingWindow(rdb redis.Scripter, limit int, window time.Duration, keyPrefix string) *RedisSlidingWindow {
	return &RedisSlidingWindow{rdb: rdb, limit: limit, window: window, prefix: keyPrefix}
}

// Allow records a request for key if the window has room.
func (r *RedisSlidingWindow) Allow(ctx context.Context, key string) (Decision, error) {
	nowNs := time.Now().UnixNano()
	member := strconv.FormatInt(nowNs, 10) + "-" + strconv.FormatUint(r.seq.Add(1), 10)
	res, err := slidingWindowScript.Run(ctx, r.rdb, []string{r.prefix + key},
		nowNs/1e6, r.window.Milliseconds(), r.limit, member).Int64Slice()
	if err != nil {
		return Decision{}, err
	}
	return Decision{
		Allowed:    res[0] == 1,
		Limit:      r.limit,
		Remaining:  int(res[1]),
		RetryAfter: time.Duration(res[2]) * time.Millisecond,
		ResetAfter: time.Duration(res[2]) * time.Millisecond,
	}, nil
}
