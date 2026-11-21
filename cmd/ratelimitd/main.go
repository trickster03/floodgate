// Command ratelimitd is a standalone, distributed rate-limit service.
//
// Other services POST a client key and get back an allow/deny decision computed
// from shared (Redis) state, so one global limit holds across every caller and
// every replica — the Envoy/Lyft "ratelimit service" pattern. Point the whole
// platform at one of these instead of embedding a limiter in each service.
//
//	curl -s localhost:8080/v1/check -d '{"key":"user:42"}'
//	{"allowed":true,"limit":20,"remaining":19,"retry_after_ms":0,"reset_after_ms":100}
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	ratelimit "github.com/trickster03/floodgate"

	"github.com/redis/go-redis/v9"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	addr := env("ADDR", ":8080")
	redisAddr := env("REDIS_ADDR", "") // empty = in-process (single replica only)
	algo := env("ALGO", "tokenbucket") // tokenbucket | sliding
	rate, _ := strconv.ParseFloat(env("RATE_PER_SEC", "10"), 64)
	burst, _ := strconv.Atoi(env("BURST", "20"))
	limit, _ := strconv.Atoi(env("LIMIT", "10"))
	window, _ := time.ParseDuration(env("WINDOW", "1s"))

	var rdb *redis.Client
	if redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: redisAddr})
	}

	var lim ratelimit.Limiter
	switch algo {
	case "sliding":
		if rdb != nil {
			lim = ratelimit.NewRedisSlidingWindow(rdb, limit, window, "rl:sw:")
		} else {
			lim = ratelimit.NewSlidingWindow(limit, window)
		}
	default:
		if rdb != nil {
			lim = ratelimit.NewRedisTokenBucket(rdb, rate, burst, "rl:tb:")
		} else {
			lim = ratelimit.NewTokenBucket(rate, burst)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/v1/check", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"POST only"}`, http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Key string `json:"key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			http.Error(w, `{"error":"missing or invalid \"key\""}`, http.StatusBadRequest)
			return
		}
		d, err := lim.Allow(r.Context(), req.Key)
		if err != nil {
			http.Error(w, `{"error":"limiter backend unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"allowed":        d.Allowed,
			"limit":          d.Limit,
			"remaining":      d.Remaining,
			"retry_after_ms": d.RetryAfter.Milliseconds(),
			"reset_after_ms": d.ResetAfter.Milliseconds(),
		})
	})

	backend := "in-process"
	if rdb != nil {
		backend = "redis(" + redisAddr + ")"
	}
	log.Printf("ratelimitd listening on %s | algo=%s backend=%s", addr, algo, backend)
	log.Fatal(http.ListenAndServe(addr, mux))
}
