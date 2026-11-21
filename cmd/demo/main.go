// Command demo serves an HTTP endpoint behind a configurable rate limiter.
//
//	go run ./cmd/demo -algo tokenbucket -rate 5 -burst 10
//	go run ./cmd/demo -algo sliding -limit 10 -window 1s -redis localhost:6379
//
// Then hammer it and watch the 200s turn into 429s:
//
//	for i in $(seq 1 20); do curl -s -o /dev/null -w "%{http_code} " localhost:8080/; done; echo
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	ratelimit "github.com/trickster03/floodgate"

	"github.com/redis/go-redis/v9"
)

func main() {
	var (
		addr    = flag.String("addr", ":8080", "listen address")
		algo    = flag.String("algo", "tokenbucket", "tokenbucket | sliding")
		rate    = flag.Float64("rate", 5, "token refill per second (tokenbucket)")
		burst   = flag.Int("burst", 10, "bucket capacity (tokenbucket)")
		limit   = flag.Int("limit", 10, "max requests per window (sliding)")
		window  = flag.Duration("window", time.Second, "window duration (sliding)")
		redisAd = flag.String("redis", "", "redis address e.g. localhost:6379 (empty = in-process)")
		header  = flag.String("header", "", "limit per this header instead of per IP, e.g. X-API-Key")
	)
	flag.Parse()

	var rdb *redis.Client
	if *redisAd != "" {
		rdb = redis.NewClient(&redis.Options{Addr: *redisAd})
		if err := rdb.Ping(context.Background()).Err(); err != nil {
			log.Fatalf("redis: %v", err)
		}
	}

	var lim ratelimit.Limiter
	switch *algo {
	case "tokenbucket":
		if rdb != nil {
			lim = ratelimit.NewRedisTokenBucket(rdb, *rate, *burst, "rl:tb:")
		} else {
			lim = ratelimit.NewTokenBucket(*rate, *burst)
		}
	case "sliding":
		if rdb != nil {
			lim = ratelimit.NewRedisSlidingWindow(rdb, *limit, *window, "rl:sw:")
		} else {
			lim = ratelimit.NewSlidingWindow(*limit, *window)
		}
	default:
		log.Fatalf("unknown -algo %q (want tokenbucket or sliding)", *algo)
	}

	key := ratelimit.KeyByIP
	if *header != "" {
		key = ratelimit.KeyByHeader(*header)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok") // not rate limited
	})

	limited := ratelimit.Middleware(lim, key)
	mux.Handle("/", limited(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})))

	backend := "in-process"
	if rdb != nil {
		backend = "redis(" + *redisAd + ")"
	}
	log.Printf("listening on %s | algo=%s backend=%s", *addr, *algo, backend)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
