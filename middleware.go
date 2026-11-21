package ratelimit

import (
	"net"
	"net/http"
	"strconv"
	"strings"
)

// KeyFunc derives the limiter key from a request. Different keys are limited
// independently (per IP, per API key, per user, ...).
type KeyFunc func(*http.Request) string

// KeyByIP keys on the client IP, honouring the first hop of X-Forwarded-For
// when present (set one only if you trust your proxy to write it).
func KeyByIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return "ip:" + strings.TrimSpace(xff[:i])
		}
		return "ip:" + strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	return "ip:" + host
}

// KeyByHeader keys on a request header (e.g. "X-API-Key"), falling back to the
// client IP when the header is absent, so an unauthenticated caller is still
// limited rather than unlimited.
func KeyByHeader(header string) KeyFunc {
	return func(r *http.Request) string {
		if v := r.Header.Get(header); v != "" {
			return "key:" + v
		}
		return KeyByIP(r)
	}
}

// Middleware wraps a handler, allowing or rejecting each request through l.
//
// On an allowed request it sets the X-RateLimit-* headers and calls the next
// handler. On a denial it writes 429 with Retry-After. If the limiter itself
// errors (e.g. Redis is unreachable) it fails open — the request is served
// rather than dropped — which keeps an outage in the limiter from taking the
// whole service down. Swap this for fail-closed if correctness beats
// availability for your endpoint.
func Middleware(l Limiter, key KeyFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d, err := l.Allow(r.Context(), key(r))
			if err != nil {
				next.ServeHTTP(w, r) // fail open
				return
			}

			h := w.Header()
			h.Set("X-RateLimit-Limit", strconv.Itoa(d.Limit))
			h.Set("X-RateLimit-Remaining", strconv.Itoa(d.Remaining))
			if d.ResetAfter > 0 {
				h.Set("X-RateLimit-Reset", strconv.Itoa(int(d.ResetAfter.Seconds()+0.999)))
			}

			if !d.Allowed {
				if d.RetryAfter > 0 {
					secs := int(d.RetryAfter.Seconds() + 0.999)
					if secs < 1 {
						secs = 1
					}
					h.Set("Retry-After", strconv.Itoa(secs))
				}
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
