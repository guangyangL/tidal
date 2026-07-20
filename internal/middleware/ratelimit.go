package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type bucket struct {
	tokens     float64
	lastRefill time.Time
}

type RateLimiter struct {
	mu       sync.Mutex
	buckets  map[string]*bucket
	rate     float64 // tokens per second
	capacity float64 // max burst
}

func NewRateLimiter(rate, capacity float64) *RateLimiter {
	return &RateLimiter{
		buckets:  make(map[string]*bucket),
		rate:     rate,
		capacity: capacity,
	}
}

// Allow checks whether the given key is allowed. Thread-safe.
func (rl *RateLimiter) Allow(key string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	b, ok := rl.buckets[key]
	if !ok {
		b = &bucket{tokens: rl.capacity, lastRefill: now}
		rl.buckets[key] = b
	}

	// refill
	elapsed := now.Sub(b.lastRefill).Seconds()
	b.tokens = min(rl.capacity, b.tokens+elapsed*rl.rate)
	b.lastRefill = now

	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

// GinMiddleware returns a per-user rate-limit middleware.
// Pass nil or rate <= 0 to disable.
func GinMiddleware(rl *RateLimiter) gin.HandlerFunc {
	if rl == nil || rl.rate <= 0 {
		return func(c *gin.Context) { c.Next() }
	}

	return func(c *gin.Context) {
		userID, exists := c.Get(CtxKeyUserID)
		if !exists {
			c.Next()
			return
		}

		if !rl.Allow(userID.(string)) {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"code": 4029,
				"msg":  "rate limit exceeded",
			})
			return
		}
		c.Next()
	}
}
