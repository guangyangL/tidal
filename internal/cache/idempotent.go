package cache

import (
	"fmt"
	"time"
)

// IdempotentCacheInmem is a simple in-memory idempotent cache implementation.
type IdempotentCacheInmem struct {
	cache map[string]time.Time
}

// NewIdempotentCacheInmem creates a new instance of IdempotentCacheInmem.
func NewIdempotentCacheInmem() *IdempotentCacheInmem {
	return &IdempotentCacheInmem{
		cache: make(map[string]time.Time),
	}
}

// SETNX adds a key to the cache if it doesn't already exist. It returns true if the key was added, false if it already existed.
// key "idempotent:{user_id}:{combo_seq}" 5s 过期
func (c *IdempotentCacheInmem) SETNX(key string) error {
	if exp, exists := c.cache[key]; exists && time.Since(exp) < 5*time.Second {
		return fmt.Errorf("key already exists")
	}
	c.cache[key] = time.Now()
	return nil
}
