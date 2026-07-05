package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

var ErrInsufficientBalance = errors.New("insufficient balance")

var preDeductScript = redis.NewScript(`
local balance = redis.call('GET', KEYS[1])
if not balance then
	return {-1, 0}
end
balance = tonumber(balance)
if balance < tonumber(ARGV[1]) then
	return {-2, 0}
end
redis.call('DECRBY', KEYS[1], ARGV[1])
return {0, balance - tonumber(ARGV[1])}
`)

type WalletCache struct {
	rdb     *redis.Client
	healthy atomic.Bool
	repo interface {
		GetBalance(ctx context.Context, userID int64) (int64, error)
		GetVersion(ctx context.Context, userID int64) (int, error)
		Deduct(ctx context.Context, userID int64, amount int64, expectedVersion int) (int, error)
	}
}

func NewWalletCache(rdb *redis.Client, repo interface {
	GetBalance(ctx context.Context, userID int64) (int64, error)
	GetVersion(ctx context.Context, userID int64) (int, error)
	Deduct(ctx context.Context, userID int64, amount int64, expectedVersion int) (int, error)
}) *WalletCache {
	wc := &WalletCache{rdb: rdb, repo: repo}
	wc.healthy.Store(true)
	return wc
}

// IsDegraded returns true when Redis is unavailable and MySQL fallback is active.
func (c *WalletCache) IsDegraded() bool {
	return !c.healthy.Load()
}

// PreDeduct atomically checks and deducts from Redis (fast path).
//
// Degraded mode: when Redis is unreachable, PreDeduct falls back to
// synchronous MySQL CAS deduction. This ensures every hit does a real
// balance check, so the WebSocket broadcast in GiftService is never
// inconsistent — if PreDeduct succeeds, the money is already held.
func (c *WalletCache) PreDeduct(ctx context.Context, userID int64, amount int64) error {
	if !c.healthy.Load() {
		return c.syncDeduct(ctx, userID, amount)
	}

	if c.rdb == nil {
		return c.syncDeduct(ctx, userID, amount)
	}

	key := walletKey(userID)
	for i := 0; i < 2; i++ {
		vals, err := preDeductScript.Run(ctx, c.rdb, []string{key}, amount).Result()
		if err != nil {
			c.healthy.Store(false)
			log.Printf("redis unavailable, enter degraded mode: %v", err)
			return c.syncDeduct(ctx, userID, amount)
		}
		arr := vals.([]interface{})
		code := int(arr[0].(int64))
		switch code {
		case 0:
			return nil
		case -1:
			bal, err := c.repo.GetBalance(ctx, userID)
			if err != nil {
				return fmt.Errorf("load balance: %w", err)
			}
			c.rdb.Set(ctx, key, bal, 0)
			continue
		case -2:
			return ErrInsufficientBalance
		}
	}
	return ErrInsufficientBalance
}

// syncDeduct directly deducts from MySQL with optimistic locking.
// Called in degraded mode or when Redis key is not cached.
func (c *WalletCache) syncDeduct(ctx context.Context, userID int64, amount int64) error {
	version, err := c.repo.GetVersion(ctx, userID)
	if err != nil {
		return err
	}
	_, err = c.repo.Deduct(ctx, userID, amount, version)
	return err
}

// TryRecover pings Redis and re-enables pre-deduct when it recovers.
// Stale balances are cleared so the next PreDeduct reloads from MySQL.
func (c *WalletCache) TryRecover(ctx context.Context) {
	if c.healthy.Load() || c.rdb == nil {
		return
	}
	if err := c.rdb.Ping(ctx).Err(); err != nil {
		return
	}
	if err := c.rdb.FlushDB(ctx).Err(); err != nil {
		log.Printf("redis recover flush: %v", err)
		return
	}
	c.healthy.Store(true)
	log.Print("redis recovered, exit degraded mode")
}

// GetBalance returns the user's balance from Redis (if available) or MySQL.
func (c *WalletCache) GetBalance(ctx context.Context, userID int64) (int64, error) {
	if c.rdb != nil {
		bal, err := c.rdb.Get(ctx, walletKey(userID)).Int64()
		if err == nil {
			return bal, nil
		}
	}
	return c.repo.GetBalance(ctx, userID)
}

// LoadBalance writes a user's MySQL balance into Redis.
func (c *WalletCache) LoadBalance(ctx context.Context, userID int64) error {
	if c.rdb == nil {
		return nil
	}
	bal, err := c.repo.GetBalance(ctx, userID)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, walletKey(userID), bal, 0).Err()
}

func walletKey(userID int64) string {
	return fmt.Sprintf("wallet:balance:%d", userID)
}
