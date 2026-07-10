package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

var ErrInsufficientBalance = errors.New("insufficient balance")

const (
	luaOK            = 0  // success, remaining balance in arr[1]
	luaCacheMiss     = -1 // key not cached
	luaInsufficient  = -2 // balance < amount
)

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
	redis.call('EXPIRE', KEYS[1], 3600)
	return {0, balance - tonumber(ARGV[1])}
`)

const walletKeyTTL = 3600 // seconds

type WalletRepository interface {
	GetBalance(ctx context.Context, userID int64) (int64, error)
}

type WalletCache struct {
	rdb  *redis.Client
	repo WalletRepository
}

func NewWalletCache(rdb *redis.Client, repo WalletRepository) *WalletCache {
	return &WalletCache{rdb: rdb, repo: repo}
}

// PreDeduct atomically checks and deducts from Redis via Lua.
// Returns ErrInsufficientBalance if balance < amount.
func (c *WalletCache) PreDeduct(ctx context.Context, userID int64, amount int64) error {
	key := walletKey(userID)
	vals, err := preDeductScript.Run(ctx, c.rdb, []string{key}, amount).Result()
	if err != nil {
		return fmt.Errorf("redis unavailable: %w", err)
	}

	arr := vals.([]any)
	code := int(arr[0].(int64))
	switch code {
	case luaOK:
		return nil
	case luaCacheMiss:
		// cache miss: load balance from DB, then retry once
		if err := c.LoadBalance(ctx, userID); err != nil {
			return err
		}
		vals, err := preDeductScript.Run(ctx, c.rdb, []string{key}, amount).Result()
		if err != nil {
			return fmt.Errorf("redis unavailable: %w", err)
		}
		arr := vals.([]any)
		if code := int(arr[0].(int64)); code != luaOK {
			return ErrInsufficientBalance
		}
		return nil
	case luaInsufficient:
		return ErrInsufficientBalance
	}
	return ErrInsufficientBalance
}

// GetBalance returns the user's balance from Redis, falling back to MySQL.
func (c *WalletCache) GetBalance(ctx context.Context, userID int64) (int64, error) {
	bal, err := c.rdb.Get(ctx, walletKey(userID)).Int64()
	if err == nil {
		return bal, nil
	}
	return c.repo.GetBalance(ctx, userID)
}

// LoadBalance writes a user's MySQL balance into Redis.
func (c *WalletCache) LoadBalance(ctx context.Context, userID int64) error {
	bal, err := c.repo.GetBalance(ctx, userID)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, walletKey(userID), bal, walletKeyTTL).Err()
}

func walletKey(userID int64) string {
	return fmt.Sprintf("wallet:balance:%d", userID)
}
