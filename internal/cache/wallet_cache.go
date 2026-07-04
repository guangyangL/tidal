package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type WalletCache struct {
	rdb *redis.Client
}

func NewWalletCache(rdb *redis.Client) *WalletCache {
	return &WalletCache{rdb: rdb}
}

func (c *WalletCache) PreDeduct(ctx context.Context, userID int64, amount int64) (bool, error) {
	// TODO: Lua script atomic check + decrement
	return false, nil
}

func (c *WalletCache) Release(ctx context.Context, userID int64, amount int64) error {
	// TODO: unfreeze
	return nil
}
