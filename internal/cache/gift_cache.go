package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type GiftCache struct {
	rdb *redis.Client
}

func NewGiftCache(rdb *redis.Client) *GiftCache {
	return &GiftCache{rdb: rdb}
}

func (c *GiftCache) GetPrice(ctx context.Context, giftID int) (int64, error) {
	// TODO: HGET gift:config:{gift_id} price
	return 0, nil
}
