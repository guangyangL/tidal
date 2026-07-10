package cache

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/guangyang/tidal/internal/model"
)

type GiftRepository interface {
	GetGiftByID(ctx context.Context, giftID int64) (*model.GiftConfig, error)
}

type GiftCache struct {
	rdb  *redis.Client
	repo GiftRepository
}

func NewGiftCache(rdb *redis.Client, repo GiftRepository) *GiftCache {
	return &GiftCache{rdb: rdb, repo: repo}
}

// GetPrice returns the gift price, trying Redis first then falling back to MySQL.
func (c *GiftCache) GetPrice(ctx context.Context, giftID int64) (int64, error) {
	if c.rdb != nil {
		price, err := c.rdb.HGet(ctx, fmt.Sprintf("gift:config:%d", giftID), "price").Int64()
		if err == nil {
			return price, nil
		}
	}

	// fallback to MySQL
	gift, err := c.repo.GetGiftByID(ctx, giftID)
	if err != nil {
		return 0, err
	}
	return gift.Price, nil
}
