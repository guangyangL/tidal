package cache

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type LeaderboardCache struct {
	rdb *redis.Client
}

func NewLeaderboardCache(rdb *redis.Client) *LeaderboardCache {
	return &LeaderboardCache{rdb: rdb}
}

func (c *LeaderboardCache) Add(ctx context.Context, roomID, userID int64, amount float64) error {
	// TODO: ZINCRBY room:leaderboard:{roomID}
	return nil
}

func (c *LeaderboardCache) TopN(ctx context.Context, roomID int64, n int) ([]redis.Z, error) {
	// TODO: ZREVRANGE room:leaderboard:{roomID} 0 n-1 WITHSCORES
	return nil, nil
}
