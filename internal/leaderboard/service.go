package leaderboard

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type RankResult struct {
	UserID int64
	Score  float64
	Rank   int
	Coarse bool
}

type Service struct {
	rdb  *redis.Client
	tree *SegmentNode
}

func NewService(rdb *redis.Client, tree *SegmentNode) *Service {
	return &Service{rdb: rdb, tree: tree}
}

// GetRank returns a user's rank: exact from ZSet if available, coarse from segment tree otherwise.
func (s *Service) GetRank(ctx context.Context, roomID string, userID int64) (*RankResult, error) {
	key := fmt.Sprintf("room:leaderboard:%s", roomID)
	member := fmt.Sprintf("%d", userID)

	// 1. Exact rank from ZSet
	score, err := s.rdb.ZScore(ctx, key, member).Result()
	if err == nil {
		rank, err := s.rdb.ZRevRank(ctx, key, member).Result()
		if err == nil {
			return &RankResult{
				UserID: userID,
				Score:  score,
				Rank:   int(rank) + 1,
			}, nil
		}
	}

	if !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("zset: %w", err)
	}

	// 2. Not in ZSet → coarse estimate from segment tree
	counterKey := fmt.Sprintf("counter:%s:%d", roomID, userID)
	total, cerr := s.rdb.Get(ctx, counterKey).Int64()
	if cerr != nil || total == 0 {
		return nil, nil // unranked
	}

	higher, err := GetRank(ctx, s.rdb, s.tree, roomID, int(total))
	if err != nil {
		return nil, fmt.Errorf("segment tree: %w", err)
	}
	return &RankResult{
		UserID: userID,
		Score:  float64(total),
		Rank:   higher + 1,
		Coarse: true,
	}, nil
}

// GetTopN returns the top N users from the ZSet.
func (s *Service) GetTopN(ctx context.Context, roomID string, n int) ([]RankItem, error) {
	key := fmt.Sprintf("room:leaderboard:%s", roomID)
	zs, err := s.rdb.ZRevRangeWithScores(ctx, key, 0, int64(n-1)).Result()
	if err != nil {
		return nil, err
	}

	data := make([]RankItem, 0, len(zs))
	for _, z := range zs {
		uid, _ := strconv.ParseInt(z.Member.(string), 10, 64)
		data = append(data, RankItem{UserID: uid, Score: z.Score})
	}
	return data, nil
}
