package leaderboard

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/guangyang/tidal/internal/event"
	"github.com/guangyang/tidal/internal/mq"
)

type Counter struct {
	rdb      *redis.Client
	producer *mq.Producer
}

func NewCounter(rdb *redis.Client, producer *mq.Producer) *Counter {
	return &Counter{rdb: rdb, producer: producer}
}

// AddScore atomically increments a user's score and publishes a change event.
// Called by GiftService after PreDeduct succeeds.
func (c *Counter) AddScore(ctx context.Context, roomID string, userID int64, delta int) error {
	key := fmt.Sprintf("room:leaderboard:%s", roomID)
	// Store individual score for coarse rank queries
	c.rdb.IncrBy(ctx, fmt.Sprintf("counter:%s:%d", roomID, userID), int64(delta))

	// Step 1: ZINCRBY
	newScore, err := c.rdb.ZIncrBy(ctx, key, float64(delta), fmt.Sprintf("%d", userID)).Result()
	if err != nil {
		return fmt.Errorf("zincrby: %w", err)
	}

	// Step 2: Publish change event to MQ
	event := event.ChangeCounterTriggerEvent{
		KeyPrefix:  roomID,
		UserID:     userID,
		DeltaScore: delta,
		Score:      int(newScore),
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if err := c.producer.Publish(ctx, "gift.leaderboard", body); err != nil {
		return fmt.Errorf("publish leaderboard event: %w", err)
	}
	return nil
}

// GetScore returns a user's total score from the counter.
func (c *Counter) GetScore(ctx context.Context, roomID string, userID int64) (int, error) {
	val, err := c.rdb.Get(ctx, fmt.Sprintf("counter:%s:%d", roomID, userID)).Int64()
	return int(val), err
}
