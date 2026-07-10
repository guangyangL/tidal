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
// ZSet update is deferred to the MQ consumer to reduce hot-path Redis ops.
func (c *Counter) AddScore(ctx context.Context, roomID string, userID int64, delta int) error {
	newTotal, err := c.rdb.IncrBy(ctx, fmt.Sprintf("counter:%s:%d", roomID, userID), int64(delta)).Result()
	if err != nil {
		return fmt.Errorf("incrby: %w", err)
	}

	event := event.ChangeCounterTriggerEvent{
		KeyPrefix:  roomID,
		UserID:     userID,
		DeltaScore: delta,
		Score:      int(newTotal),
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
