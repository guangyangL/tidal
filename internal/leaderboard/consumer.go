package leaderboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"

	"github.com/guangyang/tidal/internal/event"
	"github.com/guangyang/tidal/internal/mq"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MQConsumer struct {
	rdb    *redis.Client
	tree   *SegmentNode
	topK   int
	msgCnt int
}

func NewMQConsumer(rdb *redis.Client, tree *SegmentNode, topK int) *MQConsumer {
	return &MQConsumer{rdb: rdb, tree: tree, topK: topK}
}

func (c *MQConsumer) Handle(body []byte) error {
	var msg event.ChangeCounterTriggerEvent
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	ctx := context.Background()
	oldScore := msg.Score - msg.DeltaScore

	// 1. Segment tree update
	if oldScore <= 0 {
		if err := AddScore(ctx, c.rdb, c.tree, msg.KeyPrefix, msg.Score); err != nil {
			return err
		}
	} else {
		if err := UpdateScore(ctx, c.rdb, c.tree, msg.KeyPrefix, oldScore, msg.Score); err != nil {
			return err
		}
	}

	// 2. ZSet update — ZADD with latest score instead of ZINCRBY on hot path
	zsetKey := fmt.Sprintf("room:leaderboard:%s", msg.KeyPrefix)
	if err := c.rdb.ZAdd(ctx, zsetKey, redis.Z{
		Score:  float64(msg.Score),
		Member: fmt.Sprintf("%d", msg.UserID),
	}).Err(); err != nil {
		return fmt.Errorf("zadd: %w", err)
	}

	// 3. Periodic pruning — keep topK elements to prevent big key
	c.msgCnt++
	if c.msgCnt%100 == 0 {
		c.rdb.ZRemRangeByRank(ctx, zsetKey, 0, int64(-(c.topK + 1)))
	}

	return nil
}

func StartConsumer(ch *amqp.Channel, rdb *redis.Client, tree *SegmentNode, topK int) (*mq.Consumer, error) {
	handler := NewMQConsumer(rdb, tree, topK)
	consumer, err := mq.NewConsumer(ch, "tidal.settle", "tidal.leaderboard", "gift.leaderboard", handler.Handle)
	if err != nil {
		return nil, err
	}
	if err := consumer.Start(); err != nil {
		return nil, err
	}
	log.Print("leaderboard mq consumer started")
	return consumer, nil
}
