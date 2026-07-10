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
	rdb  *redis.Client
	tree *SegmentNode
}

func NewMQConsumer(rdb *redis.Client, tree *SegmentNode) *MQConsumer {
	return &MQConsumer{rdb: rdb, tree: tree}
}

func (c *MQConsumer) Handle(body []byte) error {
	var msg event.ChangeCounterTriggerEvent
	if err := json.Unmarshal(body, &msg); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	ctx := context.Background()
	oldScore := msg.Score - msg.DeltaScore

	if oldScore <= 0 {
		return AddScore(ctx, c.rdb, c.tree, msg.KeyPrefix, msg.Score)
	}
	return UpdateScore(ctx, c.rdb, c.tree, msg.KeyPrefix, oldScore, msg.Score)
}

func StartConsumer(ch *amqp.Channel, rdb *redis.Client, tree *SegmentNode) (*mq.Consumer, error) {
	handler := NewMQConsumer(rdb, tree)
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
