package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/guangyang/tidal/internal/cache"
	"github.com/guangyang/tidal/internal/event"
	"github.com/guangyang/tidal/internal/leaderboard"
	"github.com/guangyang/tidal/internal/mq"
)

type GiftService struct {
	rdb         *redis.Client
	dedup       *cache.Deduplicator
	giftCache   *cache.GiftCache
	walletCache *cache.WalletCache
	counter     *leaderboard.Counter
	producer    *mq.Producer
}

func NewGiftService(
	rdb *redis.Client,
	dedup *cache.Deduplicator,
	gc *cache.GiftCache,
	wc *cache.WalletCache,
	c *leaderboard.Counter,
	p *mq.Producer,
) *GiftService {
	return &GiftService{
		rdb:         rdb,
		dedup:       dedup,
		giftCache:   gc,
		walletCache: wc,
		counter:     c,
		producer:    p,
	}
}

type SendGiftResult int

const (
	SendOK SendGiftResult = iota
	SendDuplicate
	SendGiftNotFound
	SendInsufficientBalance
	SendServerError
)

type SendGiftOutput struct {
	Result     SendGiftResult
	ComboCount int
}

func (s *GiftService) SendGift(ctx context.Context, userID, roomID, anchorID, giftID, comboSeq int64) (*SendGiftOutput, error) {
	// 1. idempotent check
	if err := s.dedup.SETNX(ctx, comboKey(userID, comboSeq)); err != nil {
		return &SendGiftOutput{Result: SendDuplicate}, nil
	}

	// 2. gift price lookup
	price, err := s.giftCache.GetPrice(ctx, giftID)
	if err != nil {
		return &SendGiftOutput{Result: SendGiftNotFound}, fmt.Errorf("gift %d: %w", giftID, err)
	}

	// 3. balance pre-deduct
	if err := s.walletCache.PreDeduct(ctx, userID, price); err != nil {
		return &SendGiftOutput{Result: SendInsufficientBalance}, nil
	}

	// 4. Combo counter via Redis INCR + TTL
	comboKey := fmt.Sprintf("combo:%d:%d:%d", roomID, userID, giftID)
	comboCount, err := s.rdb.Incr(ctx, comboKey).Result()
	if err != nil {
		log.Printf("combo incr: %v", err)
		comboCount = 1
	} else {
		s.rdb.Expire(ctx, comboKey, 3*time.Second)
	}

	// 5. Leaderboard counter (ZINCRBY + segment tree MQ)
	_ = s.counter.AddScore(ctx, strconv.FormatInt(roomID, 10), userID, int(price))

	// 6. Publish settlement event to MQ
	settleEvent := event.GiftSettleEvent{
		UserID:     userID,
		AnchorID:   anchorID,
		GiftID:     giftID,
		RoomID:     roomID,
		Price:      price,
		ComboSeq:   comboSeq,
		ComboCount: int(comboCount),
	}
	body, _ := json.Marshal(settleEvent)
	_ = s.producer.Publish(ctx, "gift.settle", body)

	// TODO MQ publish for WS broadcast

	return &SendGiftOutput{Result: SendOK, ComboCount: int(comboCount)}, nil
}

func comboKey(userID, comboSeq int64) string {
	return fmt.Sprintf("idempotent:%d:%d", userID, comboSeq)
}
