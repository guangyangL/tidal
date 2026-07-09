package service

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/guangyang/tidal/internal/event"
	"github.com/guangyang/tidal/internal/model"
	"github.com/guangyang/tidal/internal/repository"
	"github.com/guangyang/tidal/pkg/token"
)

type settleGroup struct {
	userID     int64
	anchorID   int64
	giftID     int64
	roomID     int64
	totalPrice int64
	totalCount int
	latestSeq  int64
}

type WalletBalanceSyncer interface {
	LoadBalance(ctx context.Context, userID int64) error
}

type SettleConsumer struct {
	walletRepo  *repository.WalletRepo
	recordRepo  *repository.RecordRepo
	walletCache WalletBalanceSyncer

	mu     sync.Mutex
	buffer []event.GiftSettleEvent
}

func NewSettleConsumer(
	walletRepo *repository.WalletRepo,
	recordRepo *repository.RecordRepo,
	walletCache WalletBalanceSyncer,
) *SettleConsumer {
	return &SettleConsumer{
		walletRepo:  walletRepo,
		recordRepo:  recordRepo,
		walletCache: walletCache,
		buffer:      make([]event.GiftSettleEvent, 0, 4096),
	}
}

func (c *SettleConsumer) Handle(body []byte) error {
	var ev event.GiftSettleEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return err
	}
	c.mu.Lock()
	c.buffer = append(c.buffer, ev)
	c.mu.Unlock()
	return nil
}

func (c *SettleConsumer) FlushLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		c.flush(ctx)
	}
}

func (c *SettleConsumer) flush(ctx context.Context) {
	c.mu.Lock()
	batch := c.buffer
	c.buffer = make([]event.GiftSettleEvent, 0, 4096)
	c.mu.Unlock()

	if len(batch) == 0 {
		return
	}

	groups := make(map[uint64]*settleGroup)
	for _, e := range batch {
		h := settleHash(e.UserID, e.AnchorID, e.GiftID, e.RoomID)
		g, ok := groups[h]
		if !ok {
			g = &settleGroup{
				userID:   e.UserID,
				anchorID: e.AnchorID,
				giftID:   e.GiftID,
				roomID:   e.RoomID,
			}
			groups[h] = g
		}
		g.totalCount += e.ComboCount
		g.totalPrice += e.Price * int64(e.ComboCount)
		if e.ComboSeq > g.latestSeq {
			g.latestSeq = e.ComboSeq
		}
	}

	for _, g := range groups {
		if g.totalCount == 0 {
			continue
		}
		if err := c.deductRetry(ctx, g.userID, int(g.totalPrice)); err != nil {
			log.Printf("settle deduct user=%d amount=%d: %v", g.userID, g.totalPrice, err)
			continue
		}
		if err := c.walletCache.LoadBalance(ctx, g.userID); err != nil {
			log.Printf("settle sync redis user=%d: %v", g.userID, err)
		}
		batchToken := token.Encode(g.userID, g.anchorID, time.Now().UnixMilli()/100*100)
		record := &model.GiftRecord{
			BatchToken:  batchToken,
			RoomID:      g.roomID,
			UserID:      g.userID,
			AnchorID:    g.anchorID,
			GiftID:      int(g.giftID),
			ComboCount:  g.totalCount,
			TotalAmount: g.totalPrice,
			Status:      model.RecordStatusDeducted,
		}
		if err := c.recordRepo.Insert(ctx, record); err != nil {
			log.Printf("settle insert record user=%d: %v", g.userID, err)
		}
	}
}

func (c *SettleConsumer) deductRetry(ctx context.Context, userID int64, amount int) error {
	for i := range 3 {
		version, err := c.walletRepo.GetVersion(ctx, userID)
		if err != nil {
			return err
		}
		_, err = c.walletRepo.Deduct(ctx, userID, int64(amount), version)
		if err == nil {
			return nil
		}
		if err == repository.ErrInsufficientBalance {
			return err
		}
		time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
	}
	return nil
}

func settleHash(userID, anchorID, giftID, roomID int64) uint64 {
	h := uint64(userID)
	h ^= uint64(anchorID) * 0x9e3779b97f4a7c15
	h ^= uint64(giftID) * 0xbf58476d1ce4e5b9
	h ^= uint64(roomID) * 0x9e3779b97f4a7c15
	return h
}
