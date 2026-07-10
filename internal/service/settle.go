package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/guangyang/tidal/internal/event"
	"github.com/guangyang/tidal/internal/model"
	"github.com/guangyang/tidal/internal/mq"
	"github.com/guangyang/tidal/internal/repository"
	"github.com/guangyang/tidal/pkg/token"
)

type settleGroup struct {
	userID     int64
	anchorID   int64
	giftID     int64
	roomID     int64
	totalPrice int64
}

type WalletBalanceSyncer interface {
	LoadBalance(ctx context.Context, userID int64) error
	SyncBalance(ctx context.Context, userID int64) error
}

type SettleConsumer struct {
	db          *sqlx.DB
	walletRepo  *repository.WalletRepo
	recordRepo  *repository.RecordRepo
	walletCache WalletBalanceSyncer

	mu     sync.Mutex
	buffer []event.GiftSettleEvent
}

func NewSettleConsumer(
	db *sqlx.DB,
	walletRepo *repository.WalletRepo,
	recordRepo *repository.RecordRepo,
	walletCache WalletBalanceSyncer,
) *SettleConsumer {
	return &SettleConsumer{
		db:          db,
		walletRepo:  walletRepo,
		recordRepo:  recordRepo,
		walletCache: walletCache,
		buffer:      make([]event.GiftSettleEvent, 0, 4096),
	}
}

func StartSettleConsumer(
	ch *amqp.Channel, exchange string,
	db *sqlx.DB,
	walletRepo *repository.WalletRepo,
	recordRepo *repository.RecordRepo,
	walletCache WalletBalanceSyncer,
) (*mq.Consumer, error) {
	sc := NewSettleConsumer(db, walletRepo, recordRepo, walletCache)
	consumer, err := mq.NewConsumer(ch, exchange, "tidal.settle.queue", "gift.settle", sc.Handle)
	if err != nil {
		return nil, err
	}
	if err := consumer.Start(); err != nil {
		return nil, err
	}
	go sc.FlushLoop(context.Background())
	log.Print("settle mq consumer started")
	return consumer, nil
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
		g.totalPrice += e.Price
	}

	for _, g := range groups {
		if g.totalPrice == 0 {
			continue
		}
		batchToken := token.Encode(g.userID, g.anchorID, time.Now().UnixMilli()/100*100)
		record := &model.GiftRecord{
			BatchToken:  batchToken,
			RoomID:      g.roomID,
			UserID:      g.userID,
			AnchorID:    g.anchorID,
			GiftID:      int(g.giftID),
			TotalAmount: g.totalPrice,
			Status:      model.RecordStatusDeducted,
		}
		if err := c.deductAndInsert(ctx, g.userID, g.totalPrice, record); err != nil {
			log.Printf("settle deduct user=%d amount=%d: %v", g.userID, g.totalPrice, err)
			continue
		}
		if err := c.walletCache.SyncBalance(ctx, g.userID); err != nil {
			log.Printf("settle sync redis user=%d: %v", g.userID, err)
		}
	}
}

// deductAndInsert wraps wallet CAS deduct and record insert in a single MySQL transaction.
// SyncBalance is intentionally outside the transaction — Redis cannot participate in 2PC
// but inconsistency is recoverable via LoadBalance.
func (c *SettleConsumer) deductAndInsert(ctx context.Context, userID int64, amount int64, record *model.GiftRecord) error {
	for i := range 3 {
		tx, err := c.db.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}

		var version int
		if err := tx.GetContext(ctx, &version, "SELECT version FROM t_user_wallet WHERE user_id = ?", userID); err != nil {
			tx.Rollback()
			return fmt.Errorf("get version: %w", err)
		}

		res, err := tx.ExecContext(ctx,
			`UPDATE t_user_wallet
			 SET balance = balance - ?,
			     version = version + 1
			 WHERE user_id = ? AND version = ? AND balance >= ?`,
			amount, userID, version, amount,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("deduct: %w", err)
		}

		rows, _ := res.RowsAffected()
		if rows == 0 {
			tx.Rollback()
			var bal int64
			if err := c.db.GetContext(ctx, &bal, "SELECT balance FROM t_user_wallet WHERE user_id = ?", userID); err == nil && bal < amount {
				return repository.ErrInsufficientBalance
			}
			time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
			continue
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO t_gift_record
			 (batch_token, room_id, user_id, anchor_id, gift_id, total_amount, status)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON DUPLICATE KEY UPDATE id=id`,
			record.BatchToken, record.RoomID, record.UserID, record.AnchorID,
			record.GiftID, record.TotalAmount, record.Status,
		)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("insert record: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}
		return nil
	}
	return fmt.Errorf("deduct retry exhausted for user %d", userID)
}

func settleHash(userID, anchorID, giftID, roomID int64) uint64 {
	h := uint64(userID)
	h ^= uint64(anchorID) * 0x9e3779b97f4a7c15
	h ^= uint64(giftID) * 0xbf58476d1ce4e5b9
	h ^= uint64(roomID) * 0x9e3779b97f4a7c15
	return h
}
