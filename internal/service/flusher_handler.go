package service

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"time"

	"github.com/guangyang/tidal/internal/aggregator"
	"github.com/guangyang/tidal/internal/cache"
	"github.com/guangyang/tidal/internal/model"
	"github.com/guangyang/tidal/internal/repository"
	"github.com/guangyang/tidal/pkg/token"
)

const maxRetries = 3

type FlusherHandler struct {
	walletRepo  *repository.WalletRepo
	recordRepo  *repository.RecordRepo
	lbCache     *cache.LeaderboardCache
	walletCache *cache.WalletCache
}

func NewFlusherHandler(
	walletRepo *repository.WalletRepo,
	recordRepo *repository.RecordRepo,
	lbCache *cache.LeaderboardCache,
	walletCache *cache.WalletCache,
) *FlusherHandler {
	return &FlusherHandler{
		walletRepo:  walletRepo,
		recordRepo:  recordRepo,
		lbCache:     lbCache,
		walletCache: walletCache,
	}
}

func (h *FlusherHandler) Handle(w *aggregator.ComboWindow) {
	ctx := context.Background()

	if h.walletCache.IsDegraded() {
		// degraded mode: each hit already deducted from MySQL in PreDeduct,
		// skip deduction and just write the record + leaderboard
	} else {
		if err := h.deductWithRetry(ctx, w); err != nil {
			log.Printf("flush deduct failed for user=%d amount=%d: %v", w.Key.UserID, w.TotalAmount, err)
			return
		}
		if err := h.walletCache.LoadBalance(ctx, w.Key.UserID); err != nil {
			log.Printf("flush sync redis balance failed: %v", err)
		}
	}

	record := buildRecord(w)
	if err := h.recordRepo.Insert(ctx, record); err != nil {
		log.Printf("flush insert record failed: %v", err)
		return
	}

	_ = h.lbCache.Add(ctx, w.Key.RoomID, w.Key.UserID, float64(w.TotalAmount))
}

// cas loop to handle concurrent balance updates
func (h *FlusherHandler) deductWithRetry(ctx context.Context, w *aggregator.ComboWindow) error {
	for i := 0; i < maxRetries; i++ {
		version, err := h.walletRepo.GetVersion(ctx, w.Key.UserID)
		if err != nil {
			return err
		}

		_, err = h.walletRepo.Deduct(ctx, w.Key.UserID, w.TotalAmount, version)
		if err == nil {
			return nil
		}
		if errors.Is(err, repository.ErrInsufficientBalance) {
			return err
		}
		if errors.Is(err, sql.ErrNoRows) {
			time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
			continue
		}
		return err
	}
	return sql.ErrNoRows
}

func buildRecord(w *aggregator.ComboWindow) *model.GiftRecord {
	batchToken := token.Encode(w.Key.UserID, w.Key.AnchorID, w.WindowStart.UnixMilli())
	return &model.GiftRecord{
		BatchToken:  batchToken,
		RoomID:      w.Key.RoomID,
		UserID:      w.Key.UserID,
		AnchorID:    w.Key.AnchorID,
		GiftID:      int(w.Key.GiftID),
		ComboCount:  int(w.ComboCount),
		TotalAmount: w.TotalAmount,
		Status:      model.RecordStatusDeducted,
	}
}
