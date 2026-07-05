package repository

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/guangyang/tidal/internal/model"
)

type GiftRepo struct {
	db *sqlx.DB
}

func NewGiftRepo(db *sqlx.DB) *GiftRepo {
	return &GiftRepo{db: db}
}

func (r *GiftRepo) GetGiftByID(ctx context.Context, giftID int64) (*model.GiftConfig, error) {
	var gift model.GiftConfig
	err := r.db.GetContext(ctx, &gift, "SELECT * FROM t_gift_config WHERE gift_id = ? AND status = 1", giftID)
	if err != nil {
		return nil, fmt.Errorf("gift %d: %w", giftID, err)
	}
	return &gift, nil
}
