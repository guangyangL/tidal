package repository

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"

	"github.com/guangyang/tidal/internal/model"
)

type RecordRepo struct {
	db *sqlx.DB
}

func NewRecordRepo(db *sqlx.DB) *RecordRepo {
	return &RecordRepo{db: db}
}

// Insert writes a gift record. If batch_token already exists, it's a no-op.
func (r *RecordRepo) Insert(ctx context.Context, record *model.GiftRecord) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO t_gift_record
		 (batch_token, room_id, user_id, anchor_id, gift_id, combo_count, total_amount, status, extra)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON DUPLICATE KEY UPDATE id=id`,
		record.BatchToken, record.RoomID, record.UserID, record.AnchorID,
		record.GiftID, record.ComboCount, record.TotalAmount, record.Status, record.Extra,
	)
	if err != nil {
		return fmt.Errorf("insert record: %w", err)
	}
	return nil
}
