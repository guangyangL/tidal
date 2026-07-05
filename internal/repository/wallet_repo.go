package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

var ErrInsufficientBalance = errors.New("insufficient balance")

type WalletRepo struct {
	db *sqlx.DB
}

func NewWalletRepo(db *sqlx.DB) *WalletRepo {
	return &WalletRepo{db: db}
}

// GetBalance returns the current balance for a user.
func (r *WalletRepo) GetBalance(ctx context.Context, userID int64) (int64, error) {
	var balance int64
	err := r.db.GetContext(ctx, &balance, "SELECT balance FROM t_user_wallet WHERE user_id = ?", userID)
	if err != nil {
		return 0, fmt.Errorf("wallet %d: %w", userID, err)
	}
	return balance, nil
}

// GetVersion reads the current optimistic-lock version for a user.
func (r *WalletRepo) GetVersion(ctx context.Context, userID int64) (int, error) {
	var version int
	err := r.db.GetContext(ctx, &version, "SELECT version FROM t_user_wallet WHERE user_id = ?", userID)
	if err != nil {
		return 0, fmt.Errorf("wallet version %d: %w", userID, err)
	}
	return version, nil
}

// Deduct attempts an optimistic-lock deduction.
// Returns the new version if successful.
// Returns ErrInsufficientBalance if balance < amount (no retry needed).
// Returns sql.ErrNoRows if version conflict (safe to retry).
func (r *WalletRepo) Deduct(ctx context.Context, userID int64, amount int64, expectedVersion int) (int, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE t_user_wallet
		 SET balance = balance - ?,
		     version = version + 1
		 WHERE user_id = ? AND version = ? AND balance >= ?`,
		amount, userID, expectedVersion, amount,
	)
	if err != nil {
		return 0, fmt.Errorf("deduct wallet %d: %w", userID, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		var bal int64
		if checkErr := r.db.GetContext(ctx, &bal, "SELECT balance FROM t_user_wallet WHERE user_id = ?", userID); checkErr == nil && bal < amount {
			return 0, ErrInsufficientBalance
		}
		return 0, sql.ErrNoRows
	}
	return expectedVersion + 1, nil
}
