package model

import "time"

type UserWallet struct {
	UserID       int64     `db:"user_id"`
	Balance      int64     `db:"balance"`
	FrozenAmount int64     `db:"frozen_amount"`
	WalletType   int8      `db:"wallet_type"`
	Version      int       `db:"version"`
	UpdateTime   time.Time `db:"update_time"`
}
