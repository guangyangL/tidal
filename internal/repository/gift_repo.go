package repository

import "github.com/jmoiron/sqlx"

type GiftRepo struct {
	db *sqlx.DB
}

func NewGiftRepo(db *sqlx.DB) *GiftRepo {
	return &GiftRepo{db: db}
}
