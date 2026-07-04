package repository

import "github.com/jmoiron/sqlx"

type RecordRepo struct {
	db *sqlx.DB
}

func NewRecordRepo(db *sqlx.DB) *RecordRepo {
	return &RecordRepo{db: db}
}
