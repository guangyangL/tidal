package model

import "time"

type GiftConfig struct {
	GiftID     int       `db:"gift_id"`
	Name       string    `db:"name"`
	Price      int64     `db:"price"`
	Status     int8      `db:"status"`
	Extra      *string   `db:"extra"`
	CreateTime time.Time `db:"create_time"`
}
