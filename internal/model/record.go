package model

import "time"

const (
	RecordStatusDeducted   int8 = 1 // 已扣款待分账
	RecordStatusSettled    int8 = 2 // 分账成功
	RecordStatusRetrying   int8 = 3 // 失败待重试
	RecordStatusDeadLetter int8 = 4 // 死信，人工处理
)

type GiftRecord struct {
	ID          int64     `db:"id"`
	BatchToken  string    `db:"batch_token"`
	RoomID      int64     `db:"room_id"`
	UserID      int64     `db:"user_id"`
	AnchorID    int64     `db:"anchor_id"`
	GiftID      int       `db:"gift_id"`
	TotalAmount int64     `db:"total_amount"`
	Status      int8      `db:"status"`
	CreateTime  time.Time `db:"create_time"`
}
