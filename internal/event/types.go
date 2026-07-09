package event

// GiftSettleEvent is published by GiftService after PreDeduct succeeds.
// Consumed by service.SettleConsumer for batched MySQL writes.
type GiftSettleEvent struct {
	UserID     int64 `json:"user_id"`
	AnchorID   int64 `json:"anchor_id"`
	GiftID     int64 `json:"gift_id"`
	RoomID     int64 `json:"room_id"`
	Price      int64 `json:"price"`
	ComboSeq   int64 `json:"combo_seq"`
	ComboCount int   `json:"combo_count"`
}

// ChangeCounterTriggerEvent is published by Counter.AddScore.
// Consumed by leaderboard.MQConsumer for segment tree updates.
type ChangeCounterTriggerEvent struct {
	KeyPrefix  string `json:"key_prefix"`
	UserID     int64  `json:"user_id"`
	DeltaScore int    `json:"delta_score"`
	Score      int    `json:"score"`
}
