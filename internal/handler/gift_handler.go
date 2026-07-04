package handler

import "github.com/gin-gonic/gin"

type GiftHandler struct {
}

func NewGiftHandler() *GiftHandler {
	return &GiftHandler{}
}

type SendGiftReq struct {
	RoomID    int64 `json:"room_id"    binding:"required"`
	AnchorID  int64 `json:"anchor_id"  binding:"required"`
	GiftID    int   `json:"gift_id"    binding:"required"`
	ComboSeq  int   `json:"combo_seq"  binding:"required"` // 用于幂等去重
}

func (h *GiftHandler) Send(c *gin.Context) {
	// TODO
}
