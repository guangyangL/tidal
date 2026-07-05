package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/guangyang/tidal/internal/service"
)

type GiftHandler struct {
	svc *service.GiftService
}

func NewGiftHandler(svc *service.GiftService) *GiftHandler {
	return &GiftHandler{svc: svc}
}

type SendGiftReq struct {
	RoomID   int64 `json:"room_id"   binding:"required"`
	AnchorID int64 `json:"anchor_id" binding:"required"`
	GiftID   int64 `json:"gift_id"   binding:"required"`
	ComboSeq int64 `json:"combo_seq" binding:"required"`
}

type SendGiftResp struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

func (h *GiftHandler) Send(c *gin.Context) {
	userIDStr, _ := c.Get("user_id")
	userID, err := strconv.ParseInt(userIDStr.(string), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, SendGiftResp{Code: 1001, Message: "invalid user id"})
		return
	}

	var req SendGiftReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, SendGiftResp{Code: 1001, Message: err.Error()})
		return
	}

	result, err := h.svc.SendGift(c.Request.Context(), userID, req.RoomID, req.AnchorID, req.GiftID, req.ComboSeq)
	if err != nil {
		c.JSON(http.StatusInternalServerError, SendGiftResp{Code: 9001, Message: err.Error()})
		return
	}

	switch result {
	case service.SendOK:
		c.JSON(http.StatusOK, SendGiftResp{Code: 0, Message: "ok"})
	case service.SendDuplicate:
		c.JSON(http.StatusConflict, SendGiftResp{Code: 3001, Message: "duplicate request"})
	case service.SendGiftNotFound:
		c.JSON(http.StatusNotFound, SendGiftResp{Code: 2002, Message: "gift not found"})
	case service.SendInsufficientBalance:
		c.JSON(http.StatusPaymentRequired, SendGiftResp{Code: 2001, Message: "insufficient balance"})
	case service.SendServerError:
		c.JSON(http.StatusServiceUnavailable, SendGiftResp{Code: 9002, Message: "service overloaded, retry later"})
	}
}
