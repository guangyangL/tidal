package leaderboard

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	svc *Service
}

func NewHandler(svc *Service) *Handler {
	return &Handler{svc: svc}
}

type TopNResp struct {
	Code    int        `json:"code"`
	Message string     `json:"message,omitempty"`
	Data    []RankItem `json:"data,omitempty"`
}

type RankItem struct {
	UserID int64   `json:"user_id"`
	Score  float64 `json:"score"`
}

func (h *Handler) TopN(c *gin.Context) {
	roomID := c.Param("room_id")
	n := 50
	if v, err := strconv.Atoi(c.DefaultQuery("top", "50")); err == nil && v > 0 && v <= 100 {
		n = v
	}

	data, err := h.svc.GetTopN(c.Request.Context(), roomID, n)
	if err != nil {
		c.JSON(http.StatusInternalServerError, TopNResp{Code: 9001, Message: "leaderboard unavailable"})
		return
	}
	c.JSON(http.StatusOK, TopNResp{Code: 0, Data: data})
}
