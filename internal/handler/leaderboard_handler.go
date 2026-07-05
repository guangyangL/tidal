package handler

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/guangyang/tidal/internal/cache"
)

type LeaderboardHandler struct {
	lbCache *cache.LeaderboardCache
}

func NewLeaderboardHandler(lbCache *cache.LeaderboardCache) *LeaderboardHandler {
	return &LeaderboardHandler{lbCache: lbCache}
}

type TopNReq struct {
	Top int `form:"top" binding:"omitempty,min=1,max=100"`
}

type TopNResp struct {
	Code    int          `json:"code"`
	Message string       `json:"message,omitempty"`
	Data    []RankEntry  `json:"data,omitempty"`
}

type RankEntry struct {
	UserID int64   `json:"user_id"`
	Amount float64 `json:"amount"`
}

func (h *LeaderboardHandler) TopN(c *gin.Context) {
	roomIDStr := c.Param("room_id")
	roomID, err := strconv.ParseInt(roomIDStr, 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, TopNResp{Code: 1001, Message: "invalid room_id"})
		return
	}

	var req TopNReq
	req.Top = 50
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, TopNResp{Code: 1001, Message: err.Error()})
		return
	}

	entries, err := h.lbCache.TopN(c.Request.Context(), roomID, req.Top)
	if err != nil {
		c.JSON(http.StatusInternalServerError, TopNResp{Code: 9001, Message: "leaderboard unavailable"})
		return
	}

	data := make([]RankEntry, 0, len(entries))
	for _, z := range entries {
		uid, _ := strconv.ParseInt(z.Member.(string), 10, 64)
		data = append(data, RankEntry{UserID: uid, Amount: z.Score})
	}

	c.JSON(http.StatusOK, TopNResp{Code: 0, Data: data})
}
