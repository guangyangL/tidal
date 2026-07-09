package leaderboard

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

type RankResp struct {
	Code    int     `json:"code"`
	Message string  `json:"message,omitempty"`
	UserID  int64   `json:"user_id,omitempty"`
	Score   float64 `json:"score,omitempty"`
	Rank    int     `json:"rank,omitempty"`
	Coarse  bool    `json:"coarse,omitempty"`
}

func (h *Handler) Rank(c *gin.Context) {
	roomID := c.Param("room_id")
	userID, err := strconv.ParseInt(c.Query("user_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, RankResp{Code: 1001, Message: "invalid user_id"})
		return
	}

	result, err := h.svc.GetRank(c.Request.Context(), roomID, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, RankResp{Code: 9001, Message: "rank unavailable"})
		return
	}
	if result == nil {
		c.JSON(http.StatusOK, RankResp{Code: 0, UserID: userID, Message: "unranked"})
		return
	}

	c.JSON(http.StatusOK, RankResp{
		Code:   0,
		UserID: result.UserID,
		Score:  result.Score,
		Rank:   result.Rank,
		Coarse: result.Coarse,
	})
}
