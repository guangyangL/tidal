package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type LeaderboardHandler struct {
}

func NewLeaderboardHandler() *LeaderboardHandler {
	return &LeaderboardHandler{}
}

type TopNReq struct {
	Top int `form:"top" binding:"omitempty,min=1,max=100"`
}

func (h *LeaderboardHandler) TopN(c *gin.Context) {
	var req TopNReq
	if req.Top == 0 {
		req.Top = 50
	}
	if err := c.ShouldBindQuery(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"code": 1001, "msg": err.Error()})
		return
	}
	// TODO
}
