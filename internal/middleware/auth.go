package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

const CtxKeyUserID = "user_id"

func Auth() gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.GetHeader("X-User-ID")
		if userID == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"code": 3001, "msg": "missing user id"})
			return
		}
		c.Set(CtxKeyUserID, userID)
		c.Next()
	}
}
