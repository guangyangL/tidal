package main

import (
	"log"

	"github.com/gin-gonic/gin"

	"github.com/guangyang/tidal/internal/config"
	"github.com/guangyang/tidal/internal/handler"
	"github.com/guangyang/tidal/internal/middleware"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	r := gin.Default()
	r.Use(middleware.Auth())

	giftH := handler.NewGiftHandler()
	lbH := handler.NewLeaderboardHandler()

	api := r.Group("/api/v1")
	{
		api.POST("/gift/send", giftH.Send)
		api.GET("/room/:room_id/leaderboard", lbH.TopN)
	}

	log.Printf("Tidal starting on %s", cfg.Server.Addr)
	if err := r.Run(cfg.Server.Addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
