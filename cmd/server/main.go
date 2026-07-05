package main

import (
	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"

	"github.com/guangyang/tidal/internal/aggregator"
	"github.com/guangyang/tidal/internal/cache"
	"github.com/guangyang/tidal/internal/config"
	"github.com/guangyang/tidal/internal/handler"
	"github.com/guangyang/tidal/internal/middleware"
	"github.com/guangyang/tidal/internal/repository"
	"github.com/guangyang/tidal/internal/service"
)

func main() {
	cfg, err := config.Load("")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// --- MySQL ---
	db, err := sqlx.Connect("mysql", cfg.MySQL.DSN)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	db.SetMaxOpenConns(cfg.MySQL.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MySQL.MaxIdleConns)

	// --- Redis ---
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	// --- repositories ---
	giftRepo := repository.NewGiftRepo(db)
	walletRepo := repository.NewWalletRepo(db)
	recordRepo := repository.NewRecordRepo(db)

	// --- caches ---
	idempCache := cache.NewIdempotentCacheInmem()
	giftCache := cache.NewGiftCache(rdb, giftRepo)
	walletCache := cache.NewWalletCache(rdb, walletRepo)
	lbCache := cache.NewLeaderboardCache(rdb)

	// warm up wallet balances in Redis
	for _, uid := range []int64{1001, 1002, 1003, 2001, 3001} {
		if err := walletCache.LoadBalance(context.Background(), uid); err != nil {
			log.Printf("warmup wallet %d: %v", uid, err)
		}
	}

	// periodic Redis health check for degraded-mode recovery
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			walletCache.TryRecover(context.Background())
		}
	}()

	// --- aggregator + flusher ---
	agg := aggregator.NewAggregator(cfg.Aggregator.WindowTTL, cfg.Aggregator.NumShards, cfg.Aggregator.MaxComboCount, cfg.Aggregator.MaxWindowsPerShard)
	agg.StartGC()

	flushHandler := service.NewFlusherHandler(walletRepo, recordRepo, lbCache, walletCache)
	flusher := aggregator.NewFlusher(agg, flushHandler.Handle, 4)
	flusher.Start()

	// --- services ---
	giftSvc := service.NewGiftService(agg, idempCache, giftCache, walletCache)

	// --- handlers ---
	giftH := handler.NewGiftHandler(giftSvc)
	lbH := handler.NewLeaderboardHandler(lbCache)

	// --- router ---
	r := gin.Default()
	r.Use(middleware.Auth())

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
