package main

import (
	"context"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"

	"github.com/guangyang/tidal/internal/cache"
	"github.com/guangyang/tidal/internal/config"
	"github.com/guangyang/tidal/internal/handler"
	"github.com/guangyang/tidal/internal/leaderboard"
	"github.com/guangyang/tidal/internal/middleware"
	"github.com/guangyang/tidal/internal/mq"
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
	dedup := cache.NewDeduplicator(rdb)
	giftCache := cache.NewGiftCache(rdb, giftRepo)
	walletCache := cache.NewWalletCache(rdb, walletRepo)

	// warm up wallet balances
	for _, uid := range []int64{1001, 1002, 1003, 2001, 3001} {
		if err := walletCache.LoadBalance(context.Background(), uid); err != nil {
			log.Printf("warmup wallet %d: %v", uid, err)
		}
	}

	// periodic Redis health check
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			walletCache.TryRecover(context.Background())
		}
	}()

	// --- MQ ---
	var mqProducer *mq.Producer
	if cfg.RabbitMQ.URL != "" {
		mqProducer, err = mq.NewProducer(cfg.RabbitMQ.URL, cfg.RabbitMQ.Exchange)
		if err != nil {
			log.Printf("mq producer: %v", err)
		}
	}

	// --- segment tree ---
	treeConf := leaderboard.NewRankConfig(1_000_000, 1024, 100)
	segTree := leaderboard.BuildSegmentTree(treeConf)

	// --- leaderboard counter + MQ consumer ---
	leaderboardCounter := leaderboard.NewCounter(rdb, mqProducer)

	if mqProducer != nil {
		lbConsumer, err := leaderboard.StartConsumer(cfg.RabbitMQ.URL, rdb, segTree)
		if err != nil {
			log.Printf("leaderboard mq consumer: %v", err)
		}
		_ = lbConsumer

		// settlement consumer: reuses flusher logic via MQ
		settleConsumer := service.NewSettleConsumer(walletRepo, recordRepo, walletCache)
		settleMQ, err := mq.NewConsumer(
			cfg.RabbitMQ.URL, cfg.RabbitMQ.Exchange,
			"tidal.settle.queue", "gift.settle",
			settleConsumer.Handle,
		)
		if err != nil {
			log.Printf("settle mq consumer: %v", err)
		} else {
			settleMQ.Start()
			go settleConsumer.FlushLoop(context.Background())
		}
	}

	// --- services ---
	giftSvc := service.NewGiftService(rdb, dedup, giftCache, walletCache, leaderboardCounter, mqProducer)
	lbSvc := leaderboard.NewService(rdb, segTree)

	// --- handlers ---
	giftH := handler.NewGiftHandler(giftSvc)
	lbH := leaderboard.NewHandler(lbSvc)

	// --- router ---
	r := gin.Default()
	r.Use(middleware.Auth())

	api := r.Group("/api/v1")
	{
		api.POST("/gift/send", giftH.Send)
		api.GET("/room/:room_id/leaderboard", lbH.TopN)
		api.GET("/room/:room_id/rank", lbH.Rank)
	}

	log.Printf("Tidal starting on %s", cfg.Server.Addr)
	if err := r.Run(cfg.Server.Addr); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
