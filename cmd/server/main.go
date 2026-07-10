package main

import (
	"log"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	amqp "github.com/rabbitmq/amqp091-go"
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

	// auto-migrate tables
	autoMigrate(db)

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

	// --- MQ ---
	var (
		mqProducer *mq.Producer
		mqConn     *amqp.Connection
	)
	if cfg.RabbitMQ.URL != "" {
		conn, closeFunc, err := mq.Connect(cfg.RabbitMQ.URL, cfg.RabbitMQ.Exchange)
		if err != nil {
			log.Fatalf("mq connect: %v", err)
		}
		mqConn = conn
		defer closeFunc()

		prodCh, err := mqConn.Channel()
		if err != nil {
			log.Fatalf("producer channel: %v", err)
		}
		mqProducer = mq.NewProducer(prodCh, cfg.RabbitMQ.Exchange)
	}

	// --- segment tree ---
	treeConf := leaderboard.NewRankConfig(1_000_000, 1024, 100)
	segTree := leaderboard.BuildSegmentTree(treeConf)

	// --- leaderboard counter + MQ consumer ---
	leaderboardCounter := leaderboard.NewCounter(rdb, mqProducer)

	if mqProducer != nil {
		lbCh, err := mqConn.Channel()
		if err != nil {
			log.Printf("leaderboard channel: %v", err)
		} else {
			lbConsumer, err := leaderboard.StartConsumer(lbCh, rdb, segTree)
			if err != nil {
				log.Printf("leaderboard mq consumer: %v", err)
			}
			_ = lbConsumer
		}

		settleCh, err := mqConn.Channel()
		if err != nil {
			log.Printf("settle channel: %v", err)
		} else {
			settleMQ, err := service.StartSettleConsumer(settleCh, cfg.RabbitMQ.Exchange, walletRepo, recordRepo, walletCache)
			if err != nil {
				log.Printf("settle mq consumer: %v", err)
			}
			_ = settleMQ
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
