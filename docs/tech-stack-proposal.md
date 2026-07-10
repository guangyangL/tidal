# 技术选型与项目结构

> 本文列出各层级的实际选型，标注理由。

---

## 一、 技术选型

### 1.1 语言与运行时

| 项 | 选择 | 理由 |
|---|---|---|
| **Go** | 1.25 | 当前开发版本，goroutine + Channel 天然适合并发削峰 |
| **Go Modules** | 标准 | 依赖管理 |

### 1.2 HTTP 框架

| 方案 | 推荐 |
|---|---|
| **Gin** | **选用**。中间件生态丰富（中间件链、参数绑定），面试最常见 |

### 1.3 MySQL 访问层

| 方案 | 推荐 |
|---|---|
| **sqlx** | **选用**。StructScan、NamedParam 比 raw sql 方便，又不遮掩 SQL 细节 |

### 1.4 Redis 客户端

| 方案 | 推荐 |
|---|---|
| **go-redis v9** | **选用**。Pipeline、Lua 脚本原生支持，社区最广 |

### 1.5 消息队列

| 方案 | 推荐 |
|---|---|
| **RabbitMQ** (amqp091-go) | **选用**。Direct 交换器 + 持久化队列，Phase 1~2 够用 |

### 1.6 其他基础设施

| 组件 | 选型 | 备注 |
|---|---|---|
| ID 生成器 | **自实现 Sonyflake** | 基于 MAC 地址自动定 workerId，epoch=2026-01-01 |
| 配置加载 | **Viper** | YAML 配置文件 + 环境变量覆盖（TIDAL_ 前缀） |
| 日志 | **标准 log** | 当前阶段够用，后续可切 zap |
| Docker | Compose | MySQL 8.0 + Redis 7 + RabbitMQ 3 编排 |

---

## 二、 目录结构

```
cmd/server/main.go          # 启动入口：依赖注入 → 启动 HTTP
config/config.yaml           # YAML 配置

internal/
├── cache/                   # Redis 访问层
│   ├── dedup.go             #   幂等去重（SETNX）
│   ├── gift.go              #   礼物配置缓存（HGET → MySQL fallback）
│   └── wallet.go            #   钱包预扣（Lua 原子扣减）
├── config/
│   └── config.go            #   Viper 配置加载 + 环境变量
├── event/
│   └── types.go             #   MQ 事件结构体（GiftSettleEvent, ChangeCounterTriggerEvent）
├── handler/
│   └── gift.go              #   POST /api/v1/gift/send
├── leaderboard/
│   ├── counter.go           #   排行榜计数器（ZINCRBY + MQ 发布）
│   ├── consumer.go          #   MQ 消费者 → 线段树更新
│   ├── handler_topn.go      #   GET /room/:id/leaderboard
│   ├── handler_rank.go      #   GET /room/:id/rank
│   ├── redis.go             #   线段树 Redis Lua 操作
│   ├── service.go           #   排行榜查询服务
│   ├── tree.go              #   线段树内存结构
│   └── types.go             #   RankConfig
├── middleware/
│   └── auth.go              #   X-User-ID header → gin.Context
├── model/
│   ├── gift.go              #   GiftConfig
│   ├── record.go            #   GiftRecord
│   └── wallet.go            #   UserWallet
├── mq/
│   ├── connect.go           #   RabbitMQ 连接 + Exchange 声明
│   ├── consumer.go          #   通用 Consumer（QueueDeclare + Bind + Consume）
│   └── producer.go          #   通用 Producer（PublishWithContext）
├── repository/
│   ├── gift_repo.go         #   礼物配置查询
│   ├── record_repo.go       #   流水写入（ON DUPLICATE KEY 幂等）
│   └── wallet_repo.go       #   钱包扣减（CAS 乐观锁）
└── service/
    ├── gift_service.go      #   送礼主流程编排
    └── settle.go            #   Settle Consumer（100ms 批量 + 分组 + CAS 扣减）

pkg/
├── idgen/
│   └── idgen.go             #   Sonyflake ID 生成 + base62 编码
└── token/
    └── token.go             #   batch_token 编码（user+anchor+window, 21 char）

deploy/
├── Dockerfile               #   多阶段构建
└── docker-compose.yml       #   MySQL + Redis + RabbitMQ + App

scripts/
├── sql/                      #   DDL 参考 + 种子数据
├── gen_targets.go            #   压测目标生成器
└── loadtest.sh               #   压测脚本
```

---

## 三、 关键数据结构

### 3.1 领域模型 (model/)

```go
// wallet.go
type UserWallet struct {
    UserID     int64     `db:"user_id"`
    Balance    int64     `db:"balance"`       // 分，避免浮点
    WalletType int8      `db:"wallet_type"`   // 0 充值币 / 1 赠送币
    Version    int       `db:"version"`       // 乐观锁
    UpdateTime time.Time `db:"update_time"`
}

// gift.go
type GiftConfig struct {
    GiftID     int       `db:"gift_id"`
    Name       string    `db:"name"`
    Price      int64     `db:"price"`
    Status     int8      `db:"status"`
    Extra      *string   `db:"extra"` // JSON string, nullable
    CreateTime time.Time `db:"create_time"`
}

// record.go
type GiftRecord struct {
    ID          int64     `db:"id"`
    BatchToken  string    `db:"batch_token"`
    RoomID      int64     `db:"room_id"`
    UserID      int64     `db:"user_id"`
    AnchorID    int64     `db:"anchor_id"`
    GiftID      int       `db:"gift_id"`
    TotalAmount int64     `db:"total_amount"`
    Status      int8      `db:"status"`  // 1-已扣款, 2-分账成功, 3-待重试, 4-死信
    CreateTime  time.Time `db:"create_time"`
}
```

### 3.2 MQ 事件 (event/)

```go
// GiftSettleEvent — GiftService 发布, SettleConsumer 消费
type GiftSettleEvent struct {
    UserID   int64 `json:"user_id"`
    AnchorID int64 `json:"anchor_id"`
    GiftID   int64 `json:"gift_id"`
    RoomID   int64 `json:"room_id"`
    Price    int64 `json:"price"`
}

// ChangeCounterTriggerEvent — Counter 发布, Leaderboard Consumer 消费
type ChangeCounterTriggerEvent struct {
    KeyPrefix  string `json:"key_prefix"`
    UserID     int64  `json:"user_id"`
    DeltaScore int    `json:"delta_score"`
    Score      int    `json:"score"`
}
```

### 3.3 线段树 (leaderboard/)

```go
type RankConfig struct {
    MaxScore     int  // 分数范围上限, 默认 1,000,000
    BucketNumber int  // 桶数量, 默认 1024
    TopK         int  // ZSet 保留 Top N, 默认 100
}

type SegmentNode struct {
    lower, upper int
    left, right  *SegmentNode
    count        int
}
```
