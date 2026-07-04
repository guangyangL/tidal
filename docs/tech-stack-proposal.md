# 技术选型与项目结构提案

> 本文列出各层级的可选方案，标注推荐项和理由，供决策。

---

## 一、 技术选型

### 1.1 语言与版本

| 项 | 选择 | 理由 |
|---|---|---|
| **Go** | 1.22+ | go 1.22 增强了 http.ServeMux，但本次仍选 Gin（中间件生态） |
| **Go Modules** | 标准 | 无需多解释 |

### 1.2 HTTP 框架

| 方案 | 性能 | 生态 | 推荐 |
|---|---|---|---|
| **Gin** | 中 | ★★★★★ | **推荐**。面试最常见，中间件丰富，文档成熟 |
| Chi | 中 | ★★★★ | 轻量，stdlib 兼容，但生态小于 Gin |
| Fiber | 高 | ★★★ | 性能最好，但 API 模仿 Express，面试官不一定熟悉 |
| std net/http | 低（手写太多） | ★★ | 不推荐，路由匹配和中间件链都要自己写 |

**选 Gin。** 面试官一定见过，你的项目不需要在框架上证明什么。

### 1.3 MySQL 访问层

| 方案 | 类型 | 推荐 |
|---|---|---|
| **sqlx** | 增强型 std sql | **推荐**。比 database/sql 更方便（StructScan、NamedParam），又不遮掩 SQL |
| sqlc | 代码生成 | 好方案，但多一层代码生成，调试略麻烦 |
| GORM | ORM | **不推荐**。面试官追问 SQL 优化时，ORM 藏了太多细节，你会答不上来 |
| raw database/sql | 原生 | 可用，但手写 Scan/反序列化啰嗦，意义不大 |

**选 sqlx。** 你写的每一句 SQL 都是面试时能展示的，ORM 在这个项目中只有劣势。

### 1.4 Redis 客户端

| 方案 | Go 版本 | 推荐 |
|---|---|---|
| **go-redis** | v9 | **推荐**。Pipeline、Cluster、Lua 脚本都支持，社区最广 |
| rueidis | v1 | 性能略高，但生态不够成熟，遇到问题不好搜 |

**选 go-redis v9。**

### 1.5 消息队列

| 方案 | 运维成本 | 推荐 |
|---|---|---|
| **RabbitMQ** | 低（Docker 单机） | **推荐**。直连交换器 + 死信队列，Phase 1~2 够用 |
| Kafka | 高（ZooKeeper/KRaft） | Phase 4 演进选项。需要分区、回溯消费时再上 |
| Redis Streams | 低 | 可用，但缺少死信和 ACK 机制，容错能力弱 |

**选 RabbitMQ。** 3 个月内不要碰 Kafka 的运维复杂度。

### 1.6 其他基础设施

| 组件 | 选型 | 备注 |
|---|---|---|
| ID 生成器 | **Sonyflake** | Go 原生实现，依赖 MaC 地址自定 workerId，无需 ZK |
| 配置加载 | **envconfig** | 轻量，环境变量注入，不引入 Viper 的复杂度 |
| 日志 | **zap** | 性能好，结构化，Lumberjack 日志轮转 |
| 测试 | **testify** | 单测 + mock，业界标配 |
| 压测 | **k6** / **wrk** | k6 脚本灵活，wrk 简单粗暴 |
| Docker | Compose | MySQL + Redis + RabbitMQ 编排 |

---

## 二、 目录结构

```
tidal-engine/
│
├── cmd/
│   └── server/
│       └── main.go                 # 启动入口：加载配置 → 组装依赖 → 启动 HTTP
│
├── internal/                       # 不对外暴露的业务代码
│   │
│   ├── config/
│   │   └── config.go               # 配置结构体 + 环境变量加载
│   │
│   ├── model/                      # 领域模型（纯 struct，无业务逻辑）
│   │   ├── wallet.go               #   UserWallet struct
│   │   ├── gift.go                 #   GiftConfig struct
│   │   └── record.go               #   GiftRecord struct
│   │
│   ├── handler/                    # HTTP 层（Gin Handlers）
│   │   ├── gift_handler.go         #   送礼 / 连击接口
│   │   └── handler_test.go         #   HTTP 层测试
│   │
│   ├── middleware/
│   │   └── auth.go                 #   Header 提取 UserID
│   │
│   ├── service/                    # 核心业务编排（事务边界在这里）
│   │   ├── gift_service.go         #   送礼主流程
│   │   ├── wallet_service.go       #   钱包扣减 + 乐观锁
│   │   └── settle_service.go       #   分账结算（MQ 消费端）
│   │
│   ├── aggregator/                 # ★ 核心：滑动窗口聚合
│   │   ├── window.go               #   SlidingWindow 结构体 + 增删改查
│   │   ├── flusher.go              #   窗口期满 → flushCh 落盘
│   │   └── aggregator_test.go      #   聚合器单元测试（并发安全验证）
│   │
│   ├── repository/                 # 数据访问层（sqlx）
│   │   ├── wallet_repo.go          #   钱包查询 / 乐观锁更新
│   │   ├── gift_repo.go            #   礼物配置查询
│   │   └── record_repo.go          #   流水写入 + 幂等处理
│   │
│   ├── cache/                      # Redis 访问层
│   │   ├── wallet_cache.go         #   余额预扣 / 查询 / 解冻
│   │   ├── gift_cache.go           #   礼物配置缓存加载与刷新
│   │   └── leaderboard.go          #   贡献榜 ZSet 写入
│   │
│   └── mq/                         # 消息队列
│       ├── producer.go             #   结算消息投递
│       └── consumer.go             #   结算消息消费 + 重试/死信
│
├── pkg/                            # 可复用的工具包
│   ├── token/
│   │   └── token.go                #   batch_token 生成（base62 编码）
│   └── idgen/
│       └── idgen.go                #   Sonyflake 全局 ID 生成
│
├── scripts/
│   └── sql/
│       ├── 001_init_schema.sql     # DDL
│       └── 002_seed_data.sql       # 测试数据（礼物配置、钱包）
│
├── docs/
│   └── technical-design.md         # 技术方案文档
│
├── deploy/
│   ├── Dockerfile                  # 多阶段构建
│   └── docker-compose.yml          # MySQL + Redis + RabbitMQ + App
│
├── Makefile                        # 常用命令
├── go.mod
└── go.sum
```

---

## 三、 关键数据结构速览

### 3.1 领域模型 (model/)

```go
// wallet.go
type UserWallet struct {
    UserID       int64     `db:"user_id"`
    Balance      int64     `db:"balance"`       // 分，避免浮点
    FrozenAmount int64     `db:"frozen_amount"` // 预扣冻结
    WalletType   int8      `db:"wallet_type"`   // 0 充值币 / 1 赠送币
    Version      int       `db:"version"`       // 乐观锁
    UpdateTime   time.Time `db:"update_time"`
}

// gift.go
type GiftConfig struct {
    GiftID     int       `db:"gift_id"`
    Name       string    `db:"name"`
    Price      int64     `db:"price"`
    Status     int8      `db:"status"`
    Extra      string    `db:"extra"` // JSON string
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
    ComboCount  int       `db:"combo_count"`
    TotalAmount int64     `db:"total_amount"`
    Status      int8      `db:"status"`
    RetryCount  int8      `db:"retry_count"`
    SettleTime  *time.Time `db:"settle_time"`
    Extra       string    `db:"extra"` // JSON string
    CreateTime  time.Time `db:"create_time"`
}
```

### 3.2 聚合器核心类型 (aggregator/)

```go
// window.go
type ComboKey struct {
    UserID   int64
    AnchorID int64
    GiftID   int
}

type ComboWindow struct {
    Key         ComboKey
    ComboCount  int32
    TotalAmount int64
    WindowStart time.Time
}

type Aggregator struct {
    mu       sync.RWMutex
    windows  map[ComboKey]*ComboWindow
    flushCh  chan *ComboWindow
    closeCh  chan struct{}
}

func (a *Aggregator) Add(key ComboKey, price int64) (*ComboWindow, bool)
// 返回: (窗口, 是否刚过期需要落盘)
```

---

## 四、 关键决策点需要你确认

1. **HTTP 框架: Gin** — 有异议吗？还是想用更轻的 Chi？
2. **DB 访问: sqlx** — 手动写 SQL，面试能聊透。确认？
3. **MQ: RabbitMQ** — Phase 1~2 用，Kafka 作为 Phase 4 演进方向。确认？
4. **配置加载: envconfig** — 环境变量注入，不引入 Viper。还是你想要 YAML 配置？
5. **测试数据** — 需要我预生成一份礼物配置（荧光棒、火箭等）的 seed SQL 吗？

前三项如果没有异议就直接按这套推进了，我先从 `cmd/main.go` 和 `config/` 开始写。
