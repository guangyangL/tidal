# Tidal（潮汐）—— 高并发直播连击打赏与结算引擎 · 技术方案文档

> **版本:** v1.0  
> **更新:** 2026-07-03  
> **关键词:** 高并发 · 内存聚合 · 滑动窗口 · 最终一致性 · 幂等

---

## 目录

1. [项目概述](#1-项目概述)
2. [业务边界](#2-业务边界)
3. [核心指标](#3-核心指标)
4. [架构总览](#4-架构总览)
5. [数据库设计](#5-数据库设计)
6. [核心模块设计](#6-核心模块设计)
7. [高并发设计要点](#7-高并发设计要点)
8. [容错与降级](#8-容错与降级)
9. [演进路线](#9-演进路线)

---

## 1. 项目概述

### 1.1 背景

直播打赏业务的流量特征呈现极端的 **"潮汐现象"**：

- **平时:** 风平浪静，每分钟几十次送礼请求，MySQL 直写毫无压力。
- **PK 决胜 / 大主播高潮:** 5 秒内涌入数万并发连击，直接打穿传统同步写架构。

Tidal 引擎正是为解决这一矛盾而生的中间层核心业务模块。

### 1.2 核心思想

一句话概括：**"把 100 次连击在内存里吃掉，只让 1 次落到数据库。"**

利用 Go 的 goroutine + Channel 做本地内存滑动窗口聚合，在进程内完成流量的"海啸削峰"，将 DB 写压力降低 1~2 个数量级。

### 1.3 系统定位

Tidal 是一个纯粹的 **中间层核心业务引擎**，不是大一统平台。

| 职责内（硬核实现） | 职责外（Mock / 假接口隔离） |
|---|---|
| 高并发送礼与连击请求接收 | 用户登录认证（网关层已做，Tidal 只认 Header 的 UserID） |
| 内存级防超卖与极速账务扣减 | 视频流推拉流（音视频基建） |
| 连击订单的滑动窗口聚合 | 用户真实充值（假设钱包已有足够"金币"） |
| 直播间财富 / 贡献榜实时计算 | 主播提现与对账（下游结算系统） |
| 可靠投递 MQ 通知分账 | 风控策略引擎（Tidal 只做幂等防重，不做规则判断） |

---

## 2. 业务边界

### 2.1 Tidal 负责的完整请求路径

```
客户端连击触发
    │
    ▼
┌─────────────────────────┐
│  1. 极速鉴权与拦截       │  ← Redis: 余额检查 + 礼物合法性
│     (内存级，百微秒级)    │
└─────────┬───────────────┘
          │ 通过
          ▼
┌─────────────────────────┐
│  2. 内存连击聚合         │  ← Go Sliding Window (3s窗口)
│     (核心亮点, 异步写)    │     同一 (user, anchor, gift) 合并计数
└─────────┬───────────────┘
          │ 窗口期满
          ▼
┌─────────────────────────┐
│  3. 异步落盘与流水生成    │  ← 1条聚合记录 = N次连击
│     (CAS乐观锁 + 幂等)   │     写入 t_gift_record + 更新 t_user_wallet
└─────────┬───────────────┘
          │ 落盘成功
          ▼
┌─────────────────────────┐
│  4. 榜单热度刷新         │  ← 异步 goroutine 写入 Redis ZSet
│     (协程异步，不阻塞主链) │     实时刷新贡献榜
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  5. 最终一致性结算        │  ← 投递 MQ → 下游分账消费
│     (MQ 可靠投递)        │
└─────────────────────────┘
```

### 2.2 不在此次实现范围内的

- 用户注册 / 登录 / 鉴权
- 直播间元数据管理（房间创建、销毁、封禁）
- 礼物特效、动画渲染
- 主播提现、平台对账
- 风控规则引擎（命中规则直接拒绝策略由业务方配置）

---

## 3. 核心指标

| 指标 | 目标值 | 备注 |
|---|---|---|
| 单机 QPS（送礼请求） | 50,000+ | 内存聚合前端接口 |
| P99 延迟 | < 50ms | 客户端"送礼成功"响应 |
| 滑动窗口聚合比 | ≥ 50:1 | 窗口内连击合并率 |
| DB 写放大系数 | ≤ 1.02 | 聚合后几乎 1:1 写入 |
| 数据零丢失 | 幂等 + 重试保障 | batch_token 唯一键兜底 |
| 分账延迟 | < 30s（P99）| 从记录落盘到 MQ 投递完成 |

---

## 4. 架构总览

### 4.1 技术栈

| 层 | 选型 | 理由 |
|---|---|---|
| 编程语言 | Go 1.22+ | 原生 goroutine + Channel，天然适合内存聚合削峰 |
| HTTP 框架 | Gin / Fiber | 高性能路由，中间件链支持 |
| 内存缓存 | Redis (go-redis) | 余额预扣、ZSet 排行榜、礼物配置缓存 |
| 主数据库 | MySQL 8.0+ (InnoDB) | 事务 + 行锁 + 唯一索引幂等 |
| 消息队列 | RabbitMQ / Kafka | 最终一致性解耦 |
| ID 生成器 | 雪花算法 (Sonyflake) | 全局趋势递增 ID |

### 4.2 模块划分

```
tidal-engine/
├── cmd/server/              # 启动入口
├── internal/
│   ├── handler/             # HTTP handler (Gin routes)
│   ├── middleware/          # 鉴权中间件 (解析 UserID)
│   ├── service/             # 核心业务逻辑
│   │   ├── gift_service.go      # 送礼主流程编排
│   │   ├── wallet_service.go    # 钱包扣减与乐观锁
│   │   └── settle_service.go    # 分账结算逻辑
│   ├── aggregator/          # ★ 滑动窗口聚合器
│   │   ├── window.go            # Sliding Window 实现
│   │   └── flusher.go           # 窗口期满 → 异步落盘
│   ├── repository/          # DB 访问层
│   │   ├── wallet_repo.go
│   │   ├── gift_repo.go
│   │   └── record_repo.go
│   ├── cache/               # Redis 访问层
│   │   ├── wallet_cache.go      # 余额预扣/解冻
│   │   ├── gift_cache.go        # 礼物配置缓存
│   │   └── leaderboard.go       # ZSet 排行榜
│   └── mq/                  # 消息队列封装
│       ├── producer.go
│       └── consumer.go
├── pkg/
│   ├── token/               # batch_token 生成器
│   └── idgen/               # 雪花算法 ID 生成
├── scripts/sql/             # DDL
├── docs/                    # 文档
└── go.mod
```

---

## 5. 数据库设计

### 5.1 设计原则

**高内聚:** 只存储 Tidal 自身流转必需的数据，不混入充值、提现、房间元数据。  
**零冗余:** 同一份数据只在最合适的位置存一份。  
**留钩子:** 用 `extra` JSON 字段和 `wallet_type` 等扩展点为上下游预留衔接能力。

### 5.2 三张核心表

详见 `scripts/sql/001_init_schema.sql`，此处只强调关键设计意图。

#### 5.2.1 `t_user_wallet` —— 用户虚拟钱包

```sql
CREATE TABLE `t_user_wallet` (
    `user_id`       BIGINT   NOT NULL,
    `balance`       BIGINT   NOT NULL DEFAULT 0,
    `frozen_amount` BIGINT   NOT NULL DEFAULT 0,   -- [新增] 两阶段扣减用
    `wallet_type`   TINYINT  NOT NULL DEFAULT 0,   -- [新增] 0-充值币 1-赠送币
    `version`       INT      NOT NULL DEFAULT 0,
    `update_time`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`user_id`)
);
```

**为什么需要 `frozen_amount`？**

> 两阶段扣减流程：
> 1. 请求到达 → Redis 扣减余额（预扣），记录冻结标记
> 2. 滑动窗口期满 → 开启 DB 事务：`balance` 扣减确认，`frozen_amount` 清0
> 3. 若第2步崩溃 → 重启后扫描 `frozen_amount > 0` 的记录，与聚合窗口状态比对后决定"确认"或"解冻"
>
> 没有这个字段，Redis 预扣后进程崩溃，资金状态永远不一致。

**乐观锁 (`version`) 的冲突处理：**

- 默认 CAS: `UPDATE SET balance = balance - ?, version = version + 1 WHERE user_id = ? AND version = ?`
- 冲突率 > 阈值时，回退到 Redis 层做预扣，DB 层异步批量消化

#### 5.2.2 `t_gift_config` —— 礼物配置

```sql
CREATE TABLE `t_gift_config` (
    `gift_id`     INT          NOT NULL AUTO_INCREMENT,
    `name`        VARCHAR(64)  NOT NULL,
    `price`       BIGINT       NOT NULL,
    `status`      TINYINT      NOT NULL DEFAULT 1,
    `extra`       JSON         DEFAULT NULL,   -- [新增] 限时折扣、特效加成
    `create_time` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`gift_id`)
);
```

**读取策略：**

- 首次启动：全量加载到 Redis Hash (`HGETALL t_gift_config`)
- 增量更新：管理者后台变更 → 发送缓存失效事件 → Tidal 监听并刷新
- 送礼请求的整个生命周期 **完全不查 MySQL**

#### 5.2.3 `t_gift_record` —— 礼物投递流水（核心账本）

```sql
CREATE TABLE `t_gift_record` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT,
    `batch_token`   VARCHAR(64)  NOT NULL,           -- 业务幂等键（非UUID，见下文）
    `room_id`       BIGINT       NOT NULL,
    `user_id`       BIGINT       NOT NULL,
    `anchor_id`     BIGINT       NOT NULL,
    `gift_id`       INT          NOT NULL,
    `combo_count`   INT          NOT NULL DEFAULT 1,
    `total_amount`  BIGINT       NOT NULL,
    `status`        TINYINT      NOT NULL DEFAULT 1, -- 1-待分账 2-成功 3-待重试 4-死信
    `retry_count`   TINYINT      NOT NULL DEFAULT 0, -- [新增] 重试次数
    `settle_time`   TIMESTAMP    NULL,                -- [新增] 分账完成时间
    `extra`         JSON         DEFAULT NULL,        -- [新增] 抽成比例等
    `create_time`   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_batch_token` (`batch_token`),
    KEY `idx_room_anchor` (`room_id`, `anchor_id`),
    KEY `idx_status_retry` (`status`, `retry_count`)
);
```

**`batch_token` 的设计 —— 为什么不用 UUID？**

```
组成: user_id(42bit) | anchor_id(42bit) | window_start_ms(42bit)
 → base62 编码 ≈ 21 字符，趋势递增，避免 InnoDB 随机 IO 页分裂
```

UUID v4 作为唯一索引在写入量大时会导致频繁的**页分裂**和**B+树节点重平衡**，实测 TPS 在 10w+ 时下降 40% 以上。趋势递增的复合 Token 将写入转化为顺序追加。

**`combo_count` 的价值：**

用户点 100 次连击 → 1 条记录，`combo_count = 100`。这是整个架构"削峰"效果的可量化证明。

**结算状态机流转：**

```
     ① 创建 (已扣款待分账)
          │
          ▼
     ② 分账成功 ◄── 正常 MQ 消费
          │
          ▼ (重试 ≤ 3 次)
     ③ 失败待重试 ──→ ④ 死信 (人工介入)
```

`idx_status_retry` 索引支撑后台定时任务或 MQ 死信队列扫描待重试数据。

### 5.3 为什么不拆更多表？

面试中常见追问："你也说了流水表会越来越大，为什么不分库分表？"

**回答口径：**

> "分库分表是**长线演进**动作，不是初期设计。Tidal 第一阶段采用 **冷热分离**（定期归档 30 天前的数据到 TiDB / ClickHouse），配合 `create_time` 分区表，单表可支撑千万级日流水。
>
> 当单表 QPS 持续超过 5000 或数据量突破 2 亿行，才会引入 ShardingSphere / 应用层分片。过早分片会带来跨节点事务、全局 ID 等不必要的复杂度，**架构演进应比业务领先一步，而不是领先十步。**"

---

## 6. 核心模块设计

### 6.1 极速鉴权与拦截（百微秒级）

**位置:** HTTP 中间件层  
**依赖:** Redis MSET / MGET

```
请求到达
  │
  ├─ 1. 从 Header 提取 UserID（网关已鉴权）
  ├─ 2. [Redis Pipeline] MGET:
  │      - KEY: wallet:balance:{user_id}        → 余额 ≥ 礼物单价？
  │      - KEY: gift:config:{gift_id}            → 礼物上架且价格一致？
  │      - KEY: room:pk:{room_id}                → 是否在 PK 状态（双倍加成？）
  ├─ 3. 任一检查不通过 → 熔断，返回 427 (余额不足) / 404 (礼物下架)
  └─ 4. 全部通过 → Redis DECRBY 预扣余额，继续向下传递
```

**注意:** 步骤 4 的 Redis DECRBY 是**预扣（冻结）**，并非真正扣减。对应的 `frozen_amount` 会在异步落盘阶段确认。如果落盘失败，解冻流程在 `flusher.go` 中补偿。

### 6.2 内存连击聚合 —— 滑动窗口（★★★★★ 核心亮点）

#### 6.2.1 为什么在内存中做？

- 即使用 Redis 做 INCRBY，10 万次写仍然会打满网络 IO
- 本地内存聚合的延迟是纳秒级的，且零网络开销
- Go 的 goroutine + Channel 模式写起来非常简洁，天然并发安全

#### 6.2.2 数据结构

```go
type ComboKey struct {
    UserID   int64
    AnchorID int64
    GiftID   int
}

type ComboWindow struct {
    Key        ComboKey
    ComboCount int32
    TotalAmount int64
    WindowStart int64  // unix nano, 窗口起始时间
    UserID     int64
}

// 全局聚合器
type Aggregator struct {
    mu       sync.RWMutex
    windows  map[ComboKey]*ComboWindow
    flushCh  chan *ComboWindow
}
```

#### 6.2.3 流转逻辑

```
每次送礼点请求
    │
    ├─ aggregator.Add(key, price)
    │      │
    │      ├─ 查找 windows[key]
    │      ├─ 不存在 → 创建新窗口，记录 WindowStart = now
    │      ├─ 存在且 (now - WindowStart) < 3s
    │      │     └─ ComboCount++, TotalAmount += price
    │      └─ 存在但 (now - WindowStart) >= 3s
    │            └─ 旧窗口 → flushCh (异步落盘)
    │            └─ 新窗口 → 重置 WindowStart，计数归零
    │
    └─ 返回 "送礼成功" 给客户端（实际还在窗口内）
```

#### 6.2.4 关键问题：进程重启丢数据？

> **回答：**
>
> "滑动窗口是纯内存结构，进程重启确实会丢失未刷新的窗口数据。但我们有 **两层兜底**：
>
> 1. **上游客户端重试机制：** 客户端收到超时或断开后会重发请求，Tidal 通过 `batch_token` 检测幂等。丢失的窗口最多导致 3 秒的连击记录丢失，对用户体验影响有限。
> 2. **Redis 中间状态：** 每次预扣都在 Redis 中留有痕迹。进程恢复后，启动阶段会扫描 Redis 中 `frozen_amount > 0` 的异常数据，通过与客户端最终确认来补偿。
>
> 如果要做到 **严格不丢**，可以引入 Write-Ahead Log（预写日志），但会牺牲 10%~20% 的性能，属于典型的 CAP 权衡。在直播场景中，丢失 1~2 秒的连击可以接受，我们选择保性能。"
>
> **（面试官会认可这种 "trade-off" 式思考方式。）**

### 6.3 异步落盘与流水生成

从 `flushCh` 消费 `ComboWindow` 的 worker goroutine：

```
worker 收到 ComboWindow
    │
    ├─ 1. 生成 batch_token = EncodeBase62(user_id, anchor_id, WindowStart)
    │
    ├─ 2. 开启 MySQL 事务
    │      ├─ UPDATE t_user_wallet SET balance -= totalAmount, 
    │      │   frozen_amount -= totalAmount, version += 1
    │      │   WHERE user_id = ? AND version = ?
    │      │   (乐观锁，失败则回滚并重试)
    │      │
    │      └─ INSERT INTO t_gift_record (batch_token, ...)
    │          ON DUPLICATE KEY UPDATE ... (幂等)
    │
    ├─ 3. 事务提交成功
    │      └─ 异步 goroutine: Redis ZINCRBY room:leaderboard:{room_id}
    │
    └─ 4. 投递 MQ 消息 (通知结算)
```

**关键设计：**

- **事务内先扣钱包再写流水：** 两者必须同时成功。钱包扣了但流水没写 = 丢钱事故。
- **幂等插入：** `batch_token` 的唯一索引确保重复投递不会生成重复流水。即使上游重试、worker 重复消费，最多产生一次有效扣款。
- **乐观锁重试：** 版本冲突时，worker 在 goroutine 内自旋重试（指数退避，最多 3 次）。超过次数进入死信。

### 6.4 榜单热度刷新

```
// 负责刷新贡献榜的 goroutine（goroutine-safe, 非阻塞）
func (s *LeaderboardService) Flush(roomID, userID int64, amount int64) {
    key := fmt.Sprintf("room:leaderboard:%d", roomID)
    s.redis.ZIncrBy(ctx, key, float64(amount), strconv.FormatInt(userID, 10))
    // ZSet 自动排序，无需额外维护
}
```

**为什么不放在事务内？**

> "榜单是 '尽力而为实时' 的 —— 就算丢失几次 ZIncrBy，用户刷新页面后下一次聚合也会覆盖。用异步 goroutine 刷新，避免 Redis 网络抖动阻塞主链路的事务提交。如果 Redis 宕机，降级为 30 秒定时全量扫描 DB 重建排行榜。榜单不参与资金链路，允许写失败。"

### 6.5 最终一致性结算

```
                    ┌──────────────────┐
                    │   t_gift_record   │
                    │   status = 1      │
                    └────────┬─────────┘
                             │
                   落盘成功触发
                             ▼
                    ┌──────────────────┐
                    │    MQ Producer    │
                    │   topic: settle   │
                    └────────┬─────────┘
                             │
                    可靠投递（confirm 模式）
                             ▼
                    ┌──────────────────┐
                    │  下游结算 Consumer │
                    │                    │
                    │ 1. 读取 settle_time │
                    │ 2. 更新 status = 2 │
                    │ 3. 调用分成接口     │
                    └──────────────────┘
```

**重试机制：**

| 重试次数 | 行为 |
|---|---|
| 0 | 首次投递 |
| 1~3 | MQ 自动重试（延时队列，每次间隔 10s） |
| > 3 | `status = 4`（死信），人工介入处理 |

---

## 7. 高并发设计要点

### 7.1 乐观锁 vs 悲观锁

| 场景 | 选择 | 理由 |
|---|---|---|
| DB 层钱包扣减 | 乐观锁 (`version`) | InnoDB 行锁在热点行冲突率高时退化为互斥，乐观锁让冲突回滚自旋，不阻塞其他事务 |
| Redis 预扣 | WATCH + MULTI / Lua | Redis 单线程不需要锁，Lua 脚本保证原子性 |
| 内存窗口 | sync.RWMutex | 读多写少（送礼读、窗口期满写），RWMutex 优于 Mutex |

### 7.2 幂等防线

```
请求层:   客户端全局序列号 (UUID)
    │
    ▼
接入层:   Redis SetNX (5s 过期) 防重复提交
    │
    ▼
持久层:   batch_token 唯一索引 (终极兜底)
```

三层防线逐步收敛，越往内层越严格。最内层 MySQL 唯一索引是**最终防线**，确保即使前面全部失效，也不会产生重复扣款。

### 7.3 内存聚合削峰效果

```
场景: 大主播 PK 最后 5s, 5000 用户每人连击 20 次 = 100,000 次请求

无聚合直写:  100,000 次 INSERT + 100,000 次 UPDATE (钱包)
            → 行锁争用 → 连接池打满 → 雪崩

Tidal 聚合: 窗口 3s, 每个 (user, anchor, gift) 聚合为 1 条
            5000 用户 × 1 条 ≈ 5,000 条 INSERT + 5,000 UPDATE
            → DB 写压力降低 95%+
```

### 7.4 数据库连接池调优

```go
// 核心参数（非缺省值）
sqlDB.SetMaxOpenConns(50)       // 默认 0=无限，必须设上限
sqlDB.SetMaxIdleConns(20)       // 保持预热连接
sqlDB.SetConnMaxLifetime(30 * time.Minute)  // 防止长时间连接被 LB 切断
```

---

## 8. 容错与降级

### 8.1 降级策略矩阵

| 故障点 | 降级动作 | 影响 |
|---|---|---|
| Redis 宕机 | 跳过预扣，直接走 DB 乐观扣减 | 延迟升高，功能正常 |
| MySQL 不可用 | 返回"送礼失败"，客户端重试 | 用户感知，资金安全 |
| 滑动窗口打满 | 同步写（绕过窗口，直接落盘） | 退化为直写，DB 压力升高 |
| MQ 不可用 | 本地文件暂存 + 定时重投 | 结算延迟，不丢数据 |
| 单机 OOM | Kubernetes 重新调度，客户端重连重试 | 秒级中断 |

### 8.2 死信处理

`t_gift_record` 中 `retry_count >= 3` 且 `status = 3` 的记录，标记为 `status = 4`（死信），不再自动重试。

监控告警规则：

```
⚠️ WARNING: 死信数量 > 10/min → 企业微信 / 钉钉告警
🔴 CRITICAL: 死信数量 > 100/min → 电话值班 On-Call
```

### 8.3 优雅关闭

```go
// server.go
func (s *Server) Shutdown(ctx context.Context) error {
    // 1. 停止接受新请求 (http.Server.Shutdown)
    // 2. 强制刷出所有未完成的滑动窗口 (Aggregator.FlushAll)
    // 3. 等待所有 in-flight 事务完成 (sync.WaitGroup)
    // 4. 关闭 MQ 连接 (确保已有消息确认)
    // 5. 关闭 DB 连接池
}
```

---

## 9. 演进路线

### Phase 1（第 1~2 周）—— 核心链路打通

- 完成 3 张表的 DDL 与初始化
- 实现送礼主流程 HTTP Handler
- 实现单机滑动窗口聚合（`aggregator/`）
- 完成 `t_user_wallet` 乐观锁扣减

### Phase 2（第 3~5 周）—— 异步与可靠性

- 聚合窗口期满 → 异步落盘
- `batch_token` 幂等生成与校验
- Redis 余额预扣 + 缓存刷新
- 榜单 ZSet 写入
- MQ 生产者接入

### Phase 3（第 6~8 周）—— 压测与加固

- 单机 5w QPS 压测（pprof 优化热点）
- 死信队列 + 重试机制
- 优雅关闭
- 集成测试（幂等、并发扣减、故障注入）

### Phase 4（第 9~12 周）—— 生产化

- Docker + Docker Compose 编排
- Prometheus 指标暴露（QPS、延迟、聚合比、死信数）
- Grafana 看板
- 文档完善 + 简历文案打磨

### 未来（不在 3 个月范围内）

| 项目 | 触发条件 |
|---|---|
| 分库分表 (ShardingSphere) | 日流水 > 2 亿行 |
| 冷热分离 (归档至 ClickHouse) | 单表 > 2TB |
| 多区域部署 (就近接入) | 东南亚/欧美用户接入 |
| 基于 QPS 的动态窗口大小 | 生产运行 1 个月后数据分析 |

---

## 附录

### A. 面试对线题库

| 面试官提问 | 回答方向 |
|---|---|
| 乐观锁冲突太高怎么办？ | Redis 预扣 + 异步批量消化；账务拆分降低热点 |
| 为什么不用 UUID 做幂等键？ | InnoDB 随机 IO 页分裂，业务语义 Token 趋势递增 |
| 滑动窗口丢数据怎么办？ | 客户端重试 + batch_token 幂等 + Redis 中间态补偿（CAP 权衡） |
| 分账失败怎么处理？ | status + retry_count + MQ 死信，3 次后人工介入 |
| 怎么保证不超卖？ | Redis 预扣冻结 → DB 乐观锁确认，两层校验 |
| 榜单数据不准怎么办？ | "尽力而为实时"，允许短暂不一致，定期全量重建 |
| K8s 滚动更新怎么保证不丢请求？ | 优雅关闭 + 客户端重试 + 幂等兜底 |

### B. 错误码约定

| HTTP 状态码 | 业务码 | 含义 |
|---|---|---|
| 200 | 0 | 成功 |
| 400 | 1001 | 请求参数不合法 |
| 402 | 2001 | 余额不足 |
| 404 | 2002 | 礼物不存在或已下架 |
| 409 | 3001 | 幂等冲突（重复请求） |
| 429 | 4001 | 触发限流 |
| 500 | 9001 | 内部服务错误 |

---

> 本文档对应 Tidal v1.0 的设计范围。所有设计决策均以 **可论证、可压测** 为前提，拒绝在面试中回答 "不知道，当时是这么写的"。
