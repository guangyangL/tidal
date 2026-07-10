# Tidal（潮汐）—— 高并发直播打赏服务 · 技术方案文档

> **版本:** v2.0
> **更新:** 2026-07-10
> **关键词:** 高并发 · Redis Lua · MQ 异步落盘 · 乐观锁 · 幂等 · 线段树排名

---

## 目录

1. [项目概述](#1-项目概述)
2. [业务边界](#2-业务边界)
3. [架构总览](#3-架构总览)
4. [数据库设计](#4-数据库设计)
5. [核心模块设计](#5-核心模块设计)
6. [高并发设计要点](#6-高并发设计要点)
7. [容错与降级](#7-容错与降级)

---

## 1. 项目概述

### 1.1 背景

直播打赏业务的流量特征呈现极端的 **"潮汐现象"**：

- **平时:** 风平浪静，每分钟几十次送礼请求。
- **PK 决胜 / 大主播高潮:** 5 秒内涌入数万并发连击，直接打穿传统同步写架构。

Tidal 通过 **Redis Lua 原子预扣 + MQ 异步批量落盘 + MySQL CAS 乐观锁** 的组合，将高峰流量削峰后写入数据库。

### 1.2 核心思想

> 请求路径上只做快操作，慢操作全部异步。

- 同步路径（毫秒级）：幂等校验 → 礼物价格查询 → Redis Lua 预扣 → 连击计数 → 排行榜写入 → MQ 投递
- 异步路径（100ms 批量）：MQ 消费 → 分组聚合 → MySQL CAS 扣减 → 流水记录 → Redis 余额同步

### 1.3 系统定位

| 职责内 | 职责外 |
| --- | --- |
| 高并发送礼请求处理 | 用户登录认证（网关层已做，Tidal 只认 Header） |
| Redis Lua 原子预扣防超卖 | 视频推拉流 |
| MQ 异步批量落盘 | 用户充值 |
| 直播间排行榜实时计算 | 主播提现与对账 |
| 可靠投递 MQ 通知分账 | 风控规则引擎 |

---

## 2. 业务边界

### 2.1 Tidal 负责的完整请求路径

```text
POST /api/v1/gift/send
    │
    ▼
┌─────────────────────────┐
│  1. Auth Middleware      │  ← X-User-ID header → user_id
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  2. Idempotent Check     │  ← Redis SETNX 600s TTL
│     (X-Request-ID)       │
└─────────┬───────────────┘
          │ 通过
          ▼
┌─────────────────────────┐
│  3. Gift Price Lookup    │  ← Redis HGET → MySQL fallback
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  4. Wallet PreDeduct     │  ← Redis Lua {GET, CHECK, DECRBY, EXPIRE}
│     (原子预扣)            │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  5. Combo Counter        │  ← Redis INCR + EXPIRE 600s
│     (连击窗口 3s)         │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  6. Leaderboard Counter  │  ← ZINCRBY + MQ → 线段树
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  7. Publish Settle Event │  ← MQ routing key: gift.settle
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│  8. Return OK            │  ← 同步路径结束
└─────────────────────────┘

异步路径:
┌─────────────────────────┐
│  Settle Consumer         │  ← 100ms 批量消费
│  ├─ 分组聚合              │
│  ├─ MySQL CAS 乐观锁扣减  │
│  ├─ INSERT 流水 (幂等)     │
│  └─ SyncBalance 安全同步 Redis│
└─────────────────────────┘

┌─────────────────────────┐
│  Leaderboard Consumer    │  ← 即时消费
│  └─ Redis Lua HINCRBY    │
│     线段树更新            │
└─────────────────────────┘
```

### 2.2 不在此次实现范围内的

- 用户注册 / 登录 / 鉴权（网关层处理）
- 直播间元数据管理
- 礼物特效、动画渲染
- 主播提现、平台对账
- 风控规则引擎

---

## 3. 架构总览

### 3.1 实际技术栈

| 层 | 选型 | 理由 |
| --- | --- | --- |
| 编程语言 | Go 1.25 | goroutine + Channel，原生并发 |
| HTTP 框架 | Gin | 路由性能 + 中间件链 |
| 配置加载 | Viper | YAML + 环境变量覆盖（TIDAL_ 前缀） |
| Redis 客户端 | go-redis v9 | Pipeline、Lua 脚本、Cluster |
| 数据库 | MySQL 8.0 (InnoDB, sqlx) | 事务 + 行锁 + 唯一索引幂等 |
| 消息队列 | RabbitMQ (amqp091-go) | Direct 交换器 + 持久化队列 |
| ID 生成 | 自实现 Sonyflake | epoch=2026-01-01，内网 IP 低 16 位定 workerId |

### 3.2 模块划分

```text
cmd/server/main.go
internal/
├── cache/        Redis 访问层（wallet Lua, gift, dedup SETNX）
├── config/       Viper 配置加载
├── event/        MQ 事件结构体
├── handler/      Gin HTTP handler
├── leaderboard/  排行榜（ZSet + 线段树 + MQ consumer）
├── middleware/   Auth（X-User-ID header）
├── model/        DB 结构体
├── mq/           RabbitMQ 封装（connect, producer, consumer）
├── repository/   MySQL/sqlx 数据访问
└── service/      业务编排（送礼流程 + settle consumer）
pkg/
├── idgen/        Sonyflake + base62 编码
└── token/        batch_token 编码
```

---

## 4. 数据库设计

### 4.1 设计原则

**高内聚:** 只存储 Tidal 自身流转必需的数据。
**零冗余:** 同一份数据只在最合适的位置存一份。
**留钩子:** 用 `extra` JSON 字段和 `wallet_type` 等扩展点为上下游预留衔接能力。

### 4.2 三张核心表

#### 4.2.1 `t_user_wallet` —— 用户虚拟钱包

```sql
CREATE TABLE `t_user_wallet` (
    `user_id`     BIGINT   NOT NULL,
    `balance`     BIGINT   NOT NULL DEFAULT 0,
    `wallet_type` TINYINT  NOT NULL DEFAULT 0,   -- 0-充值币 1-赠送币
    `version`     INT      NOT NULL DEFAULT 0,   -- 乐观锁
    `update_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`user_id`)
);
```

**乐观锁 (`version`) 的 CAS 扣减：**

```sql
UPDATE t_user_wallet
SET balance = balance - ?, version = version + 1
WHERE user_id = ? AND version = ? AND balance >= ?
```

- `RowsAffected = 0` + balance 不足 → `ErrInsufficientBalance`（不重试）
- `RowsAffected = 0` + balance 充足 → 版本冲突 → 重试（最多 3 次，指数退避 10ms/20ms/30ms）

#### 4.2.2 `t_gift_config` —— 礼物配置

```sql
CREATE TABLE `t_gift_config` (
    `gift_id`     INT         NOT NULL AUTO_INCREMENT,
    `name`        VARCHAR(64) NOT NULL,
    `price`       BIGINT      NOT NULL,
    `status`      TINYINT     NOT NULL DEFAULT 1,
    `extra`       JSON        DEFAULT NULL,
    `create_time` TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`gift_id`)
);
```

**读取策略：** Redis `HGET gift:config:{gift_id} price` → 命中直接返回 → cache miss 查 MySQL 回表。

#### 4.2.3 `t_gift_record` —— 礼物投递流水

```sql
CREATE TABLE `t_gift_record` (
    `id`           BIGINT      NOT NULL AUTO_INCREMENT,
    `batch_token`  VARCHAR(64) NOT NULL,          -- 幂等键
    `room_id`      BIGINT      NOT NULL,
    `user_id`      BIGINT      NOT NULL,
    `anchor_id`    BIGINT      NOT NULL,
    `gift_id`      INT         NOT NULL,
    `total_amount` BIGINT      NOT NULL,          -- 聚合后总金额
    `status`       TINYINT     NOT NULL DEFAULT 1,
    `create_time`  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_batch_token` (`batch_token`),
    KEY `idx_room_anchor` (`room_id`, `anchor_id`)
);
```

**`batch_token` 设计：**

```text
组成: user_id(42bit) | anchor_id(42bit) | window_start_ms(42bit)
 → base62 编码 21-22 字符，趋势递增，避免 InnoDB 随机 IO 页分裂
```

UUID v4 作为唯一索引在写入量大时会导致频繁的 **页分裂** 和 **B+树节点重平衡**。趋势递增的复合 Token 将写入转化为顺序追加。

**结算状态：**

| status | 含义 |
| --- | --- |
| 1 | 已扣款待分账 |
| 2 | 分账成功 |
| 3 | 失败待重试 |
| 4 | 死信（人工介入） |

### 4.3 Redis Key 设计

| Key | 类型 | TTL | 说明 |
|---|---|---|---|
| `idempotent:{user_id}:{reqID}` | String | 600s | 幂等防重，SETNX，值固定 "1" |
| `wallet:balance:{user_id}` | String | 3600s | 钱包余额，Lua 预扣 + SyncBalance 安全同步，每次写续期 |
| `gift:config:{gift_id}` | Hash | — | 礼物配置全量缓存，field: price/name/status/extra |
| `combo:{room}:{user}:{gift}` | String | 600s | 连击计数，INCR + EXPIRE |
| `room:leaderboard:{room}` | ZSet | — | 排行榜，member=userID, score=累计送出 coins |
| `counter:{room}:{user}` | String | — | 用户在该房间的累计总分，O(1) 查询 |
| `seg_tree:{room}` | Hash | — | 线段树粗估排名，field=区间(如 `0-976`), value=该区间人数 |

**线段树 (Segment Tree)：**

```
配置: MaxScore=1,000,000, BucketNumber=1024 → segLen=977, 共 2047 个 field

seg_tree:{room_id}:  Hash
  0-976:            100     ← 最低桶
  977-1953:         45
  ...
  999024-1000000:   1       ← 最高桶
```

- 写：MQ Consumer 收到 leaderboard 事件 → Lua `HINCRBY` 更新路径节点
- 读：`HMGET` 路径节点 → 桶内线性插值 → 粗估排名
- 排行榜查询优先走 ZSet `ZREVRANK`（精确），miss 才回退线段树（粗估）

**每请求 Redis 操作：**

```
SETNX     idempotent:{uid}:{reqID}        (写)
HGET      gift:config:{gid}               (读)
EVAL      wallet:balance:{uid}            (写, Lua)
INCR      combo:{room}:{user}:{gift}      (写)
EXPIRE    combo:{room}:{user}:{gift}      (写)
ZINCRBY   room:leaderboard:{room}         (写)
INCRBY    counter:{room}:{user}           (写)
```

---

## 5. 核心模块设计

### 5.1 幂等校验（接入层）

```text
X-Request-ID header → Redis SETNX idempotent:{user_id}:{reqID} 600s TTL
  ├─ 成功 → 继续
  └─ 失败 → HTTP 409 + code 3001 "duplicate request"
```

第 2 层防线：`batch_token` UNIQUE KEY，MySQL 级别兜底。

### 5.2 钱包预扣（Redis Lua）

```lua
-- 原子操作：GET → CHECK → DECRBY → EXPIRE
local balance = redis.call('GET', KEYS[1])
if not balance then return {-1, 0} end       -- cache miss
balance = tonumber(balance)
if balance < tonumber(ARGV[1]) then return {-2, 0} end  -- insufficient
redis.call('DECRBY', KEYS[1], ARGV[1])
redis.call('EXPIRE', KEYS[1], 3600)
return {0, balance - tonumber(ARGV[1])}
```

**三态返回：**

- `0` — 扣减成功
- `-1` — cache miss → 从 MySQL 加载余额 → SET Redis → 重试一次 Lua
- `-2` — 余额不足 → 返回 HTTP 402

**Key 设计：** `wallet:balance:{user_id}`，TTL=1h，每次预扣自动续期。

### 5.3 连击计数（Redis INCR）

```redis
INCR combo:{room_id}:{user_id}:{gift_id}
EXPIRE combo:{room_id}:{user_id}:{gift_id} 600
```

- TTL=600s 即连击窗口。连续送礼 → combo 递增，超过 10 分钟不送 → key 过期归零
- 全服务共享 Redis，WS 网关可直读 combo 数广播房间

### 5.4 排行榜

#### 写路径: Counter.AddScore

```text
1. INCRBY counter:{room}:{user} {delta}          → 个人总分（粗估排名用）
2. ZINCRBY room:leaderboard:{room} {delta} {uid} → ZSet 精确排名
3. MQ Publish gift.leaderboard                    → 线段树异步更新
```

#### 读路径

```text
GET /api/v1/room/:room_id/leaderboard?top=50
  → ZREVRANGE room:leaderboard:{room} 0 49 WITHSCORES  (精确)

GET /api/v1/room/:room_id/rank?user_id=1001
  → ZSet 有 → ZREVRANK 精确
  → ZSet 无 → counter:{room}:{user} 拿积分 → HMGet 线段树 → 粗估排名
```

#### 线段树

分数范围 [0, 1,000,000]，1024 个桶，固定 ~2047 个 Redis Hash key：

```text
seg_tree:{room_id}:  Hash
  0-976:            100
  999024-1000000:   1
```

- **更新：** Leaderboard Consumer 收到 MQ 事件 → Lua `HINCRBY` 路径节点
- **查询：** `HMGET` 路径节点 + 桶内线性插值 → 粗估排名
- **优势：** key 数量固定，人数再大不膨胀 ZSet 大 key

### 5.5 异步落盘（Settle Consumer）

```text
100ms ticker 触发
  ├─ 1. 从 buffer 取出本批事件
  ├─ 2. 按 (user, anchor, gift, room) hash 分组
  ├─ 3. 每组: 累加 totalAmount
  ├─ 4. MySQL 事务:
  │     ├─ walletRepo.Deduct(userID, amount, version)  — CAS 乐观锁, 最多 retry 3 次
  │     └─ recordRepo.Insert(record)                    — batch_token UNIQUE KEY 幂等
  └─ 5. walletCache.SyncBalance(userID)                 — 安全同步（只降不升）
```

**关键设计：**

- **CAS 先扣钱包再写流水：** 钱包扣了但流水没写 = 丢钱事故，两者顺序不可颠倒
- **乐观锁重试：** 版本冲突时指数退避（10ms → 20ms → 30ms），最多 3 次
- **幂等插入：** `ON DUPLICATE KEY UPDATE id=id`，重复投递不产生重复流水
- **SyncBalance 安全同步：** Lua 脚本只在 MySQL < Redis 时写入，防止覆盖未结算的 Redis 预扣，保证 Redis 永不高估余额

### 5.6 MQ 可靠投递

```text
Producer:
  PublishWithContext(ctx, exchange, routingKey, persistent=true)
  → 3s timeout → 失败则客户端重试

Consumer:
  Handle(msg) → 成功 → Ack
  Handle(msg) → 失败 → Nack(requeue=true) → 重新入队
```

Exchange: `tidal.settle` (direct, durable)

Queues:

- `tidal.settle.queue` ← routing key `gift.settle`（落盘消费）
- `tidal.leaderboard` ← routing key `gift.leaderboard`（线段树消费）

---

## 6. 高并发设计要点

### 6.1 乐观锁 vs 悲观锁

| 场景 | 选择 | 理由 |
| --- | --- | --- |
| DB 层钱包扣减 | 乐观锁 (`version`) | 冲突回滚自旋，不阻塞其他事务 |
| Redis 预扣 | Lua 脚本 | Redis 单线程不需要锁，Lua 保证原子性 |

### 6.2 幂等防线

```text
接入层: Redis SETNX (600s TTL) 防重复提交
    │
    ▼
持久层: batch_token UNIQUE KEY (终极兜底)
```

两层防线逐步收敛，MySQL 唯一索引是 **最终防线**。

### 6.3 批量聚合削峰效果

```text
场景: 大主播 PK 5s, 5000 用户每人连击 20 次 = 100,000 次请求

无聚合直写: 100,000 INSERT + 100,000 UPDATE → 行锁争用 → 雪崩

Tidal: 100ms 批量分组, 每 (user,anchor,gift,room) 聚合为 1 条
      5000 用户 × 1 条/窗口 ≈ 5000 INSERT + 5000 UPDATE (每 100ms)
      → DB 写压力降低 95%+
```

### 6.4 数据库连接池

```go
db.SetMaxOpenConns(50)
db.SetMaxIdleConns(20)
```

---

## 7. 容错与降级

### 7.1 降级策略

| 故障点 | 降级动作 | 影响 |
| --- | --- | --- |
| Redis 不可用 | Lua 脚本失败 → 上层拒绝请求 | 功能不可用，资金安全 |
| MySQL 不可用 | Settle consumer 阻塞 → 自然背压 | 落盘延迟，Redis 余额在 |
| MQ 不可用 | Publish 超时 3s → 返回成功（best effort） | 少量事件丢失，可通过补偿机制恢复 |
| 版本冲突高 | 重试 3 次 → 放弃本批 | 进入下个 100ms 窗口重试 |

### 7.2 优雅关闭（TODO）

当前未实现。计划方案：

```text
1. signal.Notify 捕获 SIGTERM/SIGINT
2. http.Server.Shutdown  — 停止接受新请求
3. Settle consumer 停止  — flush 最后一批
4. MQ 连接关闭            — 确保已确认消息
5. DB 连接池关闭
```

---

## 附录

### A. 面试对线题库

| 面试官提问 | 回答方向 |
| --- | --- |
| 乐观锁冲突太高怎么办？ | Redis 预扣 + 异步批量消化；拆钱包分片降低热点 |
| 为什么不用 UUID 做幂等键？ | InnoDB 随机 IO 页分裂，业务语义 Token 趋势递增 |
| Redis 预扣后进程崩溃怎么办？ | wallet key TTL 1h 自动过期 → 下次请求触发 LoadBalance → MySQL 余额是准的；未结算的预扣不会多扣钱 |
| 分账失败怎么处理？ | status + 重试 + MQ 死信，3 次后人工介入 |
| 怎么保证不超卖？ | Redis Lua 预扣 + MySQL `WHERE balance >= ?` 双重校验 |
| 榜单数据不准怎么办？ | "尽力而为实时"，ZSet 存精确 TopN，线段树粗估其余。定期可全量重建 |
| 线段树为什么是 1024 个桶？ | 2047 个 Hash key 固定内存，Redis HMGET 一次取全路径 O(log N) |

### B. 错误码约定

| HTTP 状态码 | 业务码 | 含义 |
| --- | --- | --- |
| 200 | 0 | 成功 |
| 400 | 1001 | 请求参数不合法 |
| 402 | 2001 | 余额不足 |
| 404 | 2002 | 礼物不存在或已下架 |
| 409 | 3001 | 幂等冲突（重复请求） |
| 500 | 9001 | 服务内部错误 |
| 503 | 9002 | 服务过载，稍后重试 |

---

> 本文档对应 Tidal v2.0 的实际实现。所有设计决策均以代码可验证为前提。
