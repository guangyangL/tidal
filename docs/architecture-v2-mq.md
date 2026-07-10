# Tidal 架构设计

## 整体流程

```
┌─────────────────────────────────────────────────────────────────┐
│                   POST /api/v1/gift/send                         │
│  1. Auth Middleware: X-User-ID header → user_id                  │
│  2. Idempotent Check: Redis SETNX idempotent:{uid}:{reqID} 600s   │
│  3. Gift Price: Redis HGET gift:config:{gid} → MySQL fallback   │
│  4. PreDeduct: Redis Lua {GET, CHECK, DECRBY, EXPIRE}           │
│  5. Combo: INCR combo:{room}:{user}:{gift} + EXPIRE 600s        │
│  6. Counter.AddScore → ZINCRBY leaderboard + MQ 线段树            │
│  7. Publish gift.settle MQ event                                │
│  8. Return OK                                                   │
└──────────────────┬──────────┬───────────────────────────────────┘
                   │          │
           ┌───────▼──┐  ┌────┴────────────────┐
           │ Redis    │  │ RabbitMQ             │
           │ ZSet     │  │ exchange: tidal.settle│
           │ 线段树    │  │ ├─ gift.settle       │
           └──────────┘  │ └─ gift.leaderboard  │
                         └──┬───────────────────┘
                            │
       ┌────────────────────┴─────────────────────┐
       │                                          │
┌──────▼──────────────────┐    ┌──────────────────▼──────┐
│ Settle Consumer         │    │ Leaderboard Consumer    │
│ 100ms 批量消费           │    │ 即时消费                 │
│                         │    │                         │
│ 按 (user,anchor,        │    │ oldScore = Score -      │
│  gift,room) hash 分组   │    │          DeltaScore     │
│                         │    │                         │
│ ┌─ MySQL CAS 乐观锁扣减  │    │ ┌─ UpdateScore/AddScore │
│ ├─ INSERT 流水(幂等)     │    │ │  Redis Lua HINCRBY   │
│ └─ LoadBalance 同步Redis │    │ └─ 线段树 Hash 更新     │
└─────────────────────────┘    └─────────────────────────┘
```

## 幂等

- `X-Request-ID` header → Redis `SETNX idempotent:{user_id}:{reqID}` 600s TTL
- 重复请求返回 HTTP 409 + code 3001
- 持久层 `batch_token` UNIQUE KEY 作为最终防线

## 礼物价格查询

- Redis `HGET gift:config:{gift_id} price` → 命中直接返回
- Cache miss → MySQL `t_gift_config` 回表 → 返回价格
- 礼物配置全量缓存，送礼链路尽量不查 MySQL

## 钱包预扣

- **正常路径**：Redis Lua 原子扣减 `{GET, CHECK, DECRBY, EXPIRE}`，5 行脚本
- **Cache miss**：自动加载 MySQL balance → Redis SET → 重试 Lua

```
Redis Lua:
  balance = GET key
  if not balance → NOT_CACHED（加载 MySQL 重试一次）
  if balance < amount → INSUFFICIENT
  DECRBY key amount
  EXPIRE key 3600
  return OK
```

- wallet key: `wallet:balance:{user_id}`, TTL=1h，每次扣减自动续期

### 余额一致性保证

Redis 预扣（快路径）和 MySQL 结算（慢路径）之间存在时间窗口：

- **PreDeduct**：请求进来立刻从 Redis 扣减
- **Settle**：100ms 批量从 MySQL CAS 扣减
- **SyncBalance**：结算后用 Lua 脚本同步 Redis，**只在 MySQL < Redis 时才写入**

```
SyncBalance Lua:
  redis_bal = GET key
  if not redis_bal → skip (key expired, next PreDeduct will LoadBalance)
  if mysql_bal < redis_bal → SET redis = mysql_bal
  // mysql_bal >= redis_bal: 跳过，保护未结算预扣不被覆盖
```

不变式：**Redis ≤ MySQL — 待结算预扣**。Redis 永远不会高估余额，杜绝超卖。

## 连击计数

```redis
INCR combo:{room_id}:{user_id}:{gift_id}
EXPIRE combo:{room_id}:{user_id}:{gift_id} 600
```

TTL=600s = 连击窗口。全服务共享 Redis → WS 网关可读 combo 数广播房间。

## Redis Key 设计

| Key | 类型 | TTL | 说明 |
|---|---|---|---|
| `idempotent:{user_id}:{reqID}` | String | 600s | 幂等防重，SETNX，值固定 "1" |
| `wallet:balance:{user_id}` | String | 3600s | 钱包余额缓存，Lua 原子预扣，每次写续期 |
| `gift:config:{gift_id}` | Hash | — | 礼物配置全量缓存，field: price/name/status/extra |
| `combo:{room}:{user}:{gift}` | String | 600s | 连击计数，INCR + EXPIRE |
| `room:leaderboard:{room}` | ZSet | — | 排行榜，member=userID, score=累计送出 coins |
| `counter:{room}:{user}` | String | — | 用户在该房间的累计总分，O(1) 查询 |
| `seg_tree:{room}` | Hash | — | 线段树粗估排名，field=区间(如 `0-976`), value=该区间人数 |

**线段树结构：**

- 配置：`MaxScore=1,000,000`, `BucketNumber=1024` → segLen=977
- 叶子节点 1024 个 + 内部节点 1023 个 = 总计 **2047 个 field**
- 写：MQ Consumer 收到 leaderboard 事件 → Lua `HINCRBY` 更新路径节点的计数
- 读：`HMGET` 路径节点 → 桶内线性插值 → 粗估排名
- ZSet 有命中 → `ZREVRANK` 精确排名；ZSet 未命中 → 回退线段树粗估

**各 key 操作频率（每请求）：**

```
请求路径 7 次 Redis 操作:
  SETNX     idempotent (写)
  HGET      gift:config (读)
  EVAL      wallet:balance (写, Lua)
  INCR      combo (写)
  EXPIRE    combo (写)
  ZINCRBY   room:leaderboard (写)
  INCRBY    counter (写)
```

## 排行榜

两个查询接口：

```
GET /api/v1/room/:room_id/leaderboard?top=50  // ZSet TopN，精确
GET /api/v1/room/:room_id/rank?user_id=1001   // 个人排名
```

### 写路径: Counter.AddScore

```
1. INCRBY counter:{room}:{user}   → 存储个人总分，供粗估排名用
2. ZINCRBY room:leaderboard:{room} {delta} {user_id}  → ZSet 精确排名
3. MQ publish gift.leaderboard → ChangeCounterTriggerEvent
```

### ZSet 精确 TopN

`ZREVRANGE room:leaderboard:{room_id} 0 N-1 WITHSCORES`，毫秒级。

### 线段树粗估排名

分数范围 [0, 1,000,000] 建 1024 个桶（线段树），固定 ~2047 个 Redis Hash key：

```
seg_tree:{room_id}:  Hash
  0-976:            100     ← 最低桶，count
  999024-1000000:   1       ← 最高桶
  ...
```

**更新**：Leaderboard Consumer 收到 MQ 事件 → Lua HINCRBY 路径节点
**查询**：HMGet 路径节点 + 桶内线性插值 → 排名

**查询链路**：
```
Rank(user_id):
  → ZSet 有 → ZREVRANK 精确
  → ZSet 无 → counter:{room}:{user} 拿积分 → HMGet 线段树 → 粗估排名
```

## 落盘

Settle Consumer 每 100ms 拉取，按 `(user, anchor, gift, room)` hash 分组：

```
// 100ms 窗口内收到
events := [{user=1, room=2, anchor=3, gift=1, price=10},
           {user=1, room=2, anchor=3, gift=1, price=10}]

// hash 分组 → 1 次 MySQL CAS 扣减 + 1 条流水
group: totalAmount = 20
```

MySQL 乐观锁 CAS 扣减（`WHERE user_id=? AND version=? AND balance>=?`）+ `INSERT ... ON DUPLICATE KEY UPDATE` 幂等 + SyncBalance 安全同步 Redis。

写路径流程：

```
1. walletRepo.Deduct(userID, amount, version) — CAS 乐观锁, 最多重试 3 次
2. recordRepo.Insert(record) — batch_token 唯一键幂等
3. walletCache.SyncBalance(userID) — 安全同步（只降不升，防止覆盖未结算预扣）
```

## 恢复机制

| 场景 | 恢复 |
|---|---|
| 进程正常启动 | warmup 钱包 → Redis key 从 MySQL 加载 |
| 进程崩溃 | MQ 持久化，consumer 追平 |
| Redis 崩溃 | Lua 脚本返回错误 → 上层拒绝请求，等 Redis 恢复 |
| MySQL 慢 | settle consumer 阻塞 → 自然背压 |
| 钱包 key 不一致 | TTL 1h 后自动过期 → cache miss 触发 LoadBalance 重载 |

## 文件结构

```
cmd/server/main.go          # 启动入口：依赖注入 → 启动 HTTP
config/config.yaml           # YAML 配置文件
internal/
├── cache/                   # Redis 访问层（wallet, gift, dedup）
├── config/                  # Viper 配置加载
├── event/                   # MQ 事件类型定义
├── handler/                 # Gin HTTP handler（gift）
├── leaderboard/             # 排行榜（counter, service, tree, redis, consumer, handlers, types）
├── middleware/              # Auth middleware（X-User-ID header）
├── model/                   # DB 结构体
├── mq/                      # RabbitMQ（connect, producer, consumer）
├── repository/              # MySQL/sqlx 数据访问
└── service/                 # 业务层（gift_service, settle）
pkg/
├── idgen/                   # Sonyflake ID 生成 + base62 编码
└── token/                   # batch_token 编码
deploy/                      # Dockerfile + docker-compose.yml
scripts/                     # 压测脚本 + DDL 参考
```
