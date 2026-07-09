# Tidal 架构设计

## 整体流程

```
┌──────────────────────────────────────────────────────────────┐
│                       送礼请求 /api/v1/gift/send               │
│  1. Idempotent Check（内存 map）                               │
│  2. 查礼物价格（Redis / MySQL fallback）                       │
│  3. PreDeduct（Redis Lua 原子扣减）                            │
│  4. INCR combo:{room}:{user}:{gift} + EXPIRE 3s → comboCount │
│  5. Counter.AddScore → ZINCRBY leaderboard + MQ 线段树        │
│  6. Publish settle MQ {user, anchor, gift, price, comboCount}│
│  7. Return OK + comboCount                                   │
└──────────────────────┬──────┬────────────────────────────────┘
                       │      │
              ┌────────▼──┐   └──────────┐
              │ Redis 实时 │              │
              │ 排行榜 ZSet│      ┌───────▼──────────────┐
              │ 线段树 Hash│      │ RabbitMQ             │
              └───────────┘      │ ├─ gift.settle       │
                                 │ └─ gift.leaderboard  │
                                 └──┬───────────────────┘
                                    │
           ┌────────────────────────┴─────────────────────┐
           │                                              │
    ┌──────▼──────────────┐              ┌────────────────▼──────┐
    │ Settle Consumer     │              │ Leaderboard Consumer  │
    │ 100ms 批量消费       │              │ 即时消费               │
    │                     │              │                       │
    │ 按 (user,anchor,    │              │ oldScore = Score-     │
    │  gift,room) 分组    │              │          DeltaScore   │
    │                     │              │                       │
    │ ┌─ MySQL 乐观锁扣减  │              │ ┌─ UpdateScore(Redis  │
    │ ├─ INSERT 流水记录  │              │ │  Lua HINCRBY)      │
    │ └─ LoadBalance 同步 │              │ └─ 线段树 Hash 更新   │
    └─────────────────────┘              └───────────────────────┘
```

## 钱包预扣扣减

- **正常路径**：Redis Lua 原子扣减 `{GET, CHECK, DECRBY, EXPIRE}`，5 行脚本
- **Redis 不可用**：自动降级到 MySQL CAS 同步扣减
- **恢复**：5s 周期 Ping → 恢复后 FlushDB 清脏 key → NOT_CACHED 重载

```
Redis Lua:
  balance = GET key
  if not balance → NOT_CACHED（加载 MySQL 重试）
  if balance < amount → INSUFFICIENT
  DECRBY key amount
  EXPIRE key 3600
  return OK
```

- wallet key TTL=1h，每次扣减自动续期。进程崩溃后 1h 自动释放

## 连击计数

```redis
INCR combo:{room_id}:{user_id}:{gift_id}
EXPIRE combo:{room_id}:{user_id}:{gift_id} 3
```

TTL=3s = 连击窗口。全服务共享 Redis → WS 网关可读 combo 数广播房间。

## 排行榜

两个查询接口：

```go
GET /api/v1/room/:room_id/leaderboard?top=50  // ZSet TopN，精确
GET /api/v1/room/:room_id/rank?user_id=1001   // 个人排名，粗估
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

**更新**：Lua 脚本批量 HINCRBY，10 次 hash 操作，O(log 桶数)
**查询**：HMGet 路径节点 + 桶内线性插值 → 排名

**查询链路**：
```
Rank(user_id):
  → ZSet 有 → ZREVRANK 精确
  → ZSet 无 → counter:{room}:{user} 拿积分 → HMGet 线段树 → 粗估排名
```

ZSet 只存 Top 10000，超出的走线段树。优势：key 数量固定，人数再大不膨胀 ZSet 大 key。

## 落盘

Settle Consumer 每 100ms 拉取，按 `(user, anchor, gift, room)` 分组：

```go
// 100ms 窗口内收到
events := [{user=1, room=2, anchor=3, gift=1, count=5},
           {user=1, room=2, anchor=3, gift=1, count=3}]

// 分组后 → 1 次 MySQL 扣减 + 1 条流水
group(user=1, anchor=3, gift=1, room=2):
  totalCount  = 8
  totalAmount = 8 * price
```

MongoDB 乐观锁 CAS 扣减 + `INSERT ... ON DUPLICATE KEY UPDATE` 防重 + LoadBalance 同步 Redis。

## 恢复机制

| 场景 | 恢复 |
|---|---|
| 进程正常启动 | warmup 钱包 → Redis key 从 MySQL 加载 |
| 进程崩溃 | MQ 持久化，consumer 追平 |
| Redis 崩溃 | 5s Ping 恢复 → FlushDB → NOT_CACHED 重载 |
| MySQL 慢 | settle consumer 阻塞 → 自然背压 |
| 钱包 key 不一致 | TTL 1h 后自动过期 → NOT_CACHED 重载 |

## 文件结构

```
internal/
├── cache/         Redis 缓存（wallet, gift, idempotent）
├── config/        Viper 配置
├── handler/       HTTP handler（gift）
├── leaderboard/   排行榜（线段树 + counter + handler + MQ consumer）
├── middleware/    Auth middleware
├── model/         DB 结构体
├── mq/            RabbitMQ（producer, consumer, settle_consumer）
├── repository/    MySQL 操作
├── service/       业务层（gift）
└── pkg/           token, idgen
```

## 面试话术

> "连击计数用 Redis INCR + TTL，跨服务可见，WS 网关直接读。落盘走 RabbitMQ 异步批量消费，100ms 聚合窗口，一次 DB 写代替多次。排行榜外挂线段树粗估，固定 2047 个节点存 Redis Hash，解决 TopN ZSet 放不下全量用户的大 key 问题。权衡是多了 MQ 依赖，部署重一些，但可靠性从尽力交付提升到持久化级别。"
