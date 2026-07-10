# Tidal · 高并发直播打赏引擎

[![Go](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue)](./LICENSE)

**Tidal（潮汐）** 是一个高并发直播连击打赏与结算引擎，专为直播 PK/赛事等流量洪峰场景设计。

> 请求路径只做快操作，慢操作全部异步。
> Redis Lua 预扣 → MQ 异步落盘 → MySQL CAS 结算

## 架构

```
POST /api/v1/gift/send
  ├─ 幂等校验  (Redis SETNX, 600s)
  ├─ 礼物价格  (Redis HGET, cache miss 回 MySQL)
  ├─ 钱包预扣  (Redis Lua 原子 DECRBY)
  ├─ 连击计数  (Redis INCR + EXPIRE, 600s)
  ├─ 排行榜    (ZSet ZINCRBY + MQ 线段树)
  └─ MQ 投递   (gift.settle 异步落盘)

MQ Consumer (100ms batch)
  ├─ 分组聚合  (按 user/room/anchor/gift hash)
  ├─ MySQL CAS (乐观锁扣减, retry 3 次)
  ├─ SyncBalance (Lua 只降不升, 防止超卖)
  └─ INSERT 流水 (batch_token 唯一键幂等)
```

## 压测数据

| QPS | 成功率 | P50 | P95 | P99 |
| --- | --- | --- | --- | --- |
| 1,000 | 99.85% | 0.6ms | 1.6ms | 4.4ms |
| 2,000 | 99.83% | 0.8ms | 1.8ms | 3.3ms |
| 3,000 | 99.73% | 0.6ms | 2.0ms | 35.5ms |
| 5,000 | 99.67% | 1.0ms | 3.6ms | 38.0ms |
| 8,000 | 96.81% | 10.4ms | 659ms | 1.16s |

> 2C/4G 容器, 单实例 Redis。饱和吞吐 ~7,500-8,000 QPS，瓶颈在 Redis 单线程。

详见 [load-test-report.md](./docs/load-test-report.md)

## 快速开始

### 环境要求

- Go 1.25+
- Docker & Docker Compose

### 启动基础设施

```bash
make docker-up
# MySQL 8.0 (3306), Redis 7 (6379), RabbitMQ 3 (5672/15672)
```

### 启动服务

```bash
make run
# 服务监听 :8080, 自动建表 + 种子数据
```

### 发送请求

```bash
curl -X POST http://localhost:8080/api/v1/gift/send \
  -H "Content-Type: application/json" \
  -H "X-User-ID: 1001" \
  -H "X-Request-ID: $(uuidgen)" \
  -d '{"room_id":1, "anchor_id":1, "gift_id":1}'
```

### 查询排行榜

```bash
# Top 50
curl "http://localhost:8080/api/v1/room/1/leaderboard?top=50"

# 个人排名
curl "http://localhost:8080/api/v1/room/1/rank?user_id=1001"
```

## 项目结构

```
cmd/server/          # 启动入口 + 自动建表
config/              # 配置文件
deploy/              # Dockerfile + docker-compose
docs/                # 设计文档 + 压测报告
internal/
  cache/             # Redis 访问层 (wallet Lua, gift, dedup)
  config/            # Viper 配置加载
  event/             # MQ 事件结构体
  handler/           # Gin HTTP handler
  leaderboard/       # 排行榜 (ZSet + 线段树 + MQ consumer)
  middleware/         # Auth middleware
  model/             # DB 结构体
  mq/                # RabbitMQ 封装
  repository/        # MySQL 数据访问 (sqlx)
  service/           # 业务层 (送礼流程 + settle consumer)
pkg/
  idgen/             # Sonyflake ID 生成
  token/             # batch_token 编码
scripts/             # 压测脚本 + DDL
```

## 核心设计

- **幂等**: `X-Request-ID` → Redis SETNX + `batch_token` UNIQUE KEY 双重保障
- **预扣**: Redis Lua 原子 `{GET, CHECK, DECRBY, EXPIRE}`，杜绝超卖
- **一致**: SyncBalance Lua 脚本保证 Redis 永不高估余额
- **落盘**: 100ms 批量聚合 + MySQL CAS 乐观锁 (retry 3)
- **排行**: ZSet 精确 TopN + 线段树粗估排名

## License

MIT
