# Tidal 压测报告

> **日期:** 2026-07-10
> **版本:** v2.0
> **环境:** macOS (Apple Silicon), Go 1.25, Docker Compose 本地部署

---

## 1. 测试环境

| 项目 | 配置 |
| --- | --- |
| CPU | Apple M-series (8 核) |
| 内存 | 16GB+ |
| Go 版本 | 1.25.4 |
| MySQL | 8.0 (Docker, 默认配置) |
| Redis | 7-alpine (Docker, 单实例) |
| RabbitMQ | 3-management-alpine (Docker, 单实例) |
| 压测工具 | vegeta 12.13.0 |
| 连接池 | MySQL 50/20, go-redis 默认 |

## 2. 测试方法

**测试端点：**

- `POST /api/v1/gift/send` — 送礼接口（写路径，全链路）
- `GET /api/v1/room/:room_id/leaderboard` — 排行榜 TopN（读路径）
- `GET /api/v1/room/:room_id/rank` — 个人排名（读路径）

**测试数据：**

- 1005 个用户，每人 1,000,000 金币
- 5 种礼物（荧光棒 10、跑车 1000、火箭 5000、嘉年华 30000、心动盲盒 50）
- 压测使用 gift_id=1（荧光棒 10 coins），最大化单用户可发送次数

**测试流程：**

1. 生成 N 个 vegeta JSON 格式 target（随机 user/room/anchor，固定 gift=1）
2. vegeta attack 按指定 QPS 发送，持续 30s
3. 每次测试前重置钱包余额、重启服务

## 3. 写路径压测结果

### 3.1 综合数据

| 测试 | QPS | 时长 | 总请求 | 成功率 | P50 | P95 | P99 | Max | Throughput |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 预热 | 500 | 10s | 5,000 | 99.92% | 1.3ms | 2.3ms | 3.2ms | 9.7ms | 500/s |
| 基准 | 1,000 | 30s | 30,000 | 99.85% | 1.5ms | 3.1ms | 4.4ms | 102ms | 998/s |
| 高压 | 2,000 | 30s | 60,000 | 99.85% | 3.2ms | 5.4ms | 13.6ms | 52ms | 1,996/s |
| 极限 | 3,000 | 30s | 90,000 | 99.52% | 6.2ms | 27.8ms | 61.1ms | 106ms | 2,985/s |

### 3.2 延迟分布

```
500 QPS:   [█████████████████████████████████████████████████] 92.6% < 2ms
                                              [██]           7.3% 2-5ms

1000 QPS:  [████████████████████████████████████████]         77.1% < 2ms
                               [█████████]                    22.2% 2-5ms

2000 QPS:  [█████████████████████████████████████████████████] 93.0% 2-5ms
                                         [███]                 5.8% 5-10ms

3000 QPS:            [████████████]                             28.5% 2-5ms
                     [██████████████████████]                   48.9% 5-10ms
                              [███████]                         15.0% 10-20ms
                                   [███]                        6.0% 20-50ms
                                        [▌]                     1.6% 50-100ms
```

### 3.3 失败分析

所有失败均为 `402 Payment Required`（余额不足），无服务端 5xx 错误（除 5000 QPS 触发本地端口耗尽外）。

- 原因：随机用户分布下，部分用户被命中频率更高，钱包提前耗尽
- 验证：`UPDATE t_user_wallet SET balance=1000000` 后重试即可恢复
- 结论：**无超卖、无服务崩溃、无数据不一致**

## 4. 读路径压测结果

| 测试 | QPS | 时长 | 成功率 | P50 | P95 | P99 | Max |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Leaderboard TopN + Rank | 500 | 10s | 100% | 0.30ms | 0.46ms | 0.68ms | 8.0ms |

- 读路径纯 Redis 操作（ZREVRANGE / ZSCORE / ZREVRANK），不涉及 MySQL
- P99 < 1ms，QPS 天花板远高于写路径
- 排行榜数据结构正确：10 个房间各有独立 ZSet，各 1000 个成员

## 5. 系统资源

| 指标 | 空闲 | 3000 QPS 压测中 |
| --- | --- | --- |
| 服务内存 (RSS) | ~20MB | ~73MB |
| 服务 CPU | 0% | ~12% |
| MySQL 记录数 | 0 | 44,673 |
| MQ 队列堆积 | 0 | 0（实时消费） |
| Redis Combo Keys | 0 | >100（压测后 3s 自动过期） |
| Redis ZSet 大小 | — | 10 个房间 × 1000 成员 |
| Redis 线段树 | — | 10 个房间，每房间 2047 个 Hash field |

## 6. 瓶颈分析

### 6.1 当前瓶颈：Redis

写路径每请求执行的操作：
- `SETNX` (幂等)
- `HGET` (礼物价格)
- `EVAL` (Lua 预扣)
- `INCR` + `EXPIRE` (连击)
- `ZINCRBY` + `INCRBY` (排行榜)
- `PUBLISH` (MQ 投递)

**合计 6-8 次 Redis 操作/请求。** 在 3000 QPS 时，Redis 承受约 20,000 ops/s，本地单实例 Redis 已经接近 CPU 瓶颈。

### 6.2 MySQL 不是瓶颈

- 44,673 条流水 = 3000 QPS × 30s 聚合到 100ms 批次 × 分组 ≈ 实际 DB 写入远低于 QPS
- `t_gift_record` 只有 44,673 行，靠 `batch_token` 唯一索引保证幂等
- MQ 队列实时清空，无积压

### 6.3 5000 QPS 测试中的端口耗尽

```
dial tcp: bind: can't assign requested address
```

这是 **macOS 本地压测的已知限制**（非服务端问题）：
- macOS 默认 ephemeral port 范围约 16,384 个
- vegeta 以 5000 QPS 速率打开连接，TIME_WAIT 回收不及时导致端口耗尽
- 解决方案：`sudo sysctl -w net.inet.tcp.msl=1000` 降低 TIME_WAIT 时间，或使用 `vegeta -keepalive=true`

### 6.4 推测上限

基于当前单机单实例 Redis 的配置：

| 场景 | 预估上限 | 限制因素 |
| --- | --- | --- |
| 单实例写 | ~5,000 QPS | Redis 单线程 CPU |
| 单实例读 | >50,000 QPS | Gin 框架 / 网卡 |
| Redis Cluster 写 | >20,000 QPS | MQ 吞吐 / MySQL 连接池 |

## 7. 验证项

| 验证项 | 结果 |
| --- | --- |
| 幂等防重 | `uk_batch_token` 唯一索引，44K 条记录 0 重复 |
| 乐观锁扣减 | CAS `WHERE version=? AND balance>=?`，0 超卖 |
| MQ 无积压 | 两个队列始终 0 堆积消息 |
| 连击过期 | combo key TTL 3s，压测停止后 3s 全部清理 |
| 线段树一致性 | 10 个房间 ZSet 各有 1000 成员，线段树计数匹配 |
| 钱包最终一致 | 余额总和 ≈ 初始总额 - 流水总额 |

## 8. 运行压测

### 裸机

```bash
# 1. 启动基础设施
make docker-up

# 2. 添加测试用户（1000 个）
docker exec tidal-mysql mysql -u root -proot tidal -e "
  INSERT INTO t_user_wallet (user_id, balance, wallet_type)
  SELECT 10000 + n, 1000000, 0 FROM (
    SELECT @row := @row + 1 AS n FROM information_schema.columns a, information_schema.columns b,
    (SELECT @row := 0) r LIMIT 1000
  ) t ON DUPLICATE KEY UPDATE balance=1000000, version=0
"

# 3. 编译并启动服务
make build && ./bin/tidal &

# 4. 运行压测
bash scripts/loadtest.sh 1000 30    # 1000 QPS × 30s
bash scripts/loadtest.sh 3000 30    # 3000 QPS × 30s

# 5. 查看结果
vegeta report /tmp/tidal_result_1000.bin
vegeta report -type='hist[0,2ms,5ms,10ms,20ms,50ms,100ms]' /tmp/tidal_result_1000.bin
```

### 容器化 (2C/4G)

```bash
# 1. 构建镜像
docker build -f deploy/Dockerfile -t tidal:latest .

# 2. 启动容器（资源限制 + 挂载 Docker 网络）
docker run -d --name tidal-loadtest \
  --cpus=2 --memory=4g \
  --network deploy_tidal-net \
  -p 8080:8080 \
  tidal:latest

# 3. 检查
docker stats tidal-loadtest --no-stream

# 4. 压测（脚本同上）
bash scripts/loadtest.sh 2000 30
bash scripts/loadtest.sh 3000 30

# 5. 清理
docker rm -f tidal-loadtest
```

## 9. 容器化压测（2C/4G 资源限制）

### 9.1 环境

| 项目 | 配置 |
| --- | --- |
| 镜像 | `tidal:latest` (multi-stage, Alpine 3.20) |
| CPU 限制 | `--cpus=2` (2 核) |
| 内存限制 | `--memory=4g` (4GB) |
| 基础设施 | MySQL/Redis/RabbitMQ 均为独立容器 |
| 网络 | Docker bridge (`deploy_tidal-net`) |

### 9.2 写路径结果

| 测试 | QPS | 时长 | 总请求 | 成功率 | P50 | P95 | P99 | Max | Throughput |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 基准 | 1,000 | 30s | 30,000 | 99.81% | 0.65ms | 1.6ms | 4.4ms | 61.8ms | 998/s |
| 高压 | 2,000 | 30s | 60,000 | 99.83% | 0.76ms | 1.8ms | 3.3ms | 57.7ms | 1,997/s |
| 极限 | 3,000 | 30s | 90,001 | 99.77% | 1.3ms | 4.0ms | 31.6ms | 138.7ms | 2,993/s |
| 超限* | 5,000 | 30s | 114,666 | 86.59% | 48.5ms | 11s | 12s | 18.3s | 2,327/s |

*5000 QPS: macOS 端口耗尽，实际有效 QPS ~3,800，连接成功部分 99,285 次全部正确处理

### 9.3 容器延迟分布

```
1000 QPS:  [█████████████████████████████████████████████████████████] 97.2% < 2ms

2000 QPS:  [█████████████████████████████████████████████████████████] 96.6% < 2ms

3000 QPS:  [████████████████████████████████████████████████████]       82.8% < 2ms
                             [█████████]                                13.4% 2-5ms
                                      [██]                               3.1% 5-50ms
```

### 9.4 容器资源消耗

| 指标 | 空闲 | 3000 QPS 峰值 |
| --- | --- | --- |
| CPU | ~0% | 10.35% (of 2 cores) |
| 内存 | ~30MB | 480MB / 4GB (11.7%) |
| 网络 I/O | — | ~local bridge, 无瓶颈 |

- 2C/4G 资源远未触顶，瓶颈在 Redis 和端口映射
- 内存 480MB 主要来自 Go GC 堆 + Redis 连接池 + MQ channel 缓冲

### 9.5 绕过端口映射：Docker 内部网络直压

Docker Desktop 的端口映射（`-p 8080:8080`）在 macOS 上经过额外代理层，导致高 QPS 时端口耗尽。将 vegeta 也跑在容器内，通过 Docker bridge 网络直连 Tidal 容器，完全消除端口瓶颈。

```bash
# 获取容器 IP
docker inspect tidal-loadtest --format '{{.NetworkSettings.Networks.deploy_tidal_net.IPAddress}}'

# vegeta alpine 容器直压
docker run --rm --network deploy_tidal-net \
  -v /tmp/targets.json:/tmp/targets.json:ro \
  alpine:3.20 sh -c '
    apk add --no-cache curl && 
    curl -sL https://github.com/tsenart/vegeta/releases/download/v12.13.0/vegeta_12.13.0_linux_arm64.tar.gz | tar xz -C /usr/local/bin/ vegeta &&
    vegeta attack -format=json -rate=8000 -duration=30s -targets=/tmp/targets.json | vegeta report -type=text
  '
```

### 9.6 真实天花板数据

| 目标 QPS | 实际吞吐 | 成功率 | P50 | P95 | P99 | Max | CPU | 内存 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 5,000 | **4,983/s** | 99.67% | 1.0ms | 3.6ms | 38.0ms | 119ms | ~10% | 48MB |
| 8,000 | **7,589/s** | 96.81% | 10.4ms | 659ms | 1.16s | 1.47s | ~10% | 480MB |
| 10,000 | 3,916/s | 77.63% | 4.5s | 12.6s | 14.2s | 19.6s | ~10% | 477MB |

**饱和点分析：**

- 5000 QPS：游刃有余，P99 仅 38ms，吞吐接近目标
- 8000 QPS：开始出现延迟尖刺（P99=1.16s），但成功率仍 >96%，吞吐 7,589/s
- 10000 QPS：**过饱和**，实际吞吐反降至 3,916/s，延迟全面爆炸

**瓶颈定位：**

容器 CPU 全程仅 9-10%，2 核远未用完。真正的瓶颈是 **Redis 单线程**：
- 每请求 7 次 Redis 操作（SETNX + HGET + EVAL + INCR + EXPIRE + ZINCRBY + INCRBY）
- 8000 QPS × 7 = **56,000 Redis ops/s**，接近单实例 Redis 上限（~80k-100k ops/s 简单命令）
- Lua 脚本（Eval）比简单命令慢 3-5x，实际 Redis CPU 已达瓶颈

## 10. 端口耗尽问题详解

### 10.1 原因
macOS 的 TCP 临时端口范围默认 49152-65535（~16K 个），TCP 连接关闭后进入 `TIME_WAIT`（2×MSL=30s），端口 30s 内不可复用。vegeta 以 5000 QPS 创建连接时，16K 端口瞬间耗尽。

### 10.2 临时缓解（macOS）
```bash
sudo sysctl -w net.inet.ip.portrange.first=10000    # 扩大到 55K 端口
sudo sysctl -w net.inet.tcp.msl=1000                # TIME_WAIT 缩短到 2s
```

### 10.3 根本解决：绕过端口映射
macOS → Docker 端口映射走额外代理，既慢又消耗端口。vegeta 直接跑在 Docker 网络内，走 bridge 直连，零端口消耗。

## 11. 裸机 vs 容器 vs 容器内网对比

| QPS | 裸机 P99 | 容器(端口映射) P99 | 容器(内网) P99 | 真实天花板 |
| --- | --- | --- | --- | --- |
| 1,000 | 4.4ms | 4.4ms | — | — |
| 2,000 | 13.6ms | 3.3ms | — | — |
| 3,000 | 61.1ms | 31.6ms | — | — |
| 5,000 | 端口耗尽 | 端口耗尽 | **38.0ms** | 游刃有余 |
| 8,000 | — | — | **1.16s** | 接近饱和 |
| 10,000 | — | — | **14.2s** | 过饱和 |

**容器反而更快的原因：**

- Docker Desktop 的 Linux VM **TCP/IP 栈远优于 macOS loopback**
- 容器 cgroup 隔离避免了 macOS 后台进程的 CPU 争抢
- `--cpus=2` 帮助 Go 调度器避免过度并行化导致的 GC 抖动
- 裸机 Go runtime 可见 8 核，创建更多 P（逻辑处理器），导致更多 GC 和上下文切换

## 12. 结论

- **真实饱和吞吐约 7,500-8,000 QPS**（2C/4G 容器，单实例 Redis）
- 瓶颈在 **Redis 单线程**（每请求 7 次 Redis 操作，Lua 脚本较重），不是 CPU 或内存
- **5000 QPS 游刃有余**（P99=38ms，99.67% 成功），是生产推荐值
- **MQ 批量落盘无积压**，100ms ticker 消费能力充足
- **MySQL CAS 乐观锁无冲突退化**，重试 3 次机制有效
- **读路径极快**（P99 < 1ms），排行榜查询对写路径无影响
- **2C/4G 配置远未触顶**，瓶颈全在 Redis，扩展应优先 Redis Cluster 或 Pipeline 合并
- macOS 压测需注意端口耗尽，**容器内网直压** 是最佳实践
