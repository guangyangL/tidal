-- ============================================================
-- Tidal (潮汐) 高并发直播连击打赏与结算引擎
-- 基础数据库表结构
-- 版本: v1.0
-- 说明: 仅3张核心表，严格遵循"高内聚、零冗余、留钩子"原则
-- ============================================================

-- ----------------------------------------------------------
-- 1. 用户虚拟钱包表
-- 职责：管理用户资产，高并发扣减的核心阵地
-- 设计意图：
--   - version: 乐观锁，替代悲观锁应对高并发
--   - wallet_type: 区分充值币/赠送币，支撑运营活动场景
-- ----------------------------------------------------------
CREATE TABLE `t_user_wallet` (
    `user_id`       BIGINT   NOT NULL COMMENT '用户ID',
    `balance`       BIGINT   NOT NULL DEFAULT 0 COMMENT '虚拟金币余额(单位:分，避免浮点数)',
    `wallet_type`   TINYINT  NOT NULL DEFAULT 0 COMMENT '钱包类型: 0-充值币, 1-赠送币(不可提现)',
    `version`       INT      NOT NULL DEFAULT 0 COMMENT '乐观锁版本号，CAS更新',
    `update_time`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户虚拟钱包';

-- ----------------------------------------------------------
-- 2. 礼物配置表
-- 职责：礼物的静态元数据，极少变更
-- 设计意图：
--   - 全量缓存在 Redis 中，送礼链路完全不查 MySQL
--   - extra 字段为运营活动（限时折扣、特效加成）留扩展口
-- ----------------------------------------------------------
CREATE TABLE `t_gift_config` (
    `gift_id`     INT          NOT NULL AUTO_INCREMENT COMMENT '礼物ID',
    `name`        VARCHAR(64)  NOT NULL COMMENT '礼物名称',
    `price`       BIGINT       NOT NULL COMMENT '礼物单价(单位:分)',
    `status`      TINYINT      NOT NULL DEFAULT 1 COMMENT '状态: 1-上架, 0-下架',
    `extra`       JSON         DEFAULT NULL COMMENT '扩展字段: 限时折扣、特效加成等活动配置',
    `create_time` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`gift_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='礼物配置表';

-- ----------------------------------------------------------
-- 3. 礼物投递流水表（核心账本）
-- 职责：记录每次 MQ 批次聚合后的结算结果
-- 设计意图：
--   - batch_token: 业务语义幂等键，由 user_id + anchor_id + window_start_ms
--     经 base62 编码生成，趋势递增避免 UUID 的随机 IO 页分裂
--   - total_amount: 该批次聚合后的实际扣款总金额
-- ----------------------------------------------------------
CREATE TABLE `t_gift_record` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '流水ID',
    `batch_token`   VARCHAR(64)  NOT NULL COMMENT '幂等控制Token(业务语义编码)，防重放',
    `room_id`       BIGINT       NOT NULL COMMENT '直播间ID',
    `user_id`       BIGINT       NOT NULL COMMENT '送礼用户ID',
    `anchor_id`     BIGINT       NOT NULL COMMENT '接收主播ID',
    `gift_id`       INT          NOT NULL COMMENT '礼物ID',
    `total_amount`  BIGINT       NOT NULL COMMENT '该批次扣款总金额(单位:分)',
    `status`        TINYINT      NOT NULL DEFAULT 1 COMMENT '结算状态: 1-已扣款, 2-分账成功, 3-失败待重试, 4-死信',
    `create_time`   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_batch_token` (`batch_token`),
    KEY `idx_room_anchor` (`room_id`, `anchor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='礼物投递流水表(批次聚合)';
