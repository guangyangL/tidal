-- Tidal 数据库表结构
-- Go 启动时 autoMigrate 会自动执行 CREATE TABLE IF NOT EXISTS
-- 此文件作为正式 DDL 参考留存

-- 用户虚拟钱包
CREATE TABLE `t_user_wallet` (
    `user_id`       BIGINT   NOT NULL COMMENT '用户ID',
    `balance`       BIGINT   NOT NULL DEFAULT 0 COMMENT '余额(分)',
    `wallet_type`   TINYINT  NOT NULL DEFAULT 0 COMMENT '0-充值币 1-赠送币',
    `version`       INT      NOT NULL DEFAULT 0 COMMENT '乐观锁版本号',
    `update_time`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户虚拟钱包';

-- 礼物配置
CREATE TABLE `t_gift_config` (
    `gift_id`     INT          NOT NULL AUTO_INCREMENT COMMENT '礼物ID',
    `name`        VARCHAR(64)  NOT NULL COMMENT '礼物名称',
    `price`       BIGINT       NOT NULL COMMENT '单价(分)',
    `status`      TINYINT      NOT NULL DEFAULT 1 COMMENT '1-上架 0-下架',
    `extra`       JSON         DEFAULT NULL COMMENT '扩展配置',
    `create_time` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`gift_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='礼物配置表';

-- 礼物投递流水
CREATE TABLE `t_gift_record` (
    `id`            BIGINT       NOT NULL AUTO_INCREMENT,
    `batch_token`   VARCHAR(64)  NOT NULL COMMENT '幂等Token',
    `room_id`       BIGINT       NOT NULL COMMENT '直播间ID',
    `user_id`       BIGINT       NOT NULL COMMENT '送礼用户ID',
    `anchor_id`     BIGINT       NOT NULL COMMENT '接收主播ID',
    `gift_id`       INT          NOT NULL COMMENT '礼物ID',
    `total_amount`  BIGINT       NOT NULL COMMENT '总计扣款(分)',
    `status`        TINYINT      NOT NULL DEFAULT 1 COMMENT '1-已扣款 2-分账成功 3-失败 4-死信',
    `create_time`   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_batch_token` (`batch_token`),
    KEY `idx_room_anchor` (`room_id`, `anchor_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='礼物投递流水';
