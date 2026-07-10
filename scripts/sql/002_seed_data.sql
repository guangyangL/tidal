-- 种子数据（Go 启动时 autoMigrate 自动执行）
-- 此文件可供手动场景或 Docker MySQL initdb 使用

-- 礼物配置
INSERT INTO t_gift_config (gift_id, name, price, status) VALUES
(1, '荧光棒',   10,   1),
(2, '心动盲盒', 30,   1),
(3, '跑车',     100,  1),
(4, '火箭',     300,  1),
(5, '嘉年华',   1000, 1)
ON DUPLICATE KEY UPDATE name=VALUES(name), price=VALUES(price);

-- 压测用户 (10000-10999, 共1000人)
INSERT INTO t_user_wallet (user_id, balance, wallet_type)
SELECT 10000 + n, 1000000, 0 FROM (
    SELECT @row := @row + 1 AS n FROM information_schema.columns a, information_schema.columns b,
    (SELECT @row := 0) r LIMIT 1000
) t ON DUPLICATE KEY UPDATE balance=1000000, version=0;
