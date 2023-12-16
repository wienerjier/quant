


-- 创建表

CREATE TABLE `hs300_daily_price_qfq` (
    `id` INT NOT NULL AUTO_INCREMENT,
    `symbol` VARCHAR(32) NOT NULL,
    `name` VARCHAR(32) NOT NULL,
    `price_date` DATETIME NOT NULL,
    `open_price` DECIMAL(19,4) NULL,
    `close_price` DECIMAL(19,4) NULL,
    `high_price` DECIMAL(19,4) NULL,
    `low_price` DECIMAL(19,4) NULL,
    `amount` int NULL,
    `turnover` BIGINT NULL, 
    `amplitude` DECIMAL(19,4) NULL,
    `chg` DECIMAL(19,4) NULL,
    `change` DECIMAL(19,4) NULL,
    `TOR` DECIMAL(19,4) NULL, 
    `created_date` DATETIME NOT NULL,
    PRIMARY KEY (`id`) 
    ) 
    ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;