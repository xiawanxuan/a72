-- ============================================================
-- 智能制造数据中台 - 测试数据库初始化脚本
-- 用于创建演示用的 MySQL 业务表，方便验证同步功能
-- ============================================================

-- 创建数据库（如不存在）
CREATE DATABASE IF NOT EXISTS smart_manufacturing
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE smart_manufacturing;

-- -----------------------------------------------------------
-- 1. 设备基础信息表 device_info
-- -----------------------------------------------------------
DROP TABLE IF EXISTS `device_info`;
CREATE TABLE `device_info` (
    `device_id`      VARCHAR(64)   NOT NULL           COMMENT '设备唯一ID',
    `device_name`    VARCHAR(128)  NOT NULL           COMMENT '设备名称',
    `device_type`    VARCHAR(64)   NOT NULL           COMMENT '设备类型:CNC/ROBOT/SENSOR/PLC...',
    `device_status`  VARCHAR(32)   NOT NULL DEFAULT 'IDLE' COMMENT '设备状态:RUNNING/IDLE/FAULT/MAINTENANCE',
    `location`       VARCHAR(256)  DEFAULT NULL       COMMENT '设备位置:车间A-产线2-工位5',
    `install_date`   DATETIME      DEFAULT NULL       COMMENT '安装日期',
    `manufacturer`   VARCHAR(128)  DEFAULT NULL       COMMENT '制造商',
    `model`          VARCHAR(128)  DEFAULT NULL       COMMENT '设备型号',
    `operator`       VARCHAR(64)   DEFAULT NULL       COMMENT '当前操作员',
    `create_time`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
    `update_time`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    PRIMARY KEY (`device_id`),
    KEY `idx_device_name` (`device_name`),
    KEY `idx_device_status` (`device_status`),
    KEY `idx_device_type` (`device_type`),
    KEY `idx_operator` (`operator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备基础信息表';

-- 插入测试数据
INSERT INTO `device_info`
(`device_id`, `device_name`, `device_type`, `device_status`, `location`, `install_date`, `manufacturer`, `model`, `operator`)
VALUES
('DEV-CNC-001', 'CNC加工中心A1',   'CNC',    'RUNNING', '车间A-产线1-工位1', '2023-06-15 10:00:00', 'FANUC',   'α-D14MiA5',   '张三'),
('DEV-CNC-002', 'CNC加工中心A2',   'CNC',    'IDLE',    '车间A-产线1-工位2', '2023-06-15 10:00:00', 'FANUC',   'α-D14MiA5',   '李四'),
('DEV-ROB-001', '焊接机器人B1',    'ROBOT',  'RUNNING', '车间B-产线3-工位1', '2023-08-20 09:00:00', 'KUKA',    'KR 500 R2830', '王五'),
('DEV-PLC-001', '主控制柜PLC',     'PLC',    'RUNNING', '中央控制室',        '2023-01-10 08:00:00', 'SIEMENS', 'S7-1500',      '赵六'),
('DEV-SEN-001', '温湿度传感器组1', 'SENSOR', 'RUNNING', '车间A-产线1',       '2023-09-01 12:00:00', 'OMRON',   'E5CC-RX2ASM',  NULL),
('DEV-CNC-003', 'CNC加工中心A3',   'CNC',    'FAULT',   '车间A-产线2-工位1', '2024-01-05 10:00:00', 'MAZAK',   'INTEGREX i-200', '孙七');

-- -----------------------------------------------------------
-- 2. 设备运行指标表 device_metrics
-- -----------------------------------------------------------
DROP TABLE IF EXISTS `device_metrics`;
CREATE TABLE `device_metrics` (
    `metric_id`    VARCHAR(128)  NOT NULL           COMMENT '指标记录ID',
    `device_id`    VARCHAR(64)   NOT NULL           COMMENT '设备ID(关联device_info.device_id)',
    `metric_type`  VARCHAR(64)   NOT NULL           COMMENT '指标类型:TEMPERATURE/PRESSURE/CURRENT/SPINDLE_SPEED/VIBRATION',
    `metric_value` DOUBLE        NOT NULL           COMMENT '指标数值',
    `metric_unit`  VARCHAR(32)   DEFAULT NULL       COMMENT '单位:°C/MPa/A/rpm/mm/s',
    `collect_time` DATETIME(3)   NOT NULL           COMMENT '采集时间(毫秒精度)',
    `quality_flag` VARCHAR(16)   NOT NULL DEFAULT 'GOOD' COMMENT '质量标志:GOOD/BAD/UNCERTAIN/SUBSTITUTED',
    `create_time`  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`metric_id`),
    KEY `idx_device_time` (`device_id`, `collect_time`),
    KEY `idx_metric_type` (`metric_type`),
    KEY `idx_quality` (`quality_flag`),
    KEY `idx_collect_time` (`collect_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备运行指标表'
PARTITION BY RANGE (TO_SECONDS(`collect_time`)) (
    PARTITION p202406 VALUES LESS THAN (TO_SECONDS('2024-07-01 00:00:00')),
    PARTITION p202407 VALUES LESS THAN (TO_SECONDS('2024-08-01 00:00:00')),
    PARTITION p202408 VALUES LESS THAN (TO_SECONDS('2024-09-01 00:00:00')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

-- 插入测试指标数据
INSERT INTO `device_metrics`
(`metric_id`, `device_id`, `metric_type`, `metric_value`, `metric_unit`, `collect_time`, `quality_flag`)
VALUES
(UUID_SHORT(), 'DEV-CNC-001', 'SPINDLE_SPEED', 8500.0,  'rpm',    NOW(3) - INTERVAL 10 SECOND, 'GOOD'),
(UUID_SHORT(), 'DEV-CNC-001', 'TEMPERATURE',   42.5,    '°C',     NOW(3) - INTERVAL 10 SECOND, 'GOOD'),
(UUID_SHORT(), 'DEV-CNC-001', 'CURRENT',       15.3,    'A',      NOW(3) - INTERVAL 10 SECOND, 'GOOD'),
(UUID_SHORT(), 'DEV-CNC-001', 'VIBRATION',     0.02,    'mm/s',   NOW(3) - INTERVAL 10 SECOND, 'GOOD'),
(UUID_SHORT(), 'DEV-ROB-001', 'TEMPERATURE',   38.9,    '°C',     NOW(3) - INTERVAL 5 SECOND,  'GOOD'),
(UUID_SHORT(), 'DEV-ROB-001', 'CURRENT',       22.1,    'A',      NOW(3) - INTERVAL 5 SECOND,  'GOOD'),
(UUID_SHORT(), 'DEV-SEN-001', 'TEMPERATURE',   25.6,    '°C',     NOW(3),                       'GOOD'),
(UUID_SHORT(), 'DEV-SEN-001', 'HUMIDITY',      48.2,    '%RH',    NOW(3),                       'GOOD'),
(UUID_SHORT(), 'DEV-CNC-003', 'ERROR_CODE',    1024.0,  'CODE',   NOW(3),                       'BAD');

-- -----------------------------------------------------------
-- 3. 设备告警记录表 device_alerts
-- -----------------------------------------------------------
DROP TABLE IF EXISTS `device_alerts`;
CREATE TABLE `device_alerts` (
    `alert_id`       VARCHAR(64)   NOT NULL           COMMENT '告警ID',
    `device_id`      VARCHAR(64)   NOT NULL           COMMENT '设备ID',
    `alert_type`     VARCHAR(64)   NOT NULL           COMMENT '告警类型:TEMP_HIGH/CURRENT_OVERLOAD/COMM_ERROR/MAINT_DUE',
    `alert_level`    VARCHAR(16)   NOT NULL           COMMENT '告警等级:INFO/WARNING/CRITICAL/EMERGENCY',
    `alert_message`  TEXT          DEFAULT NULL       COMMENT '告警详细信息',
    `alert_time`     DATETIME(3)   NOT NULL           COMMENT '告警发生时间',
    `acknowledged`   TINYINT(1)    NOT NULL DEFAULT 0 COMMENT '是否已确认:0=未确认 1=已确认',
    `acknowledge_by` VARCHAR(64)   DEFAULT NULL       COMMENT '确认人',
    `acknowledge_time` DATETIME    DEFAULT NULL       COMMENT '确认时间',
    `resolved`       TINYINT(1)    NOT NULL DEFAULT 0 COMMENT '是否已解决:0=未解决 1=已解决',
    `resolve_time`   DATETIME      DEFAULT NULL       COMMENT '解决时间',
    `resolve_note`   TEXT          DEFAULT NULL       COMMENT '解决说明',
    `operator`       VARCHAR(64)   DEFAULT NULL       COMMENT '当前负责人',
    `create_time`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`alert_id`),
    KEY `idx_device_alert` (`device_id`, `alert_time`),
    KEY `idx_alert_level` (`alert_level`),
    KEY `idx_alert_type` (`alert_type`),
    KEY `idx_resolved` (`resolved`),
    KEY `idx_acknowledged` (`acknowledged`),
    KEY `idx_operator` (`operator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备告警记录表';

-- 插入测试告警数据
INSERT INTO `device_alerts`
(`alert_id`, `device_id`, `alert_type`, `alert_level`, `alert_message`, `alert_time`,
 `acknowledged`, `acknowledge_by`, `acknowledge_time`, `resolved`, `resolve_time`, `resolve_note`, `operator`)
VALUES
('ALT-20240615-0001', 'DEV-CNC-003', 'TEMP_HIGH',         'CRITICAL', '主轴温度达到65°C，超过阈值60°C', NOW() - INTERVAL 1 HOUR,  1, '管理员A', NOW() - INTERVAL 30 MINUTE, 0, NULL, NULL, '赵六'),
('ALT-20240615-0002', 'DEV-CNC-001', 'MAINT_DUE',         'WARNING',  '设备运行累计2000小时，建议更换切削液', NOW() - INTERVAL 6 HOUR,  1, '李四',    NOW() - INTERVAL 5 HOUR,  1, NOW() - INTERVAL 4 HOUR, '已完成切削液更换', '李四'),
('ALT-20240615-0003', 'DEV-PLC-001', 'COMM_ERROR',        'INFO',     'PLC心跳丢失1次后自动恢复',           NOW() - INTERVAL 30 MINUTE, 0, NULL,      NULL,                  1, NOW() - INTERVAL 29 MINUTE, '网络抖动自动恢复', '赵六'),
('ALT-20240615-0004', 'DEV-SEN-001', 'UNCALIBRATED',      'WARNING',  '传感器连续运行30天未校准',           NOW() - INTERVAL 10 MINUTE, 0, NULL,      NULL,                  0, NULL, NULL, '孙七'),
('ALT-20240615-0005', 'DEV-ROB-001', 'CURRENT_OVERLOAD',  'WARNING',  '焊接瞬间电流超过额定值115%',          NOW() - INTERVAL 2 MINUTE,  0, NULL,      NULL,                  0, NULL, NULL, '王五');

-- -----------------------------------------------------------
-- 4. 同步日志记录表 sync_log（系统自用，不在同步范围内）
-- -----------------------------------------------------------
DROP TABLE IF EXISTS `sync_log`;
CREATE TABLE `sync_log` (
    `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `session_id`    VARCHAR(64)     NOT NULL,
    `rule_id`       VARCHAR(64)     DEFAULT NULL,
    `direction`     VARCHAR(32)     NOT NULL,
    `operation`     VARCHAR(256)    NOT NULL,
    `status`        VARCHAR(16)     NOT NULL,
    `records_count` INT             DEFAULT 0,
    `error_msg`     TEXT,
    `executed_at`   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `idx_session` (`session_id`),
    KEY `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步执行日志表(黑名单)';

-- -----------------------------------------------------------
-- 完成信息
-- -----------------------------------------------------------
SELECT '初始化完成!' AS message;
SELECT COUNT(*) AS device_info_count FROM device_info;
SELECT COUNT(*) AS device_metrics_count FROM device_metrics;
SELECT COUNT(*) AS device_alerts_count FROM device_alerts;

SHOW TABLES;
