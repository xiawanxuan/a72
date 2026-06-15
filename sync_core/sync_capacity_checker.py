"""
同步前置时序数据容量校验子模块
功能：
1. 自动检测目标库存储分区上限（保留策略、配额）
2. 预估本次同步操作的数据写入量
3. 评估磁盘溢出风险，提前拦截高危同步
4. 复用原有元数据采集模块，不新增网络层
5. 生成容量校验报告，支持软阈值警告和硬阈值拦截

插入位置：SyncOrchestrator.run_sync() 中
  init_connections() → [★ capacity_checker.check()] → collect_all() → ...
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from .db_adapters.mysql_adapter import MySQLAdapter
from .db_adapters.influxdb_adapter import InfluxDBAdapter
from .config_manager import ConfigManager, SyncRule, SyncDirection
from .metadata_collector import MetadataCollector
from .sync_filter import SyncFilter
from .log_rollback import LogLevel, SyncPhase

logger = logging.getLogger(__name__)


class CapacityStatus(str, Enum):
    """容量校验状态"""
    SAFE = "safe"              # 安全，可执行
    WARNING = "warning"        # 接近阈值，仅警告
    BLOCKED = "blocked"        # 超过阈值，拦截
    SKIPPED = "skipped"        # 跳过校验
    ERROR = "error"            # 校验出错


@dataclass
class CapacityThresholds:
    """容量阈值配置"""
    soft_warning_pct: float = 70.0       # 软警告阈值(%)：使用率达到此值输出警告
    hard_block_pct: float = 90.0          # 硬拦截阈值(%)：超过则拒绝同步
    max_increment_gb: float = 5.0         # 单次同步最大增量(GB)
    min_free_gb: float = 1.0              # 最小保留空闲(GB)
    min_retention_days: int = 7           # 最小保留周期(天)

    @classmethod
    def from_args(cls, threshold_pct: Optional[float] = None) -> "CapacityThresholds":
        """从命令行参数构建阈值"""
        inst = cls()
        if threshold_pct is not None:
            inst.hard_block_pct = threshold_pct
            inst.soft_warning_pct = threshold_pct * 0.8
        return inst


@dataclass
class TableCapacityStats:
    """单表/单测量容量统计"""
    name: str
    source_type: str  # "mysql" | "influxdb"
    paired_name: str
    row_count: int = 0
    size_bytes: int = 0
    size_gb: float = 0.0
    estimated_increment_bytes: int = 0  # 本次同步预估增量
    estimated_increment_gb: float = 0.0
    sync_direction: Optional[SyncDirection] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "source_type": self.source_type,
            "paired_name": self.paired_name,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "size_gb": round(self.size_gb, 6),
            "estimated_increment_gb": round(self.estimated_increment_gb, 6),
            "sync_direction": self.sync_direction.value if self.sync_direction else None,
            "extra": self.extra
        }


@dataclass
class CapacityCheckResult:
    """容量校验完整结果"""
    status: CapacityStatus
    message: str
    disk_usage: Dict[str, Any] = field(default_factory=dict)
    bucket_retention: Dict[str, Any] = field(default_factory=dict)
    table_stats: List[TableCapacityStats] = field(default_factory=list)
    total_estimated_increment_gb: float = 0.0
    usage_percent_after_sync: float = 0.0
    warnings: List[str] = field(default_factory=list)
    blocked_reasons: List[str] = field(default_factory=dict)
    thresholds: CapacityThresholds = field(default_factory=CapacityThresholds)
    skipped: bool = False
    check_duration_ms: int = 0

    @property
    def can_proceed(self) -> bool:
        return self.status in (CapacityStatus.SAFE, CapacityStatus.WARNING, CapacityStatus.SKIPPED)

    def to_dict(self) -> Dict:
        return {
            "status": self.status.value,
            "can_proceed": self.can_proceed,
            "message": self.message,
            "disk_usage": self.disk_usage,
            "bucket_retention": self.bucket_retention,
            "table_stats": [t.to_dict() for t in self.table_stats],
            "total_estimated_increment_gb": round(self.total_estimated_increment_gb, 6),
            "usage_percent_after_sync": round(self.usage_percent_after_sync, 2),
            "warnings": self.warnings,
            "blocked_reasons": self.blocked_reasons,
            "thresholds": {
                "soft_warning_pct": self.thresholds.soft_warning_pct,
                "hard_block_pct": self.thresholds.hard_block_pct,
                "max_increment_gb": self.thresholds.max_increment_gb,
                "min_free_gb": self.thresholds.min_free_gb,
                "min_retention_days": self.thresholds.min_retention_days
            },
            "check_duration_ms": self.check_duration_ms
        }


class CapacityChecker:
    """
    时序数据容量校验器

    设计原则：
    - 复用原有元数据采集模块和数据库适配器
    - 仅新增容量校验逻辑，不改动原有网络通路和协议解析
    - 支持软警告（可继续）和硬拦截（不可继续）两级风险控制
    """

    def __init__(self,
                 mysql_adapter: MySQLAdapter,
                 influxdb_adapter: InfluxDBAdapter,
                 config_manager: ConfigManager,
                 metadata_collector: Optional[MetadataCollector] = None,
                 sync_filter: Optional[SyncFilter] = None):
        self.mysql = mysql_adapter
        self.influx = influxdb_adapter
        self.config = config_manager
        self.collector = metadata_collector
        self.sync_filter = sync_filter
        self.bucket = self.influx.config.get("bucket", "")
        self.org = self.influx.config.get("org", "")

    def check(self, thresholds: Optional[CapacityThresholds] = None,
              skip_check: bool = False,
              log_fn=None) -> CapacityCheckResult:
        """
        执行完整容量校验

        Args:
            thresholds: 阈值配置，None 则使用默认
            skip_check: 是否跳过校验（用于 --skip-capacity-check）
            log_fn: 日志回调函数 (phase, level, message)

        Returns:
            CapacityCheckResult: 校验结果
        """
        start_ts = datetime.now()
        thresholds = thresholds or CapacityThresholds()

        def _log(level: LogLevel, msg: str):
            if log_fn:
                log_fn(SyncPhase.CONNECT, level, msg)
            logger.log(level.value, msg)

        if skip_check:
            _log(LogLevel.WARNING, "容量校验已跳过 (--skip-capacity-check)")
            return CapacityCheckResult(
                status=CapacityStatus.SKIPPED,
                message="容量校验已跳过，不执行磁盘溢出检测",
                skipped=True,
                check_duration_ms=int((datetime.now() - start_ts).total_seconds() * 1000)
            )

        _log(LogLevel.INFO, "开始容量校验：检测目标库存储分区上限")

        result = CapacityCheckResult(thresholds=thresholds)

        try:
            result.bucket_retention = self._check_retention_policy(thresholds, result.warnings, result.blocked_reasons)
            result.disk_usage = self.influx.fetch_disk_usage(self.bucket)

            if result.disk_usage.get("quota_bytes"):
                quota_gb = result.disk_usage["quota_gb"]
                usage_gb = result.disk_usage["estimated_usage_gb"]
                usage_pct = result.disk_usage.get("usage_percent", 0)
                _log(LogLevel.INFO,
                     f"磁盘使用: {usage_gb:.4f} GB / {quota_gb:.4f} GB "
                     f"({usage_pct:.2f}%)")

                if usage_pct >= thresholds.hard_block_pct:
                    result.blocked_reasons.append(
                        f"当前磁盘使用率 {usage_pct:.2f}% 已超过硬阈值 {thresholds.hard_block_pct}%"
                    )
                elif usage_pct >= thresholds.soft_warning_pct:
                    result.warnings.append(
                        f"当前磁盘使用率 {usage_pct:.2f}% 已接近阈值 {thresholds.soft_warning_pct}%"
                    )
                free_gb = quota_gb - usage_gb
                if free_gb < thresholds.min_free_gb:
                    result.blocked_reasons.append(
                        f"剩余可用空间 {free_gb:.4f} GB 小于最小保留 {thresholds.min_free_gb} GB"
                    )
            else:
                result.warnings.append(
                    "未检测到存储配额限制，基于保留策略预估容量"
                )
                usage_gb = result.disk_usage.get("estimated_usage_gb", 0)
                usage_pct = 0

            enabled_rules = self._get_filtered_rules()
            _log(LogLevel.INFO, f"检测 {len(enabled_rules)} 个同步对的数据规模")

            for rule in enabled_rules:
                try:
                    stats = self._collect_table_capacity(rule)
                    if stats:
                        result.table_stats.append(stats)
                        result.total_estimated_increment_gb += stats.estimated_increment_gb
                except Exception as e:
                    logger.warning(f"采集 [{rule.rule_id}] 容量信息失败: {e}")
                    result.warnings.append(f"采集 [{rule.rule_id}] 容量信息失败: {e}")

            _log(LogLevel.INFO,
                 f"本次同步预估增量: {result.total_estimated_increment_gb:.6f} GB")

            if result.total_estimated_increment_gb > thresholds.max_increment_gb:
                result.blocked_reasons.append(
                    f"单次同步增量 {result.total_estimated_increment_gb:.4f} GB "
                    f"超过最大允许 {thresholds.max_increment_gb} GB"
                )

            if result.disk_usage.get("quota_bytes"):
                quota_gb = result.disk_usage["quota_gb"]
                usage_gb = result.disk_usage["estimated_usage_gb"]
                projected_usage_gb = usage_gb + result.total_estimated_increment_gb
                result.usage_percent_after_sync = (
                    projected_usage_gb / quota_gb * 100
                    if quota_gb > 0 else 0
                )
                _log(LogLevel.INFO,
                     f"同步后预估使用率: {result.usage_percent_after_sync:.2f}%")

                if result.usage_percent_after_sync >= thresholds.hard_block_pct:
                    result.blocked_reasons.append(
                        f"同步后预估使用率 {result.usage_percent_after_sync:.2f}% "
                        f"超过硬阈值 {thresholds.hard_block_pct}%"
                    )
                elif result.usage_percent_after_sync >= thresholds.soft_warning_pct:
                    result.warnings.append(
                        f"同步后预估使用率 {result.usage_percent_after_sync:.2f}% "
                        f"接近软阈值 {thresholds.soft_warning_pct}%"
                    )

            if result.blocked_reasons:
                result.status = CapacityStatus.BLOCKED
                result.message = "容量校验未通过，同步被拦截"
                _log(LogLevel.ERROR,
                     f"容量校验拦截: {len(result.blocked_reasons)} 个原因")
                for reason in result.blocked_reasons:
                    _log(LogLevel.ERROR, f"  · {reason}")
            elif result.warnings:
                result.status = CapacityStatus.WARNING
                result.message = "容量校验通过，但存在风险警告"
                _log(LogLevel.WARNING,
                     f"容量校验通过，{len(result.warnings)} 个警告")
            else:
                result.status = CapacityStatus.SAFE
                result.message = "容量校验通过，磁盘空间充足"
                _log(LogLevel.INFO, "容量校验通过，磁盘空间充足")

        except Exception as e:
            logger.error(f"容量校验异常: {e}", exc_info=True)
            result.status = CapacityStatus.ERROR
            result.message = f"容量校验异常: {e}"
            result.warnings.append(f"校验异常: {e}")
            _log(LogLevel.ERROR, f"容量校验异常: {e}")

        result.check_duration_ms = int(
            (datetime.now() - start_ts).total_seconds() * 1000
        )
        return result

    def _get_filtered_rules(self) -> List[SyncRule]:
        """应用 SyncFilter 过滤后的规则列表"""
        rules = self.config.get_enabled_rules()

        if not self.sync_filter or self.sync_filter.is_empty:
            return rules

        filtered = []
        for rule in rules:
            if self.sync_filter.rule_ids and rule.rule_id not in self.sync_filter.rule_ids:
                continue
            if self.sync_filter.tables and rule.mysql_table not in self.sync_filter.tables:
                continue
            if self.sync_filter.measurements and rule.influxdb_measurement not in self.sync_filter.measurements:
                continue
            if self.sync_filter.direction and rule.sync_direction != self.sync_filter.direction:
                continue
            filtered.append(rule)
        return filtered

    def _check_retention_policy(self, thresholds: CapacityThresholds,
                                 warnings: List[str], blocked: List[str]) -> Dict[str, Any]:
        """检查保留策略是否合理"""
        retention = self.influx.fetch_bucket_retention(self.bucket)

        if not retention.get("exists"):
            warnings.append(f"Bucket [{self.bucket}] 不存在或无权限访问")
            return retention

        if retention.get("retention_days") is not None:
            days = retention["retention_days"]
            if days < thresholds.min_retention_days:
                blocked.append(
                    f"数据保留期 {days:.1f} 天小于最小要求 {thresholds.min_retention_days} 天"
                )
            elif days < thresholds.min_retention_days * 2:
                warnings.append(
                    f"数据保留期 {days:.1f} 天较短，建议扩大至 {thresholds.min_retention_days * 2} 天以上"
                )
        else:
            warnings.append(
                "Bucket 保留策略为永久(forever)，可能导致磁盘无限增长"
            )

        return retention

    def _collect_table_capacity(self, rule: SyncRule) -> Optional[TableCapacityStats]:
        """采集单条同步规则对应的双边容量数据"""
        mysql_stats = self.influx.fetch_mysql_row_count(
            rule.mysql_table, self.mysql
        )
        influx_stats = self.influx.fetch_measurement_series_count(
            rule.influxdb_measurement, self.bucket, time_range_hours=24
        )

        mysql_size = mysql_stats.get("size_mb", 0) * 1024 * 1024
        influx_size = influx_stats.get("estimated_size_bytes", 0)

        mysql_rows = mysql_stats.get("row_count", 0)
        influx_rows = influx_stats.get("point_count", 0)

        increment_bytes = self._estimate_increment(
            rule, mysql_rows, influx_rows, mysql_size, influx_size
        )
        increment_gb = increment_bytes / (1024 * 1024 * 1024)

        stats = TableCapacityStats(
            name=rule.mysql_table,
            source_type="mysql",
            paired_name=rule.influxdb_measurement,
            row_count=mysql_rows,
            size_bytes=int(mysql_size),
            size_gb=mysql_size / (1024 * 1024 * 1024),
            estimated_increment_bytes=int(increment_bytes),
            estimated_increment_gb=increment_gb,
            sync_direction=rule.sync_direction,
            extra={
                "rule_id": rule.rule_id,
                "influx_point_count": influx_rows,
                "influx_size_gb": influx_size / (1024 * 1024 * 1024),
                "mysql_rows": mysql_rows,
                "mysql_size_gb": mysql_size / (1024 * 1024 * 1024)
            }
        )
        return stats

    def _estimate_increment(self, rule: SyncRule,
                             mysql_rows: int, influx_rows: int,
                             mysql_size: int, influx_size: int) -> int:
        """
        预估本次同步的数据增量（字节）

        估算模型：
        - 双向同步：取双边较小值的 0.5%（假设差异率）
        - MySQL→InfluxDB：MySQL 行数 × 平均行大小 × 同步比例
        - InfluxDB→MySQL：InfluxDB 点数 × 平均点大小 × 同步比例
        """
        if mysql_rows == 0 and influx_rows == 0:
            return 0

        mysql_avg_row = mysql_size / mysql_rows if mysql_rows > 0 else 256
        influx_avg_point = influx_size / influx_rows if influx_rows > 0 else 256

        sync_ratio = 0.02  # 默认假设 2% 的数据有差异需要同步

        if rule.sync_direction == SyncDirection.MYSQL_TO_INFLUX:
            if mysql_rows > influx_rows:
                new_rows = mysql_rows - influx_rows
                return int(new_rows * max(mysql_avg_row, influx_avg_point) * 1.1)
            return int(influx_rows * sync_ratio * influx_avg_point)

        elif rule.sync_direction == SyncDirection.INFLUX_TO_MYSQL:
            if influx_rows > mysql_rows:
                new_rows = influx_rows - mysql_rows
                return int(new_rows * max(mysql_avg_row, influx_avg_point) * 1.1)
            return int(mysql_rows * sync_ratio * mysql_avg_row)

        else:
            if mysql_rows > influx_rows:
                diff_mysql = (mysql_rows - influx_rows) * max(mysql_avg_row, influx_avg_point)
            else:
                diff_mysql = 0
            if influx_rows > mysql_rows:
                diff_influx = (influx_rows - mysql_rows) * max(mysql_avg_row, influx_avg_point)
            else:
                diff_influx = 0
            sync_size = min(mysql_rows, influx_rows) * sync_ratio * max(mysql_avg_row, influx_avg_point)
            return int(diff_mysql + diff_influx + sync_size)

    def check_single(self, rule: SyncRule) -> Optional[TableCapacityStats]:
        """单个规则的容量快速检测（供外部调用）"""
        try:
            return self._collect_table_capacity(rule)
        except Exception as e:
            logger.error(f"单个规则容量检测失败 [{rule.rule_id}]: {e}")
            return None
