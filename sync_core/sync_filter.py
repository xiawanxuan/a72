"""
同步过滤参数优先级解析模块
负责：
1. 从 CLI args 解析过滤参数，构建 SyncFilter 数据对象
2. 按优先级解析: rule > table/measurement > direction > target > diff_type
3. 对 DiffAnalysisResult / GenerateResult 施加多条件 AND 过滤
4. 空结果分支输出诊断信息（而非静默退出）

此模块仅处理参数解析与结果过滤，不触碰核心协议解析引擎
（diff_engine / sync_executor / metadata_collector）
"""

import logging
from typing import Dict, List, Optional, Any, Set, FrozenSet
from dataclasses import dataclass, field
from enum import Enum

from .diff_engine import (
    PairDiffResult, DiffAnalysisResult, FieldDiff, IndexDiff,
    DiffType, DiffDirection
)
from .config_manager import SyncDirection

logger = logging.getLogger(__name__)


class FilterTarget(str, Enum):
    """过滤目标数据库"""
    MYSQL = "mysql"
    INFLUXDB = "influxdb"
    BOTH = "both"


@dataclass
class SyncFilter:
    """
    同步过滤条件容器

    优先级（从高到低）:
      1. rule_ids      - 精确匹配规则 ID，最高优先级
      2. tables         - 匹配 MySQL 表名
      3. measurements   - 匹配 InfluxDB measurement 名
      4. direction      - 匹配同步方向
      5. target         - 匹配目标数据库（仅生成 mysql/influxdb 侧脚本）
      6. diff_types     - 匹配差异类型

    多个条件之间为 AND 关系；同一条件内多个值为 OR 关系
    例如: --rule R1 --direction bidirectional --target mysql
    意味着: (规则=R1) AND (方向=bidirectional) AND (目标=mysql)
    """
    rule_ids: FrozenSet[str] = field(default_factory=frozenset)
    tables: FrozenSet[str] = field(default_factory=frozenset)
    measurements: FrozenSet[str] = field(default_factory=frozenset)
    direction: Optional[SyncDirection] = None
    target: FilterTarget = FilterTarget.BOTH
    diff_types: FrozenSet[DiffType] = field(default_factory=frozenset)

    @property
    def is_empty(self) -> bool:
        return (not self.rule_ids and not self.tables and not self.measurements
                and self.direction is None and self.target == FilterTarget.BOTH
                and not self.diff_types)

    def describe(self) -> str:
        parts = []
        if self.rule_ids:
            parts.append(f"规则={','.join(self.rule_ids)}")
        if self.tables:
            parts.append(f"表={','.join(self.tables)}")
        if self.measurements:
            parts.append(f"测量={','.join(self.measurements)}")
        if self.direction:
            parts.append(f"方向={self.direction.value}")
        if self.target != FilterTarget.BOTH:
            parts.append(f"目标={self.target.value}")
        if self.diff_types:
            parts.append(f"差异类型={','.join(d.value for d in self.diff_types)}")
        return " AND ".join(parts) if parts else "无过滤(全量)"


def resolve_filter(args) -> SyncFilter:
    """
    从 argparse Namespace 解析过滤参数，按优先级构建 SyncFilter

    解析优先级:
      1. --rule 优先级最高，若指定则仅同步这些规则对应的表/测量对
      2. --table / --measurement 进一步缩小范围
      3. --direction 过滤同步方向
      4. --target 过滤脚本目标库
      5. --diff-type 过滤差异类型

    参数来源: args.rule, args.table, args.measurement,
              args.direction, args.target, args.diff_type
    """
    rule_ids: Set[str] = set()
    tables: Set[str] = set()
    measurements: Set[str] = set()
    direction: Optional[SyncDirection] = None
    target = FilterTarget.BOTH
    diff_types: Set[DiffType] = set()

    if hasattr(args, "rule") and args.rule:
        for r in args.rule:
            for part in r.split(","):
                part = part.strip()
                if part:
                    rule_ids.add(part)

    if hasattr(args, "table") and args.table:
        for t in args.table:
            for part in t.split(","):
                part = part.strip()
                if part:
                    tables.add(part)

    if hasattr(args, "measurement") and args.measurement:
        for m in args.measurement:
            for part in m.split(","):
                part = part.strip()
                if part:
                    measurements.add(part)

    if hasattr(args, "direction") and args.direction:
        try:
            direction = SyncDirection(args.direction)
        except ValueError:
            valid = [d.value for d in SyncDirection]
            logger.warning(f"无效同步方向: {args.direction}, 有效值: {valid}, 忽略此过滤条件")

    if hasattr(args, "target") and args.target:
        try:
            target = FilterTarget(args.target)
        except ValueError:
            valid = [t.value for t in FilterTarget]
            logger.warning(f"无效目标库: {args.target}, 有效值: {valid}, 忽略此过滤条件")

    if hasattr(args, "diff_type") and args.diff_type:
        for dt in args.diff_type:
            for part in dt.split(","):
                part = part.strip()
                if part:
                    try:
                        diff_types.add(DiffType(part))
                    except ValueError:
                        valid = [d.value for d in DiffType]
                        logger.warning(f"无效差异类型: {part}, 有效值: {valid}, 忽略")

    sf = SyncFilter(
        rule_ids=frozenset(rule_ids),
        tables=frozenset(tables),
        measurements=frozenset(measurements),
        direction=direction,
        target=target,
        diff_types=frozenset(diff_types)
    )

    logger.info(f"过滤条件解析完成: {sf.describe()}")
    return sf


def apply_filter_to_diff(diff_result: DiffAnalysisResult,
                         sync_filter: SyncFilter) -> DiffAnalysisResult:
    """
    对差异分析结果施加过滤条件

    过滤逻辑（多条件 AND）:
      1. rule_ids: pair.rule.rule_id 在集合中
      2. tables:   pair.mysql_schema.source_name 在集合中
      3. measurements: pair.influx_schema.source_name 在集合中
      4. direction: pair.rule.sync_direction == direction（无规则的双向配对默认通过）
      5. diff_types: pair 中存在指定类型的差异
      6. target: 在后续脚本生成阶段过滤，此阶段仅标记

    返回新的 DiffAnalysisResult，保留未过滤的 pair
    """
    if sync_filter.is_empty:
        logger.info("无过滤条件，保留全量差异结果")
        return diff_result

    filtered = DiffAnalysisResult()
    filtered.errors = list(diff_result.errors)

    for pair in diff_result.pair_results:
        if not _match_pair(pair, sync_filter):
            logger.debug(f"过滤排除: [MySQL]{pair.mysql_schema.source_name} "
                         f"<-> [InfluxDB]{pair.influx_schema.source_name}")
            continue

        if sync_filter.diff_types:
            pair = _filter_pair_diffs(pair, sync_filter.diff_types)

        if not pair.has_diff and sync_filter.diff_types:
            logger.debug(f"差异类型过滤后无匹配差异: "
                         f"[MySQL]{pair.mysql_schema.source_name}")
            continue

        filtered.pair_results.append(pair)

    for src_type, schema, direction in diff_result.unpaired_create_targets:
        if not _match_unpaired(src_type, schema, sync_filter):
            continue
        filtered.unpaired_create_targets.append((src_type, schema, direction))

    kept = len(filtered.pair_results)
    total = len(diff_result.pair_results)
    logger.info(f"差异过滤: {total} -> {kept} 对 (条件: {sync_filter.describe()})")

    return filtered


def apply_filter_to_generate(gen_result, sync_filter: SyncFilter):
    """
    对脚本生成结果施加 target 过滤

    仅当 sync_filter.target != BOTH 时生效:
      - MYSQL: 仅保留 mysql 侧脚本
      - INFLUXDB: 仅保留 influxdb 侧脚本
    """
    from .sync_executor import GenerateResult

    if sync_filter.target == FilterTarget.BOTH:
        return gen_result

    filtered = GenerateResult()

    for s in gen_result.create_table_scripts:
        if _match_target(s.target_db, sync_filter.target):
            filtered.create_table_scripts.append(s)

    for s in gen_result.mysql_scripts:
        if _match_target(s.target_db, sync_filter.target):
            filtered.mysql_scripts.append(s)

    for s in gen_result.influxdb_scripts:
        if _match_target(s.target_db, sync_filter.target):
            filtered.influxdb_scripts.append(s)

    kept = len(filtered.all_scripts)
    total = len(gen_result.all_scripts)
    logger.info(f"脚本目标过滤: {total} -> {kept} (target={sync_filter.target.value})")

    return filtered


def build_filter_diagnostic(diff_result: DiffAnalysisResult,
                            sync_filter: SyncFilter) -> Dict[str, Any]:
    """
    当过滤后结果为空时，构建诊断信息
    帮助用户理解为何无输出，而非静默退出
    """
    diag: Dict[str, Any] = {
        "filter": sync_filter.describe(),
        "total_pairs_before_filter": len(diff_result.pair_results),
        "total_unpaired_before_filter": len(diff_result.unpaired_create_targets),
        "filter_match_details": []
    }

    for pair in diff_result.pair_results:
        reason = _explain_mismatch(pair, sync_filter)
        if reason:
            diag["filter_match_details"].append({
                "mysql_table": pair.mysql_schema.source_name,
                "influx_measurement": pair.influx_schema.source_name,
                "rule_id": pair.rule.rule_id if pair.rule else None,
                "mismatch_reasons": reason
            })

    return diag


def _match_pair(pair: PairDiffResult, sf: SyncFilter) -> bool:
    """判断单个配对是否匹配所有过滤条件（AND 逻辑）"""
    if sf.rule_ids:
        rule_id = pair.rule.rule_id if pair.rule else None
        if rule_id not in sf.rule_ids:
            return False

    if sf.tables:
        if pair.mysql_schema.source_name not in sf.tables:
            return False

    if sf.measurements:
        if pair.influx_schema.source_name not in sf.measurements:
            return False

    if sf.direction:
        pair_dir = pair.rule.sync_direction if pair.rule else SyncDirection.BIDIRECTIONAL
        if pair_dir != sf.direction:
            return False

    if sf.diff_types:
        pair_types = {d.diff_type for d in pair.field_diffs} | {d.diff_type for d in pair.index_diffs}
        if not pair_types.intersection(sf.diff_types):
            return False

    return True


def _match_unpaired(src_type: str, schema, sf: SyncFilter) -> bool:
    """判断未配对目标是否匹配过滤条件"""
    if sf.rule_ids:
        rule_id = schema.rule.rule_id if schema.rule else None
        if rule_id not in sf.rule_ids:
            return False

    if src_type == "mysql" and sf.tables:
        if schema.source_name not in sf.tables:
            return False

    if src_type == "influxdb" and sf.measurements:
        if schema.source_name not in sf.measurements:
            return False

    if sf.direction and schema.rule:
        if schema.rule.sync_direction != sf.direction:
            return False

    return True


def _filter_pair_diffs(pair: PairDiffResult,
                       diff_types: FrozenSet[DiffType]) -> PairDiffResult:
    """过滤 pair 中的差异项，仅保留匹配 diff_types 的项"""
    filtered_fields = [d for d in pair.field_diffs if d.diff_type in diff_types]
    filtered_indexes = [d for d in pair.index_diffs if d.diff_type in diff_types]

    return PairDiffResult(
        rule=pair.rule,
        mysql_schema=pair.mysql_schema,
        influx_schema=pair.influx_schema,
        field_diffs=filtered_fields,
        index_diffs=filtered_indexes
    )


def _match_target(script_target: str, filter_target: FilterTarget) -> bool:
    """判断脚本目标库是否匹配过滤目标"""
    if filter_target == FilterTarget.BOTH:
        return True
    if filter_target == FilterTarget.MYSQL and script_target == "mysql":
        return True
    if filter_target == FilterTarget.INFLUXDB and script_target == "influxdb":
        return True
    return False


def _explain_mismatch(pair: PairDiffResult, sf: SyncFilter) -> List[str]:
    """解释为何某个 pair 不匹配过滤条件"""
    reasons = []

    if sf.rule_ids:
        rule_id = pair.rule.rule_id if pair.rule else None
        if rule_id not in sf.rule_ids:
            reasons.append(f"规则ID [{rule_id}] 不在指定集合 {set(sf.rule_ids)} 中")

    if sf.tables:
        if pair.mysql_schema.source_name not in sf.tables:
            reasons.append(f"表名 [{pair.mysql_schema.source_name}] 不在指定集合 {set(sf.tables)} 中")

    if sf.measurements:
        if pair.influx_schema.source_name not in sf.measurements:
            reasons.append(f"测量名 [{pair.influx_schema.source_name}] 不在指定集合 {set(sf.measurements)} 中")

    if sf.direction:
        pair_dir = pair.rule.sync_direction if pair.rule else SyncDirection.BIDIRECTIONAL
        if pair_dir != sf.direction:
            reasons.append(f"同步方向 [{pair_dir.value}] != 过滤方向 [{sf.direction.value}]")

    if sf.diff_types:
        pair_types = {d.diff_type for d in pair.field_diffs} | {d.diff_type for d in pair.index_diffs}
        if not pair_types.intersection(sf.diff_types):
            reasons.append(f"差异类型 [{pair_types}] 与过滤类型 {set(sf.diff_types)} 无交集")

    return reasons
