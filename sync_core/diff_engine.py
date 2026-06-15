"""
标签字段差异对比引擎
识别 MySQL 表与 InfluxDB measurement 之间的双向差异：
- 字段新增、删除、类型变更
- 标签变更（新增/删除标签）
- 索引差异（新增/删除索引）
"""

import logging
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum

from .metadata_collector import UnifiedSchema, FieldMetadata, IndexMetadata, CollectResult
from .config_manager import SyncRule, SyncDirection

logger = logging.getLogger(__name__)


class DiffType(str, Enum):
    """差异类型枚举"""
    FIELD_ADD = "field_add"
    FIELD_DROP = "field_drop"
    FIELD_TYPE_CHANGE = "field_type_change"
    FIELD_NULLABLE_CHANGE = "field_nullable_change"
    TAG_ADD = "tag_add"
    TAG_DROP = "tag_drop"
    INDEX_ADD = "index_add"
    INDEX_DROP = "index_drop"
    INDEX_COLUMN_CHANGE = "index_column_change"
    PRIMARY_KEY_CHANGE = "primary_key_change"


class DiffDirection(str, Enum):
    """差异作用方向"""
    MYSQL_TO_INFLUX = "mysql_to_influx"  # 需要在 InfluxDB 侧执行变更
    INFLUX_TO_MYSQL = "influx_to_mysql"  # 需要在 MySQL 侧执行变更


@dataclass
class FieldDiff:
    """字段级差异"""
    diff_type: DiffType
    field_name: str
    direction: DiffDirection
    mysql_field: Optional[FieldMetadata] = None
    influx_field: Optional[FieldMetadata] = None
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    description: str = ""

    def to_dict(self) -> Dict:
        return {
            "diff_type": self.diff_type.value,
            "field_name": self.field_name,
            "direction": self.direction.value,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "description": self.description
        }


@dataclass
class IndexDiff:
    """索引级差异"""
    diff_type: DiffType
    index_name: str
    direction: DiffDirection
    mysql_index: Optional[IndexMetadata] = None
    influx_index: Optional[IndexMetadata] = None
    old_columns: List[str] = field(default_factory=list)
    new_columns: List[str] = field(default_factory=list)
    description: str = ""

    def to_dict(self) -> Dict:
        return {
            "diff_type": self.diff_type.value,
            "index_name": self.index_name,
            "direction": self.direction.value,
            "old_columns": self.old_columns,
            "new_columns": self.new_columns,
            "description": self.description
        }


@dataclass
class PairDiffResult:
    """单对（表-measurement）差异分析结果"""
    rule: Optional[SyncRule]
    mysql_schema: UnifiedSchema
    influx_schema: UnifiedSchema
    field_diffs: List[FieldDiff] = field(default_factory=list)
    index_diffs: List[IndexDiff] = field(default_factory=list)

    @property
    def has_diff(self) -> bool:
        return len(self.field_diffs) > 0 or len(self.index_diffs) > 0

    @property
    def mysql_diffs(self) -> int:
        return len([d for d in self.field_diffs if d.direction == DiffDirection.INFLUX_TO_MYSQL]) + \
               len([d for d in self.index_diffs if d.direction == DiffDirection.INFLUX_TO_MYSQL])

    @property
    def influx_diffs(self) -> int:
        return len([d for d in self.field_diffs if d.direction == DiffDirection.MYSQL_TO_INFLUX]) + \
               len([d for d in self.index_diffs if d.direction == DiffDirection.MYSQL_TO_INFLUX])

    def summary(self) -> Dict[str, Any]:
        type_count: Dict[str, int] = {}
        for d in self.field_diffs:
            type_count[d.diff_type.value] = type_count.get(d.diff_type.value, 0) + 1
        for d in self.index_diffs:
            type_count[d.diff_type.value] = type_count.get(d.diff_type.value, 0) + 1
        return {
            "rule_id": self.rule.rule_id if self.rule else None,
            "mysql_table": self.mysql_schema.source_name,
            "influx_measurement": self.influx_schema.source_name,
            "has_diff": self.has_diff,
            "total_diffs": len(self.field_diffs) + len(self.index_diffs),
            "field_diffs": len(self.field_diffs),
            "index_diffs": len(self.index_diffs),
            "mysql_side_changes": self.mysql_diffs,
            "influx_side_changes": self.influx_diffs,
            "diff_type_breakdown": type_count
        }

    def to_dict(self) -> Dict:
        return {
            **self.summary(),
            "field_diffs": [d.to_dict() for d in self.field_diffs],
            "index_diffs": [d.to_dict() for d in self.index_diffs]
        }


@dataclass
class DiffAnalysisResult:
    """全量差异分析结果"""
    pair_results: List[PairDiffResult] = field(default_factory=list)
    unpaired_create_targets: List[tuple] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def pairs_with_diff(self) -> List[PairDiffResult]:
        return [p for p in self.pair_results if p.has_diff]

    @property
    def total_diffs(self) -> int:
        return sum(len(p.field_diffs) + len(p.index_diffs) for p in self.pair_results)

    def summary(self) -> Dict[str, Any]:
        return {
            "total_pairs": len(self.pair_results),
            "pairs_with_diff": len(self.pairs_with_diff),
            "total_diffs": self.total_diffs,
            "unpaired_targets": len(self.unpaired_create_targets),
            "errors": len(self.errors),
            "pair_summaries": [p.summary() for p in self.pair_results]
        }

    def to_dict(self) -> Dict:
        return {
            **self.summary(),
            "pair_results": [p.to_dict() for p in self.pair_results],
            "unpaired_create_targets": [
                {"source_type": u[0], "source_name": u[1].source_name,
                 "target_name": u[1].paired_name}
                for u in self.unpaired_create_targets
            ],
            "errors": self.errors
        }


class DiffEngine:
    """差异对比引擎"""

    def __init__(self, config_manager=None):
        self.config = config_manager

    def analyze(self, collect_result: CollectResult) -> DiffAnalysisResult:
        """全量差异分析"""
        result = DiffAnalysisResult()
        logger.info("========== 开始差异分析 ==========")

        for mysql_schema, influx_schema, rule in collect_result.paired_schemas:
            try:
                pair_result = self._analyze_pair(mysql_schema, influx_schema, rule)
                result.pair_results.append(pair_result)
                if pair_result.has_diff:
                    logger.info(f"发现差异: [MySQL]{mysql_schema.source_name} <-> "
                                f"[InfluxDB]{influx_schema.source_name}: "
                                f"{len(pair_result.field_diffs)} 字段差异, "
                                f"{len(pair_result.index_diffs)} 索引差异")
            except Exception as e:
                msg = (f"差异分析失败 [MySQL]{mysql_schema.source_name} <-> "
                       f"[InfluxDB]{influx_schema.source_name}: {e}")
                logger.error(msg, exc_info=True)
                result.errors.append(msg)

        for schema in collect_result.unpaired_mysql:
            try:
                direction = SyncDirection.MYSQL_TO_INFLUX
                if schema.rule:
                    direction = schema.rule.sync_direction
                if direction in (SyncDirection.MYSQL_TO_INFLUX, SyncDirection.BIDIRECTIONAL):
                    result.unpaired_create_targets.append(("mysql", schema, direction))
                    logger.info(f"MySQL 表需要在 InfluxDB 创建: {schema.source_name} -> {schema.paired_name}")
            except Exception as e:
                msg = f"处理未配对 MySQL 表失败 [{schema.source_name}]: {e}"
                logger.warning(msg)
                result.errors.append(msg)

        for schema in collect_result.unpaired_influxdb:
            try:
                direction = SyncDirection.INFLUX_TO_MYSQL
                if schema.rule:
                    direction = schema.rule.sync_direction
                if direction in (SyncDirection.INFLUX_TO_MYSQL, SyncDirection.BIDIRECTIONAL):
                    result.unpaired_create_targets.append(("influxdb", schema, direction))
                    logger.info(f"InfluxDB measurement 需要在 MySQL 创建: {schema.source_name} -> {schema.paired_name}")
            except Exception as e:
                msg = f"处理未配对 InfluxDB measurement 失败 [{schema.source_name}]: {e}"
                logger.warning(msg)
                result.errors.append(msg)

        logger.info(f"========== 差异分析完成: {result.summary()} ==========")
        return result

    def _analyze_pair(self, mysql_schema: UnifiedSchema,
                      influx_schema: UnifiedSchema,
                      rule: Optional[SyncRule]) -> PairDiffResult:
        """分析单对表-measurement 的差异"""
        pair_result = PairDiffResult(
            rule=rule,
            mysql_schema=mysql_schema,
            influx_schema=influx_schema
        )

        direction = rule.sync_direction if rule else SyncDirection.BIDIRECTIONAL

        self._analyze_field_diffs(pair_result, mysql_schema, influx_schema, direction)
        self._analyze_index_diffs(pair_result, mysql_schema, influx_schema, direction)

        return pair_result

    def _analyze_field_diffs(self, result: PairDiffResult,
                             mysql: UnifiedSchema, influx: UnifiedSchema,
                             direction: SyncDirection):
        """分析字段级差异"""
        mysql_fields: Dict[str, FieldMetadata] = mysql.fields
        influx_fields: Dict[str, FieldMetadata] = influx.fields

        all_field_names = set(mysql_fields.keys()) | set(influx_fields.keys())

        for fname in sorted(all_field_names):
            m_field = mysql_fields.get(fname)
            i_field = influx_fields.get(fname)

            if m_field and not i_field:
                if direction in (SyncDirection.MYSQL_TO_INFLUX, SyncDirection.BIDIRECTIONAL):
                    desc = (f"MySQL 字段 [{fname}]({m_field.type}) 在 InfluxDB 不存在, "
                            f"需要新增为{'标签' if m_field.is_tag else '普通字段'}")
                    result.field_diffs.append(FieldDiff(
                        diff_type=DiffType.TAG_ADD if m_field.is_tag else DiffType.FIELD_ADD,
                        field_name=fname,
                        direction=DiffDirection.MYSQL_TO_INFLUX,
                        mysql_field=m_field,
                        new_value=m_field.to_dict(),
                        description=desc
                    ))

            elif i_field and not m_field:
                if direction in (SyncDirection.INFLUX_TO_MYSQL, SyncDirection.BIDIRECTIONAL):
                    desc = (f"InfluxDB 字段 [{fname}]({i_field.type}, {'tag' if i_field.is_tag else 'field'}) "
                            f"在 MySQL 不存在, 需要新增")
                    diff_type = DiffType.TAG_ADD if i_field.is_tag else DiffType.FIELD_ADD
                    result.field_diffs.append(FieldDiff(
                        diff_type=diff_type,
                        field_name=fname,
                        direction=DiffDirection.INFLUX_TO_MYSQL,
                        influx_field=i_field,
                        new_value=i_field.to_dict(),
                        description=desc
                    ))

            elif m_field and i_field:
                if m_field.type != i_field.type:
                    if direction in (SyncDirection.MYSQL_TO_INFLUX, SyncDirection.BIDIRECTIONAL):
                        if not i_field.cardinality:
                            desc = (f"字段 [{fname}] 类型不一致: MySQL={m_field.type}, "
                                    f"InfluxDB={i_field.type}, 以 MySQL 为准同步")
                            result.field_diffs.append(FieldDiff(
                                diff_type=DiffType.FIELD_TYPE_CHANGE,
                                field_name=fname,
                                direction=DiffDirection.MYSQL_TO_INFLUX,
                                mysql_field=m_field,
                                influx_field=i_field,
                                old_value=i_field.type,
                                new_value=m_field.type,
                                description=desc
                            ))

                if m_field.is_tag != i_field.is_tag:
                    if m_field.is_tag and not i_field.is_tag:
                        if direction in (SyncDirection.MYSQL_TO_INFLUX, SyncDirection.BIDIRECTIONAL):
                            desc = (f"字段 [{fname}] 在 MySQL 配置为标签, "
                                    f"但在 InfluxDB 为普通字段, 需转换为标签")
                            result.field_diffs.append(FieldDiff(
                                diff_type=DiffType.TAG_ADD,
                                field_name=fname,
                                direction=DiffDirection.MYSQL_TO_INFLUX,
                                mysql_field=m_field,
                                influx_field=i_field,
                                old_value="field",
                                new_value="tag",
                                description=desc
                            ))
                    elif not m_field.is_tag and i_field.is_tag:
                        if direction in (SyncDirection.INFLUX_TO_MYSQL, SyncDirection.BIDIRECTIONAL):
                            desc = (f"字段 [{fname}] 在 InfluxDB 为标签, "
                                    f"但在 MySQL 未配置为标签字段")
                            result.field_diffs.append(FieldDiff(
                                diff_type=DiffType.TAG_DROP,
                                field_name=fname,
                                direction=DiffDirection.MYSQL_TO_INFLUX,
                                mysql_field=m_field,
                                influx_field=i_field,
                                old_value="tag",
                                new_value="field",
                                description=desc
                            ))

                if m_field.nullable != i_field.nullable:
                    if direction in (SyncDirection.INFLUX_TO_MYSQL, SyncDirection.BIDIRECTIONAL):
                        desc = (f"字段 [{fname}] nullable 属性不一致: "
                                f"MySQL={m_field.nullable}, InfluxDB={i_field.nullable}")
                        result.field_diffs.append(FieldDiff(
                            diff_type=DiffType.FIELD_NULLABLE_CHANGE,
                            field_name=fname,
                            direction=DiffDirection.INFLUX_TO_MYSQL,
                            mysql_field=m_field,
                            influx_field=i_field,
                            old_value=m_field.nullable,
                            new_value=i_field.nullable,
                            description=desc
                        ))

    def _analyze_index_diffs(self, result: PairDiffResult,
                             mysql: UnifiedSchema, influx: UnifiedSchema,
                             direction: SyncDirection):
        """分析索引级差异"""
        mysql_indexes: Dict[str, IndexMetadata] = mysql.indexes
        influx_indexes: Dict[str, IndexMetadata] = influx.indexes

        mysql_col_to_idx: Dict[Tuple, str] = {}
        for iname, idx in mysql_indexes.items():
            key = tuple(sorted(idx.columns))
            mysql_col_to_idx[key] = iname

        influx_col_to_idx: Dict[Tuple, str] = {}
        for iname, idx in influx_indexes.items():
            key = tuple(sorted(idx.columns))
            influx_col_to_idx[key] = iname

        all_col_sets = set(mysql_col_to_idx.keys()) | set(influx_col_to_idx.keys())

        for cols in sorted(all_col_sets, key=lambda x: (len(x), x)):
            m_idx_name = mysql_col_to_idx.get(cols)
            i_idx_name = influx_col_to_idx.get(cols)

            m_idx = mysql_indexes[m_idx_name] if m_idx_name else None
            i_idx = influx_indexes[i_idx_name] if i_idx_name else None

            if m_idx and not i_idx:
                if direction in (SyncDirection.MYSQL_TO_INFLUX, SyncDirection.BIDIRECTIONAL):
                    tag_cols = [c for c in cols if c in mysql.tag_names]
                    if tag_cols or m_idx.unique:
                        idx_name = f"idx_{influx.source_name}_{'_'.join(cols)}"
                        desc = (f"MySQL 索引 ({','.join(cols)}) 在 InfluxDB 不存在, 需要同步标签索引")
                        result.index_diffs.append(IndexDiff(
                            diff_type=DiffType.INDEX_ADD,
                            index_name=idx_name,
                            direction=DiffDirection.MYSQL_TO_INFLUX,
                            mysql_index=m_idx,
                            new_columns=list(cols),
                            description=desc
                        ))

            elif i_idx and not m_idx:
                if direction in (SyncDirection.INFLUX_TO_MYSQL, SyncDirection.BIDIRECTIONAL):
                    idx_name = f"idx_{mysql.source_name}_{'_'.join(cols)}"
                    desc = (f"InfluxDB 标签索引 ({','.join(cols)}) 在 MySQL 不存在, 需要新增 BTree 索引")
                    result.index_diffs.append(IndexDiff(
                        diff_type=DiffType.INDEX_ADD,
                        index_name=idx_name,
                        direction=DiffDirection.INFLUX_TO_MYSQL,
                        influx_index=i_idx,
                        new_columns=list(cols),
                        description=desc
                    ))

            elif m_idx and i_idx:
                if m_idx.unique != i_idx.unique:
                    desc = (f"索引 ({','.join(cols)}) unique 属性不一致: "
                            f"MySQL={m_idx.unique}, InfluxDB={i_idx.unique}")
                    result.index_diffs.append(IndexDiff(
                        diff_type=DiffType.INDEX_COLUMN_CHANGE,
                        index_name=m_idx_name or i_idx_name,
                        direction=DiffDirection.INFLUX_TO_MYSQL,
                        mysql_index=m_idx,
                        influx_index=i_idx,
                        description=desc
                    ))

        primary_diff = set(mysql.primary_keys) ^ set(influx.primary_keys)
        if primary_diff:
            only_in_mysql = set(mysql.primary_keys) - set(influx.primary_keys)
            only_in_influx = set(influx.primary_keys) - set(mysql.primary_keys)
            if only_in_mysql and direction in (SyncDirection.MYSQL_TO_INFLUX, SyncDirection.BIDIRECTIONAL):
                desc = f"主键差异: MySQL有{list(only_in_mysql)}, InfluxDB缺少, 需同步为标签"
                for pk in only_in_mysql:
                    result.index_diffs.append(IndexDiff(
                        diff_type=DiffType.PRIMARY_KEY_CHANGE,
                        index_name=f"pk_{pk}",
                        direction=DiffDirection.MYSQL_TO_INFLUX,
                        new_columns=[pk],
                        description=desc
                    ))
            if only_in_influx and direction in (SyncDirection.INFLUX_TO_MYSQL, SyncDirection.BIDIRECTIONAL):
                desc = f"主键差异: InfluxDB有{list(only_in_influx)}, MySQL缺少"
                for pk in only_in_influx:
                    result.index_diffs.append(IndexDiff(
                        diff_type=DiffType.PRIMARY_KEY_CHANGE,
                        index_name=f"pk_{pk}",
                        direction=DiffDirection.INFLUX_TO_MYSQL,
                        new_columns=[pk],
                        description=desc
                    ))
