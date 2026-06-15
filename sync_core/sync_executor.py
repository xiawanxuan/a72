"""
同步脚本生成执行器
功能：
1. 将差异分析结果转换为可执行的 MySQL SQL / InfluxDB Flux 脚本
2. 支持两种运行模式：预览（仅输出脚本）、执行（实际运行）
3. 为每一步操作生成对应的回滚脚本
4. 执行失败时自动触发 RollbackEngine 回滚
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime

from .diff_engine import (PairDiffResult, DiffAnalysisResult, FieldDiff, IndexDiff,
                          DiffType, DiffDirection)
from .metadata_collector import UnifiedSchema, FieldMetadata
from .config_manager import ConfigManager, SyncDirection, RunMode, SyncRule
from .log_rollback import (SessionManager, RollbackEngine, RollbackStep,
                           SyncPhase, LogLevel)
from .db_adapters.mysql_adapter import MySQLAdapter
from .db_adapters.influxdb_adapter import InfluxDBAdapter

logger = logging.getLogger(__name__)


@dataclass
class ScriptItem:
    """单个脚本项（含执行 + 回滚）"""
    item_id: str
    target_db: str  # "mysql" | "influxdb"
    script_type: str  # "sql" | "flux"
    operation: str  # 描述
    exec_script: str  # 执行脚本
    rollback_script: str  # 回滚脚本
    pair_rule_id: Optional[str] = None
    diff_type: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "item_id": self.item_id,
            "target_db": self.target_db,
            "script_type": self.script_type,
            "operation": self.operation,
            "exec_script": self.exec_script,
            "rollback_script": self.rollback_script,
            "pair_rule_id": self.pair_rule_id,
            "diff_type": self.diff_type,
            "depends_on": self.depends_on
        }


@dataclass
class GenerateResult:
    """脚本生成结果"""
    mysql_scripts: List[ScriptItem] = field(default_factory=list)
    influxdb_scripts: List[ScriptItem] = field(default_factory=list)
    create_table_scripts: List[ScriptItem] = field(default_factory=list)

    @property
    def all_scripts(self) -> List[ScriptItem]:
        return self.create_table_scripts + self.mysql_scripts + self.influxdb_scripts

    def summary(self) -> Dict:
        by_type: Dict[str, int] = {}
        for s in self.all_scripts:
            k = f"{s.target_db}_{s.diff_type or 'create'}"
            by_type[k] = by_type.get(k, 0) + 1
        return {
            "total": len(self.all_scripts),
            "mysql_count": len(self.mysql_scripts) + len(
                [s for s in self.create_table_scripts if s.target_db == "mysql"]),
            "influxdb_count": len(self.influxdb_scripts) + len(
                [s for s in self.create_table_scripts if s.target_db == "influxdb"]),
            "breakdown": by_type
        }

    def to_dict(self) -> Dict:
        return {
            "summary": self.summary(),
            "create_table_scripts": [s.to_dict() for s in self.create_table_scripts],
            "mysql_scripts": [s.to_dict() for s in self.mysql_scripts],
            "influxdb_scripts": [s.to_dict() for s in self.influxdb_scripts]
        }


@dataclass
class ExecuteResult:
    """执行结果"""
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    failed_items: List[Dict] = field(default_factory=list)
    rollback_triggered: bool = False
    rollback_report: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "total": self.total,
            "success": self.success,
            "failed": self.failed,
            "skipped": self.skipped,
            "failed_items": self.failed_items,
            "rollback_triggered": self.rollback_triggered,
            "rollback_report": self.rollback_report
        }


class ScriptGenerator:
    """脚本生成器 - 负责 SQL / Flux 脚本生成"""

    def __init__(self, config: ConfigManager):
        self.config = config
        self._item_counter = 0

    def _next_id(self, prefix: str = "SCRIPT") -> str:
        self._item_counter += 1
        return f"{prefix}_{datetime.now().strftime('%H%M%S')}_{self._item_counter:04d}"

    def generate_all(self, diff_result: DiffAnalysisResult) -> GenerateResult:
        """全量脚本生成入口"""
        gen = GenerateResult()
        logger.info(f"========== 开始生成同步脚本: {len(diff_result.pair_results)} 对差异 ==========")

        for pair in diff_result.pair_results:
            try:
                self._generate_pair_scripts(pair, gen)
            except Exception as e:
                logger.error(f"生成脚本失败 [pair {pair.mysql_schema.source_name}]: {e}", exc_info=True)

        for src_type, schema, direction in diff_result.unpaired_create_targets:
            try:
                if src_type == "mysql":
                    self._gen_influx_create_from_mysql(schema, direction, gen)
                else:
                    self._gen_mysql_create_from_influx(schema, direction, gen)
            except Exception as e:
                logger.error(f"生成建表脚本失败 [{src_type} {schema.source_name}]: {e}", exc_info=True)

        logger.info(f"========== 脚本生成完成: {gen.summary()} ==========")
        return gen

    def _generate_pair_scripts(self, pair: PairDiffResult, gen: GenerateResult):
        """生成单对差异的脚本"""
        rule_id = pair.rule.rule_id if pair.rule else None
        mysql_table = pair.mysql_schema.source_name
        influx_meas = pair.influx_schema.source_name

        field_diffs_sorted = sorted(pair.field_diffs,
                                    key=lambda d: (d.direction.value,
                                                   d.diff_type.value not in (
                                                       "field_drop", "tag_drop"),
                                                   d.field_name))

        for diff in field_diffs_sorted:
            if diff.direction == DiffDirection.MYSQL_TO_INFLUX:
                self._gen_influx_field_diff(diff, influx_meas, rule_id, gen)
            else:
                self._gen_mysql_field_diff(diff, mysql_table, rule_id, pair.mysql_schema, gen)

        for diff in pair.index_diffs:
            if diff.direction == DiffDirection.MYSQL_TO_INFLUX:
                self._gen_influx_index_diff(diff, influx_meas, rule_id, gen)
            else:
                self._gen_mysql_index_diff(diff, mysql_table, rule_id, gen)

    def _gen_influx_field_diff(self, diff: FieldDiff, measurement: str,
                               rule_id: Optional[str], gen: GenerateResult):
        """生成 InfluxDB 侧字段变更脚本"""
        influx_field = diff.influx_field
        mysql_field = diff.mysql_field or diff.influx_field

        if diff.diff_type in (DiffType.FIELD_ADD, DiffType.TAG_ADD):
            fname = diff.field_name
            ftype = mysql_field.type if mysql_field else "string"
            is_tag = diff.diff_type == DiffType.TAG_ADD or (mysql_field and mysql_field.is_tag)

            op = f"InfluxDB measurement[{measurement}] " \
                 f"{'新增标签' if is_tag else '新增字段'}[{fname}]({ftype})"

            exec_script = self._flux_write_schema_point(
                measurement, fname, ftype, is_tag,
                tag_values=mysql_field.sample_values[:3] if mysql_field else []
            )
            if is_tag:
                exec_script = self._flux_add_tag_migration(
                    measurement, fname, ftype
                ) + "\n\n-- Schema marker:\n" + exec_script

            rollback = f"-- Note: InfluxDB 字段无法彻底删除; " \
                       f"建议通过 retention policy 清理旧数据.\n" \
                       f"-- 如需立即移除标签[{fname}], 执行以下 Flux 重写任务:\n" \
                       f"{self._flux_drop_field_or_tag(measurement, fname, is_tag)}"

            gen.influxdb_scripts.append(ScriptItem(
                item_id=self._next_id("IFX"),
                target_db="influxdb", script_type="flux",
                operation=op, exec_script=exec_script,
                rollback_script=rollback,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

        elif diff.diff_type == DiffType.FIELD_TYPE_CHANGE:
            fname = diff.field_name
            old_type = diff.old_value or "string"
            new_type = diff.new_value or "string"

            op = f"InfluxDB measurement[{measurement}] 字段[{fname}] 类型变更 {old_type}->{new_type}"

            exec_script = self._flux_migrate_field_type(measurement, fname, old_type, new_type)
            rollback = self._flux_migrate_field_type(measurement, fname, new_type, old_type)

            gen.influxdb_scripts.append(ScriptItem(
                item_id=self._next_id("IFX"),
                target_db="influxdb", script_type="flux",
                operation=op, exec_script=exec_script,
                rollback_script=rollback,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

        elif diff.diff_type in (DiffType.FIELD_DROP, DiffType.TAG_DROP):
            fname = diff.field_name
            is_tag = diff.diff_type == DiffType.TAG_DROP
            op = f"InfluxDB measurement[{measurement}] " \
                 f"{'删除标签' if is_tag else '删除字段'}[{fname}]"
            exec_script = self._flux_drop_field_or_tag(measurement, fname, is_tag)
            rollback = f"-- 无法恢复已删除的 InfluxDB 字段数据; " \
                       f"如需重新标记请从 MySQL 重新同步."

            gen.influxdb_scripts.append(ScriptItem(
                item_id=self._next_id("IFX"),
                target_db="influxdb", script_type="flux",
                operation=op, exec_script=exec_script,
                rollback_script=rollback,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

    def _gen_mysql_field_diff(self, diff: FieldDiff, table: str,
                              rule_id: Optional[str],
                              mysql_schema: UnifiedSchema, gen: GenerateResult):
        """生成 MySQL 侧字段变更脚本"""
        influx_field = diff.influx_field
        ref_field = diff.mysql_field or diff.influx_field

        if diff.diff_type in (DiffType.FIELD_ADD, DiffType.TAG_ADD):
            fname = diff.field_name
            mysql_type = self.config.map_influx_type_to_mysql(
                ref_field.type if ref_field else "string"
            )
            nullable = "NULL" if (ref_field and ref_field.nullable) else "NOT NULL"
            default_clause = ""
            if ref_field and ref_field.default and str(ref_field.default).lower() != "none":
                default_clause = f" DEFAULT '{ref_field.default}'"

            after_clause = ""
            field_names = mysql_schema.get_field_names()
            if fname in field_names and field_names.index(fname) > 0:
                after_clause = f" AFTER `{field_names[field_names.index(fname) - 1]}`"
            elif field_names:
                after_clause = f" AFTER `{field_names[-1]}`"

            op = f"MySQL 表[{table}] 新增字段[{fname}]({mysql_type})"
            exec_sql = (f"ALTER TABLE `{table}` "
                        f"ADD COLUMN `{fname}` {mysql_type} {nullable}{default_clause}{after_clause};")
            rollback_sql = f"ALTER TABLE `{table}` DROP COLUMN `{fname}`;"

            gen.mysql_scripts.append(ScriptItem(
                item_id=self._next_id("MYSQL"),
                target_db="mysql", script_type="sql",
                operation=op, exec_script=exec_sql,
                rollback_script=rollback_sql,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

        elif diff.diff_type == DiffType.FIELD_TYPE_CHANGE:
            fname = diff.field_name
            new_type = self.config.map_influx_type_to_mysql(
                (diff.new_value or "string") if isinstance(diff.new_value, str) else "string"
            )
            op = f"MySQL 表[{table}] 字段[{fname}] 类型变更为 {new_type}"
            exec_sql = f"ALTER TABLE `{table}` MODIFY COLUMN `{fname}` {new_type};"
            old_type = self.config.map_influx_type_to_mysql(
                (diff.old_value or "string") if isinstance(diff.old_value, str) else "string"
            )
            rollback_sql = f"ALTER TABLE `{table}` MODIFY COLUMN `{fname}` {old_type};"
            gen.mysql_scripts.append(ScriptItem(
                item_id=self._next_id("MYSQL"),
                target_db="mysql", script_type="sql",
                operation=op, exec_script=exec_sql,
                rollback_script=rollback_sql,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

        elif diff.diff_type == DiffType.FIELD_NULLABLE_CHANGE:
            fname = diff.field_name
            ref = diff.mysql_field or diff.influx_field
            base_type = ref.raw_type or self.config.map_influx_type_to_mysql(ref.type)
            new_null = "" if not diff.new_value else "NULL"
            old_null = "" if not diff.old_value else "NULL"
            op = f"MySQL 表[{table}] 字段[{fname}] nullable 属性变更"
            exec_sql = f"ALTER TABLE `{table}` MODIFY COLUMN `{fname}` {base_type} {new_null};"
            rollback_sql = f"ALTER TABLE `{table}` MODIFY COLUMN `{fname}` {base_type} {old_null};"
            gen.mysql_scripts.append(ScriptItem(
                item_id=self._next_id("MYSQL"),
                target_db="mysql", script_type="sql",
                operation=op, exec_script=exec_sql,
                rollback_script=rollback_sql,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

    def _gen_influx_index_diff(self, diff: IndexDiff, measurement: str,
                               rule_id: Optional[str], gen: GenerateResult):
        """生成 InfluxDB 侧索引（标签）脚本"""
        cols = diff.new_columns or []

        if diff.diff_type == DiffType.INDEX_ADD:
            tags_part = ",".join(cols)
            op = f"InfluxDB measurement[{measurement}] 确保标签索引: {tags_part}"
            exec_script = self._flux_ensure_tag_indexes(measurement, cols)
            rollback = f"-- 标签索引由 cardinality 自动管理; 无需显式删除."
            gen.influxdb_scripts.append(ScriptItem(
                item_id=self._next_id("IFX"),
                target_db="influxdb", script_type="flux",
                operation=op, exec_script=exec_script,
                rollback_script=rollback,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

        elif diff.diff_type == DiffType.PRIMARY_KEY_CHANGE:
            cols = diff.new_columns or []
            op = f"InfluxDB measurement[{measurement}] 主键(必选标签)同步: {cols}"
            exec_script = self._flux_ensure_tag_indexes(measurement, cols)
            rollback = "-- 主键变更无显式回滚."
            gen.influxdb_scripts.append(ScriptItem(
                item_id=self._next_id("IFX"),
                target_db="influxdb", script_type="flux",
                operation=op, exec_script=exec_script,
                rollback_script=rollback,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

    def _gen_mysql_index_diff(self, diff: IndexDiff, table: str,
                              rule_id: Optional[str], gen: GenerateResult):
        """生成 MySQL 侧索引脚本"""
        idx_name = diff.index_name or f"idx_{table}_{'_'.join(diff.new_columns)}"
        cols_sql = ",".join(f"`{c}`" for c in diff.new_columns)

        if diff.diff_type == DiffType.INDEX_ADD:
            unique_keyword = "UNIQUE" if (diff.mysql_index and diff.mysql_index.unique) else "INDEX"
            op = f"MySQL 表[{table}] 新增{'唯一' if unique_keyword == 'UNIQUE' else ''}索引 {idx_name} ({cols_sql})"
            exec_sql = f"CREATE {unique_keyword} {idx_name} ON `{table}` ({cols_sql});"
            rollback_sql = f"DROP INDEX {idx_name} ON `{table}`;"
            gen.mysql_scripts.append(ScriptItem(
                item_id=self._next_id("MYSQL"),
                target_db="mysql", script_type="sql",
                operation=op, exec_script=exec_sql,
                rollback_script=rollback_sql,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

        elif diff.diff_type == DiffType.INDEX_COLUMN_CHANGE:
            op = f"MySQL 表[{table}] 重建索引 {idx_name}"
            drop_sql = f"DROP INDEX {idx_name} ON `{table}`;"
            create_sql = f"CREATE INDEX {idx_name} ON `{table}` ({cols_sql});"
            exec_sql = drop_sql + "\n" + create_sql
            rollback_sql = create_sql + "\n" + drop_sql
            gen.mysql_scripts.append(ScriptItem(
                item_id=self._next_id("MYSQL"),
                target_db="mysql", script_type="sql",
                operation=op, exec_script=exec_sql,
                rollback_script=rollback_sql,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

        elif diff.diff_type == DiffType.PRIMARY_KEY_CHANGE:
            cols = diff.new_columns or []
            cols_sql = ",".join(f"`{c}`" for c in cols)
            op = f"MySQL 表[{table}] 同步主键: {cols}"
            exec_sql = (f"ALTER TABLE `{table}` DROP PRIMARY KEY, "
                        f"ADD PRIMARY KEY ({cols_sql});")
            rollback = "-- 主键变更请手动恢复原主键结构."
            gen.mysql_scripts.append(ScriptItem(
                item_id=self._next_id("MYSQL"),
                target_db="mysql", script_type="sql",
                operation=op, exec_script=exec_sql,
                rollback_script=rollback,
                pair_rule_id=rule_id, diff_type=diff.diff_type.value
            ))

    def _gen_influx_create_from_mysql(self, mysql_schema: UnifiedSchema,
                                       direction: SyncDirection, gen: GenerateResult):
        """从 MySQL 表元数据生成 InfluxDB measurement 初始化脚本"""
        meas = mysql_schema.paired_name
        op = f"创建 InfluxDB measurement[{meas}] (从 MySQL 表[{mysql_schema.source_name}])"

        tag_names = mysql_schema.tag_names
        field_defs = []
        for f in mysql_schema.fields.values():
            mark = "TAG" if f.name in tag_names else f.type.upper()
            field_defs.append(f"- `{f.name}`: {mark}")

        schema_comment = f"/*\nMeasurement: {meas}\n来源: MySQL {mysql_schema.source_name}\n字段定义:\n"
        schema_comment += "\n".join(field_defs) + "\n*/\n"

        exec_script = schema_comment + self._flux_init_measurement(meas, mysql_schema)
        rollback = (f"-- InfluxDB measurement 无需显式 DROP;\n"
                    f"-- 如需删除 [{meas}] 所有数据, 执行:\n"
                    f"-- DeleteAPI.delete(start=1970, stop=2100, predicate='_measurement=\"{meas}\"')")

        gen.create_table_scripts.append(ScriptItem(
            item_id=self._next_id("IFXCREATE"),
            target_db="influxdb", script_type="flux",
            operation=op, exec_script=exec_script,
            rollback_script=rollback, diff_type="create_measurement"
        ))

    def _gen_mysql_create_from_influx(self, influx_schema: UnifiedSchema,
                                       direction: SyncDirection, gen: GenerateResult):
        """从 InfluxDB measurement 元数据生成 MySQL 建表脚本"""
        table = influx_schema.paired_name
        op = f"创建 MySQL 表[{table}] (从 InfluxDB measurement[{influx_schema.source_name}])"

        col_defs = []
        pk_fields = influx_schema.primary_keys or influx_schema.tag_names[:1]

        for fname, fmeta in influx_schema.fields.items():
            mtype = self.config.map_influx_type_to_mysql(fmeta.type)
            null = "NULL"
            default = ""
            if fname in pk_fields:
                null = "NOT NULL"
            col_defs.append(f"  `{fname}` {mtype} {null}{default}")

        if pk_fields:
            col_defs.append(f"  PRIMARY KEY ({','.join(f'`{p}`' for p in pk_fields)})")

        tag_cols = [t for t in influx_schema.tag_names if t not in pk_fields]
        for t in tag_cols:
            col_defs.append(f"  KEY `idx_{table}_{t}` (`{t}`)")

        create_sql = (f"CREATE TABLE IF NOT EXISTS `{table}` (\n"
                      + ",\n".join(col_defs)
                      + f"\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 "
                      f"COMMENT='Synced from InfluxDB {influx_schema.source_name}';")

        rollback_sql = f"DROP TABLE IF EXISTS `{table}`;"

        gen.create_table_scripts.append(ScriptItem(
            item_id=self._next_id("MYSQLCREATE"),
            target_db="mysql", script_type="sql",
            operation=op, exec_script=create_sql,
            rollback_script=rollback_sql, diff_type="create_table"
        ))

    # ---------------- Flux 脚本模板方法 ----------------

    def _flux_init_measurement(self, measurement: str, mysql_schema: UnifiedSchema) -> str:
        tag_names = mysql_schema.tag_names
        field_items = []
        tag_items = []
        for fname, fmeta in mysql_schema.fields.items():
            if fname in tag_names:
                tag_items.append(f"    {fname}: \"SCHEMA_TAG_{fname}\"")
            else:
                vmap = {"string": "\"SCHEMA_FIELD_PLACEHOLDER\"",
                        "int": "0", "float": "0.0", "boolean": "false",
                        "datetime": "2024-01-01T00:00:00Z"}
                val = vmap.get(fmeta.type, "\"SCHEMA_FIELD_PLACEHOLDER\"")
                field_items.append(f"    {fname}: {val}")

        tags_block = "\n".join(tag_items)
        fields_block = "\n".join(field_items)
        return f'''// ============================================================
// 初始化 Measurement Schema: {measurement}
// 通过写入一个带完整字段结构的标记数据点建立 schema
// ============================================================
import "array"
import "influxdata/influxdb/v1"

schemaMarkerData = array.from(rows: [
  {{
    _time: now(),
    _measurement: "{measurement}",
{tags_block}
{fields_block}
  }}
])

schemaMarkerData
  |> to(bucket: "DEVICE_METRICS_BUCKET", org: "SMART_FACTORY_ORG")

// 验证: 列出 measurement 的字段和标签
// import "influxdata/influxdb/schema"
// schema.measurementFieldKeys(bucket: "DEVICE_METRICS_BUCKET", measurement: "{measurement}")
// schema.measurementTagKeys(bucket: "DEVICE_METRICS_BUCKET", measurement: "{measurement}")
'''

    def _flux_write_schema_point(self, measurement: str, field: str,
                                 ftype: str, is_tag: bool,
                                 tag_values: List = None) -> str:
        if is_tag:
            sample = "SAMPLE_VALUE" if not tag_values else tag_values[0]
            body = (f"    _measurement: \"{measurement}\",\n"
                    f"    {field}: \"{sample}\",\n"
                    f"    schema_marker_field: \"{field}\"")
        else:
            vmap = {"string": "\"NEW_FIELD_PLACEHOLDER\"",
                    "int": "0", "float": "0.0", "boolean": "false"}
            val = vmap.get(ftype, "\"NEW_FIELD_PLACEHOLDER\"")
            body = (f"    _measurement: \"{measurement}\",\n"
                    f"    schema_marker: \"{field}\",\n"
                    f"    {field}: {val}")

        return f'''import "array"
// 写入 schema 标记数据点: measurement={measurement}, field={field}, is_tag={is_tag}
array.from(rows: [
  {{
    _time: now(),
{body}
  }}
])
|> to(bucket: "DEVICE_METRICS_BUCKET", org: "SMART_FACTORY_ORG")
'''

    def _flux_add_tag_migration(self, measurement: str, tag_name: str,
                                 ftype: str) -> str:
        return f'''// ========================================================
// 将字段 [{tag_name}] 迁移为标签 (通过重写数据点)
// 注意: 需在 InfluxDB 中配置为 Task 定期执行, 或一次性运行
// ========================================================
data = from(bucket: "DEVICE_METRICS_BUCKET")
  |> range(start: -10y, stop: now())
  |> filter(fn: (r) => r._measurement == "{measurement}")

data
  |> map(fn: (r) => ({{ r with
      {tag_name}: string(v: r._value),
      _measurement: "{measurement}"
  }}))
  |> to(bucket: "DEVICE_METRICS_BUCKET", org: "SMART_FACTORY_ORG")
'''

    def _flux_drop_field_or_tag(self, measurement: str, name: str,
                                 is_tag: bool) -> str:
        target = "tags" if is_tag else "columns"
        return f'''// 移除 {measurement} 的 {"标签" if is_tag else "字段"} [{name}]
// 注意: InfluxDB 无法物理删除, 需重建 bucket 或使用 TTL
// 以下 Flux 提供了重写副本移除字段的方式:

from(bucket: "DEVICE_METRICS_BUCKET")
  |> range(start: -10y, stop: now())
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> drop(columns: ["{name}"])
  |> to(bucket: "DEVICE_METRICS_BUCKET_NEW", org: "SMART_FACTORY_ORG")
'''

    def _flux_migrate_field_type(self, measurement: str, field: str,
                                  old_type: str, new_type: str) -> str:
        conversion = {
            ("int", "float"): "float(v: r._value)",
            ("float", "int"): "int(v: r._value)",
            ("string", "int"): "int(v: r._value)",
            ("int", "string"): "string(v: r._value)",
            ("float", "string"): "string(v: r._value)",
            ("string", "float"): "float(v: r._value)",
            ("boolean", "string"): "string(v: r._value)",
        }
        fn = conversion.get((old_type, new_type), f"{new_type}(v: r._value)")
        return f'''// 字段 [{field}] 类型转换 {old_type} -> {new_type}
from(bucket: "DEVICE_METRICS_BUCKET")
  |> range(start: -10y, stop: now())
  |> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{field}")
  |> map(fn: (r) => ({{ r with
      _value: {fn},
      _field: "{field}"
  }}))
  |> to(bucket: "DEVICE_METRICS_BUCKET", org: "SMART_FACTORY_ORG")
'''

    def _flux_ensure_tag_indexes(self, measurement: str, tags: List[str]) -> str:
        tag_enum = "\n".join([f"// - {t}" for t in tags])
        return f'''// ========================================================
// 为 measurement[{measurement}] 确保标签索引
// InfluxDB 标签在首次写入时自动索引
// 以下脚本为需要建立基数检查的场景提供预验证
// 标签列表:
{tag_enum}
// ========================================================
import "influxdata/influxdb/schema"

schema.tagKeys(bucket: "DEVICE_METRICS_BUCKET",
               predicate: (r) => r._measurement == "{measurement}")
  |> keep(columns: ["_value"])
  |> filter(fn: (r) => contains(value: r._value,
                                 arr: {json.dumps(tags)}))
  |> yield(name: "existing_tags")
'''

    # ---------------- 导出脚本文件 ----------------

    def export_scripts_to_disk(self, gen_result: GenerateResult,
                                output_dir: str = "output_scripts") -> Dict[str, str]:
        """将脚本导出为磁盘文件"""
        os.makedirs(output_dir, exist_ok=True)
        paths = {}

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        if gen_result.create_table_scripts:
            sql_path = os.path.join(output_dir, f"create_tables_{ts}.sql")
            flux_path = os.path.join(output_dir, f"create_measurements_{ts}.flux")
            with open(sql_path, "w", encoding="utf-8") as fs, \
                 open(flux_path, "w", encoding="utf-8") as ff:
                for s in gen_result.create_table_scripts:
                    line = f"-- ===== {s.operation} =====\n{s.exec_script}\n\n"
                    if s.target_db == "mysql":
                        fs.write(line)
                    else:
                        ff.write(line)
            paths["create_tables_sql"] = sql_path
            paths["create_measurements_flux"] = flux_path

        if gen_result.mysql_scripts:
            path = os.path.join(output_dir, f"mysql_sync_{ts}.sql")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"-- MySQL 同步变更脚本 生成时间 {ts}\n")
                f.write(f"-- 变更数量: {len(gen_result.mysql_scripts)}\n")
                f.write("SET FOREIGN_KEY_CHECKS = 0;\n\n")
                for s in gen_result.mysql_scripts:
                    f.write(f"-- [{s.diff_type}] {s.operation}\n")
                    f.write(f"-- 回滚: {s.rollback_script}\n")
                    f.write(f"{s.exec_script}\n\n")
                f.write("\nSET FOREIGN_KEY_CHECKS = 1;\n")
            paths["mysql_sql"] = path

        if gen_result.influxdb_scripts:
            path = os.path.join(output_dir, f"influxdb_sync_{ts}.flux")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"// InfluxDB 同步 Flux 脚本 生成时间 {ts}\n")
                f.write(f"// 变更数量: {len(gen_result.influxdb_scripts)}\n\n")
                for s in gen_result.influxdb_scripts:
                    f.write(f"// [{s.diff_type}] {s.operation}\n")
                    f.write(f"// 回滚: \n{s.rollback_script}\n\n")
                    f.write(f"{s.exec_script}\n\n")
            paths["influx_flux"] = path

        json_path = os.path.join(output_dir, f"scripts_{ts}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(gen_result.to_dict(), f, ensure_ascii=False, indent=2, default=str)
        paths["manifest"] = json_path

        logger.info(f"脚本已导出到目录: {output_dir}, 文件: {list(paths.keys())}")
        return paths


class SyncExecutor:
    """同步执行器 - 负责执行脚本并管理回滚"""

    def __init__(self, mysql: MySQLAdapter, influx: InfluxDBAdapter,
                 session_mgr: SessionManager, rollback_engine: RollbackEngine,
                 run_mode: RunMode = RunMode.PREVIEW):
        self.mysql = mysql
        self.influx = influx
        self.sm = session_mgr
        self.rb = rollback_engine
        self.run_mode = run_mode
        self._register_executors()

    def _register_executors(self):
        def mysql_ex(sql: str):
            for line in [l.strip() for l in sql.split(";") if l.strip()]:
                if not line.lower().startswith("--") and not line.startswith("/*"):
                    self.mysql.execute_ddl(line)

        def influx_ex(flux: str):
            if "to(bucket:" in flux and "array.from" in flux:
                self.influx.execute_flux(flux)
            else:
                self.influx.execute_flux(flux)

        self.rb.register_executors(mysql_ex, influx_ex)

    def execute(self, gen_result: GenerateResult) -> ExecuteResult:
        """执行脚本"""
        result = ExecuteResult(total=len(gen_result.all_scripts))
        run_mode_str = "预览模式" if self.run_mode == RunMode.PREVIEW else "执行模式"
        self.sm.log(phase=SyncPhase.EXECUTE, level=LogLevel.INFO,
                    message=f"[BEGIN] {run_mode_str}: 共 {result.total} 个脚本项")

        if self.run_mode == RunMode.PREVIEW:
            for s in gen_result.all_scripts:
                logger.info(f"  [PREVIEW][{s.target_db}] {s.operation}")
                logger.debug(f"    执行脚本:\n{s.exec_script[:200]}")
                logger.debug(f"    回滚脚本:\n{s.rollback_script[:200]}")
                result.skipped += 1
                self.sm.log(phase=SyncPhase.EXECUTE, level=LogLevel.INFO,
                            message=f"[预览] [{s.target_db.upper()}] {s.operation}",
                            details={"script_id": s.item_id,
                                     "exec_preview": s.exec_script[:200],
                                     "rollback_preview": s.rollback_script[:200]},
                            rule_id=s.pair_rule_id)
            self.sm.log(phase=SyncPhase.PREVIEW, level=LogLevel.INFO,
                        message=f"预览完成, 共 {result.skipped} 个脚本项未执行")
            return result

        try:
            self.mysql.begin_transaction()
        except Exception:
            pass

        try:
            for idx, s in enumerate(gen_result.all_scripts):
                self.sm.log(phase=SyncPhase.EXECUTE, level=LogLevel.INFO,
                            message=f"[{idx + 1}/{result.total}] 执行: {s.operation}",
                            rule_id=s.pair_rule_id)
                try:
                    if s.target_db == "mysql":
                        phase = SyncPhase.EXECUTE_MYSQL
                        self._execute_mysql(s)
                    else:
                        phase = SyncPhase.EXECUTE_INFLUXDB
                        self._execute_influx(s)
                    self._record_success(s)
                    result.success += 1
                    self.sm.log(phase=phase, level=LogLevel.INFO,
                                message=f"执行成功: {s.operation}")
                except Exception as e:
                    result.failed += 1
                    err = f"执行失败 [{s.item_id}]: {s.operation} - {e}"
                    logger.error(err, exc_info=True)
                    self.sm.log(phase=SyncPhase.EXECUTE, level=LogLevel.ERROR,
                                message=err,
                                details={"item_id": s.item_id,
                                         "script": s.exec_script[:500],
                                         "error": str(e)},
                                rule_id=s.pair_rule_id)
                    result.failed_items.append({"item_id": s.item_id,
                                                "operation": s.operation,
                                                "error": str(e),
                                                "target_db": s.target_db})
                    self._record_failed(s)
                    raise

            try:
                self.mysql.commit_transaction()
            except Exception:
                pass

        except Exception:
            try:
                self.mysql.rollback_transaction()
            except Exception:
                pass
            result.rollback_triggered = True
            self.sm.log(phase=SyncPhase.ROLLBACK, level=LogLevel.WARNING,
                        message="检测到失败, 开始自动回滚所有变更")
            result.rollback_report = self.rb.execute_rollback()
            if isinstance(result.rollback_report, bool):
                result.rollback_report = self.rb.generate_rollback_report()

        self.sm.log(phase=SyncPhase.EXECUTE, level=LogLevel.INFO,
                    message=f"[END] 执行完成: 成功={result.success}, 失败={result.failed}, "
                            f"回滚={result.rollback_triggered}")
        return result

    def _execute_mysql(self, s: ScriptItem):
        """执行 MySQL 脚本"""
        for raw in s.exec_script.split(";"):
            line = raw.strip()
            if not line or line.startswith("--") or line.startswith("/*"):
                continue
            try:
                self.mysql.execute_ddl(line)
            except Exception as e:
                if "Duplicate column" in str(e) or "already exists" in str(e):
                    logger.warning(f"DDL 幂等跳过: {e}")
                elif "check that column/key exists" in str(e) and "DROP" in line.upper():
                    logger.warning(f"DROP 幂等跳过(不存在): {e}")
                else:
                    raise

    def _execute_influx(self, s: ScriptItem):
        """执行 InfluxDB Flux 脚本"""
        if "to(bucket:" not in s.exec_script and "array.from" not in s.exec_script:
            try:
                self.influx.execute_flux(s.exec_script)
            except Exception as e:
                logger.warning(f"只读 Flux 查询执行警告 (可忽略): {e}")
            return

        try:
            self.influx.execute_flux(s.exec_script)
        except Exception as e:
            if "already" in str(e).lower() or "exists" in str(e).lower():
                logger.warning(f"InfluxDB 幂等跳过: {e}")
            else:
                raise

    def _record_success(self, s: ScriptItem):
        self.sm.log_operation(
            target=s.target_db,
            operation=s.operation,
            script=s.exec_script,
            rollback_script=s.rollback_script,
            script_type=s.script_type,
            success=True
        )

    def _record_failed(self, s: ScriptItem):
        self.sm.log_operation(
            target=s.target_db,
            operation=s.operation,
            script=s.exec_script,
            rollback_script=s.rollback_script,
            script_type=s.script_type,
            success=False
        )
