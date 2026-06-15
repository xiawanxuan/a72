"""
时序测量元数据采集器
负责从 MySQL 和 InfluxDB 双边采集元数据，并进行统一规范化
输出标准化的元数据结构供差异对比引擎使用
"""

import logging
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime

from .db_adapters.mysql_adapter import MySQLAdapter
from .db_adapters.influxdb_adapter import InfluxDBAdapter
from .config_manager import ConfigManager, SyncRule, SyncDirection

logger = logging.getLogger(__name__)


@dataclass
class FieldMetadata:
    """标准化的字段元数据"""
    name: str
    type: str
    nullable: bool = True
    default: Optional[str] = None
    primary_key: bool = False
    is_tag: bool = False
    is_index: bool = False
    comment: str = ""
    raw_type: str = ""
    cardinality: int = 0
    sample_values: List[Any] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
            "default": self.default,
            "primary_key": self.primary_key,
            "is_tag": self.is_tag,
            "is_index": self.is_index,
            "comment": self.comment,
            "raw_type": self.raw_type,
            "cardinality": self.cardinality,
            "sample_values": self.sample_values
        }


@dataclass
class IndexMetadata:
    """标准化的索引元数据"""
    name: str
    columns: List[str]
    unique: bool = False
    index_type: str = "INDEX"

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "columns": self.columns,
            "unique": self.unique,
            "type": self.index_type
        }


@dataclass
class UnifiedSchema:
    """统一的数据源元数据结构（MySQL表 或 InfluxDB measurement）"""
    source_type: str  # "mysql" or "influxdb"
    source_name: str  # table name or measurement name
    paired_name: str  # 配对的另一侧名称
    rule: Optional[SyncRule]
    fields: Dict[str, FieldMetadata] = field(default_factory=dict)
    indexes: Dict[str, IndexMetadata] = field(default_factory=dict)
    primary_keys: List[str] = field(default_factory=list)
    tag_names: List[str] = field(default_factory=list)
    collected_at: datetime = field(default_factory=datetime.now)
    extra: Dict[str, Any] = field(default_factory=dict)

    def get_field_names(self) -> List[str]:
        return sorted(self.fields.keys())

    def get_index_names(self) -> List[str]:
        return sorted(self.indexes.keys())

    def to_dict(self) -> Dict:
        return {
            "source_type": self.source_type,
            "source_name": self.source_name,
            "paired_name": self.paired_name,
            "rule_id": self.rule.rule_id if self.rule else None,
            "fields": {k: v.to_dict() for k, v in self.fields.items()},
            "indexes": {k: v.to_dict() for k, v in self.indexes.items()},
            "primary_keys": self.primary_keys,
            "tag_names": self.tag_names,
            "collected_at": self.collected_at.isoformat(),
            "extra": self.extra
        }


@dataclass
class CollectResult:
    """元数据采集结果"""
    mysql_schemas: Dict[str, UnifiedSchema] = field(default_factory=dict)
    influxdb_schemas: Dict[str, UnifiedSchema] = field(default_factory=dict)
    paired_schemas: List[tuple] = field(default_factory=list)  # (mysql_schema, influxdb_schema, rule)
    unpaired_mysql: List[UnifiedSchema] = field(default_factory=list)
    unpaired_influxdb: List[UnifiedSchema] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "mysql_schemas": {k: v.to_dict() for k, v in self.mysql_schemas.items()},
            "influxdb_schemas": {k: v.to_dict() for k, v in self.influxdb_schemas.items()},
            "paired_count": len(self.paired_schemas),
            "unpaired_mysql": [s.source_name for s in self.unpaired_mysql],
            "unpaired_influxdb": [s.source_name for s in self.unpaired_influxdb],
            "errors": self.errors,
            "stats": self.stats
        }


class MetadataCollector:
    """元数据采集器"""

    def __init__(self, mysql_adapter: MySQLAdapter,
                 influxdb_adapter: InfluxDBAdapter,
                 config_manager: ConfigManager):
        self.mysql = mysql_adapter
        self.influxdb = influxdb_adapter
        self.config = config_manager

    def collect_all(self) -> CollectResult:
        """全量采集双边元数据"""
        result = CollectResult()
        logger.info("========== 开始采集双边元数据 ==========")

        try:
            mysql_tables = self.mysql.fetch_tables()
            allowed_tables = self.config.filter_allowed_tables(mysql_tables)
            logger.info(f"MySQL 候选表: {len(allowed_tables)} / {len(mysql_tables)}")
        except Exception as e:
            msg = f"采集 MySQL 表列表失败: {e}"
            logger.error(msg, exc_info=True)
            result.errors.append(msg)
            allowed_tables = []

        try:
            influx_measurements = self.influxdb.fetch_measurements()
            allowed_measurements = self.config.filter_allowed_measurements(influx_measurements)
            logger.info(f"InfluxDB 候选 measurement: {len(allowed_measurements)} / {len(influx_measurements)}")
        except Exception as e:
            msg = f"采集 InfluxDB measurement 列表失败: {e}"
            logger.error(msg, exc_info=True)
            result.errors.append(msg)
            allowed_measurements = []

        sync_tables = self.config.get_sync_tables()
        sync_measurements = self.config.get_sync_measurements()

        for t in sync_tables:
            if t not in allowed_tables:
                allowed_tables.append(t)
        for m in sync_measurements:
            if m not in allowed_measurements:
                allowed_measurements.append(m)

        logger.info("开始采集 MySQL 表元数据...")
        mysql_raw_schemas = {}
        for table in allowed_tables:
            try:
                mysql_raw_schemas[table] = self.mysql.fetch_table_schema(table)
            except Exception as e:
                msg = f"采集 MySQL 表 [{table}] 元数据失败: {e}"
                logger.warning(msg)
                result.errors.append(msg)

        logger.info("开始采集 InfluxDB measurement 元数据...")
        influx_raw_schemas = {}
        for ms in allowed_measurements:
            try:
                influx_raw_schemas[ms] = self.influxdb.fetch_measurement_schema(ms)
            except Exception as e:
                msg = f"采集 InfluxDB measurement [{ms}] 元数据失败: {e}"
                logger.warning(msg)
                result.errors.append(msg)

        logger.info("开始构建标准化元数据结构...")
        for table_name, raw in mysql_raw_schemas.items():
            try:
                schema = self._build_mysql_schema(table_name, raw)
                result.mysql_schemas[table_name] = schema
            except Exception as e:
                msg = f"构建 MySQL 标准元数据失败 [{table_name}]: {e}"
                logger.warning(msg)
                result.errors.append(msg)

        for ms_name, raw in influx_raw_schemas.items():
            try:
                schema = self._build_influxdb_schema(ms_name, raw)
                result.influxdb_schemas[ms_name] = schema
            except Exception as e:
                msg = f"构建 InfluxDB 标准元数据失败 [{ms_name}]: {e}"
                logger.warning(msg)
                result.errors.append(msg)

        self._pair_schemas(result)

        result.stats = {
            "mysql_tables": len(result.mysql_schemas),
            "influxdb_measurements": len(result.influxdb_schemas),
            "paired_schemas": len(result.paired_schemas),
            "unpaired_mysql": len(result.unpaired_mysql),
            "unpaired_influxdb": len(result.unpaired_influxdb),
            "errors": len(result.errors)
        }

        logger.info(f"========== 元数据采集完成: {result.stats} ==========")
        return result

    def _build_mysql_schema(self, table_name: str, raw: Dict[str, Any]) -> UnifiedSchema:
        """将 MySQL 原始元数据转换为标准化结构"""
        rule = self.config.get_rule_by_mysql_table(table_name)
        paired_name = rule.influxdb_measurement if rule else table_name

        tag_set: Set[str] = set()
        index_set: Set[str] = set()
        if rule:
            tag_set = set(rule.tag_fields)
            index_set = set(rule.index_fields)

        fields: Dict[str, FieldMetadata] = {}
        for fname, finfo in raw.get("fields", {}).items():
            if not self.config.is_field_allowed(fname):
                continue
            normalized_type = self.config.map_mysql_type_to_influx(finfo.get("type", "VARCHAR"))
            fm = FieldMetadata(
                name=fname,
                type=normalized_type,
                nullable=finfo.get("nullable", True),
                default=finfo.get("default"),
                primary_key=finfo.get("primary_key", False),
                is_tag=fname in tag_set,
                is_index=(fname in index_set) or finfo.get("primary_key", False),
                comment=finfo.get("comment", ""),
                raw_type=finfo.get("full_type", finfo.get("type", ""))
            )
            fields[fname] = fm

        indexes: Dict[str, IndexMetadata] = {}
        for iname, iinfo in raw.get("indexes", {}).items():
            idx_cols = [c for c in iinfo.get("columns", []) if c in fields]
            if idx_cols:
                indexes[iname] = IndexMetadata(
                    name=iname,
                    columns=idx_cols,
                    unique=iinfo.get("unique", False),
                    index_type=iinfo.get("type", "INDEX")
                )

        primary_keys = [k for k in raw.get("primary_keys", []) if k in fields]

        schema = UnifiedSchema(
            source_type="mysql",
            source_name=table_name,
            paired_name=paired_name,
            rule=rule,
            fields=fields,
            indexes=indexes,
            primary_keys=primary_keys,
            tag_names=[f for f in sorted(tag_set) if f in fields]
        )
        return schema

    def _build_influxdb_schema(self, ms_name: str, raw: Dict[str, Any]) -> UnifiedSchema:
        """将 InfluxDB 原始元数据转换为标准化结构"""
        rule = self.config.get_rule_by_measurement(ms_name)
        paired_name = rule.mysql_table if rule else ms_name

        tag_set: Set[str] = set()
        index_set: Set[str] = set()
        if rule:
            tag_set = set(rule.tag_fields)
            index_set = set(rule.index_fields)

        fields: Dict[str, FieldMetadata] = {}

        for tname, tinfo in raw.get("tags", {}).items():
            if not self.config.is_field_allowed(tname):
                continue
            fm = FieldMetadata(
                name=tname,
                type=tinfo.get("type", "string"),
                nullable=True,
                default=None,
                primary_key=False,
                is_tag=True,
                is_index=True,
                raw_type="TAG",
                cardinality=tinfo.get("cardinality", 0),
                sample_values=tinfo.get("sample_values", [])
            )
            fields[tname] = fm

        for fname, finfo in raw.get("fields", {}).items():
            if not self.config.is_field_allowed(fname):
                continue
            if fname in fields:
                continue
            fm = FieldMetadata(
                name=fname,
                type=finfo.get("type", "string"),
                nullable=True,
                default=None,
                primary_key=False,
                is_tag=False,
                is_index=fname in index_set,
                raw_type="FIELD",
                cardinality=finfo.get("cardinality", 0)
            )
            fields[fname] = fm

        if rule:
            for fname, fconf in rule.field_mapping.items():
                if fname not in fields and self.config.is_field_allowed(fname):
                    fields[fname] = FieldMetadata(
                        name=fname,
                        type=fconf.type,
                        nullable=True,
                        default=None,
                        primary_key=fconf.primary_key,
                        is_tag=fname in tag_set,
                        is_index=fconf.index
                    )

        indexes: Dict[str, IndexMetadata] = {}
        if rule:
            for idx_field in rule.index_fields:
                if idx_field in fields:
                    idx_name = f"idx_{ms_name}_{idx_field}"
                    indexes[idx_name] = IndexMetadata(
                        name=idx_name,
                        columns=[idx_field],
                        unique=fields[idx_field].primary_key,
                        index_type="TAG_INDEX" if fields[idx_field].is_tag else "FIELD_INDEX"
                    )

        primary_tag = raw.get("primary_tag")
        primary_keys = []
        if primary_tag and primary_tag in fields:
            primary_keys = [primary_tag]
            fields[primary_tag].primary_key = True

        schema = UnifiedSchema(
            source_type="influxdb",
            source_name=ms_name,
            paired_name=paired_name,
            rule=rule,
            fields=fields,
            indexes=indexes,
            primary_keys=primary_keys,
            tag_names=[f for f in sorted(tag_set) if f in fields]
        )
        return schema

    def _pair_schemas(self, result: CollectResult):
        """配对双边元数据结构"""
        paired_mysql = set()
        paired_influx = set()

        for rule in self.config.get_enabled_rules():
            m_schema = result.mysql_schemas.get(rule.mysql_table)
            i_schema = result.influxdb_schemas.get(rule.influxdb_measurement)
            if m_schema and i_schema:
                result.paired_schemas.append((m_schema, i_schema, rule))
                paired_mysql.add(rule.mysql_table)
                paired_influx.add(rule.influxdb_measurement)
                logger.info(f"配对成功: [MySQL]{rule.mysql_table} <-> [InfluxDB]{rule.influxdb_measurement}")

        for tname, schema in result.mysql_schemas.items():
            if tname not in paired_mysql:
                if schema.paired_name in result.influxdb_schemas:
                    i_schema = result.influxdb_schemas[schema.paired_name]
                    result.paired_schemas.append((schema, i_schema, None))
                    paired_influx.add(schema.paired_name)
                else:
                    result.unpaired_mysql.append(schema)

        for mname, schema in result.influxdb_schemas.items():
            if mname not in paired_influx:
                result.unpaired_influxdb.append(schema)

        logger.info(f"配对统计: 成功 {len(result.paired_schemas)} 对, "
                     f"未配对 MySQL {len(result.unpaired_mysql)}, "
                     f"未配对 InfluxDB {len(result.unpaired_influxdb)}")

    def collect_single_pair(self, rule: SyncRule) -> Optional[tuple]:
        """采集单个同步规则对应的双边元数据"""
        try:
            mysql_raw = self.mysql.fetch_table_schema(rule.mysql_table)
            influx_raw = self.influxdb.fetch_measurement_schema(rule.influxdb_measurement)
            m_schema = self._build_mysql_schema(rule.mysql_table, mysql_raw)
            i_schema = self._build_influxdb_schema(rule.influxdb_measurement, influx_raw)
            return (m_schema, i_schema, rule)
        except Exception as e:
            logger.error(f"采集单对元数据失败 [{rule.rule_id}]: {e}", exc_info=True)
            return None
