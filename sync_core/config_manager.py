"""
同步黑白名单配置管理器
负责加载、解析、管理同步规则 JSON 配置
支持白名单、黑名单、同步方向、类型映射等配置项
"""

import json
import os
import logging
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class SyncDirection(str, Enum):
    """同步方向枚举"""
    MYSQL_TO_INFLUX = "mysql_to_influx"
    INFLUX_TO_MYSQL = "influx_to_mysql"
    BIDIRECTIONAL = "bidirectional"


class RunMode(str, Enum):
    """运行模式枚举"""
    PREVIEW = "preview"
    EXECUTE = "execute"


@dataclass
class FieldMapping:
    """字段映射配置"""
    name: str
    type: str = "string"
    index: bool = False
    primary_key: bool = False
    is_tag: bool = False


@dataclass
class SyncRule:
    """单条同步规则"""
    rule_id: str
    rule_name: str
    mysql_table: str
    influxdb_measurement: str
    sync_direction: SyncDirection
    enabled: bool
    field_mapping: Dict[str, FieldMapping] = field(default_factory=dict)
    tag_fields: List[str] = field(default_factory=list)
    index_fields: List[str] = field(default_factory=list)
    extra_config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "mysql_table": self.mysql_table,
            "influxdb_measurement": self.influxdb_measurement,
            "sync_direction": self.sync_direction.value,
            "enabled": self.enabled,
            "tag_fields": self.tag_fields,
            "index_fields": self.index_fields,
            "field_mapping": {k: vars(v) for k, v in self.field_mapping.items()}
        }


class ConfigManager:
    """同步配置管理器"""

    def __init__(self, rules_path: str = "config/sync_rules.json",
                 db_config_path: str = "config/db_config.ini"):
        self.rules_path = rules_path
        self.db_config_path = db_config_path
        self._raw_config: Dict[str, Any] = {}
        self._sync_rules: Dict[str, SyncRule] = {}
        self._type_mapping_m2i: Dict[str, str] = {}
        self._type_mapping_i2m: Dict[str, str] = {}
        self._whitelist: Dict[str, Any] = {}
        self._blacklist: Dict[str, Any] = {}
        self._schedule_config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self):
        """加载并解析配置文件"""
        if not os.path.exists(self.rules_path):
            raise FileNotFoundError(f"同步规则配置文件不存在: {self.rules_path}")

        try:
            with open(self.rules_path, "r", encoding="utf-8") as f:
                self._raw_config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"解析同步规则配置失败: {e}")

        self._parse_sync_rules()
        self._parse_type_mapping()
        self._parse_list_configs()
        self._parse_schedule()

        logger.info(f"配置加载成功: {len(self._sync_rules)} 条同步规则, "
                    f"白名单={self._whitelist.get('enabled', False)}, "
                    f"黑名单={self._blacklist.get('enabled', False)}")

    def _parse_sync_rules(self):
        """解析同步规则列表"""
        raw_rules = self._raw_config.get("sync_pairs", [])
        for raw in raw_rules:
            try:
                field_mapping = {}
                raw_fm = raw.get("field_mapping", {})
                for fname, fconf in raw_fm.items():
                    fm = FieldMapping(
                        name=fname,
                        type=fconf.get("type", "string"),
                        index=fconf.get("index", False),
                        primary_key=fconf.get("primary_key", False),
                        is_tag=fname in raw.get("tag_fields", [])
                    )
                    field_mapping[fname] = fm

                direction = SyncDirection(raw.get("sync_direction", "bidirectional"))
                rule = SyncRule(
                    rule_id=raw.get("rule_id", ""),
                    rule_name=raw.get("rule_name", ""),
                    mysql_table=raw.get("mysql_table", ""),
                    influxdb_measurement=raw.get("influxdb_measurement", ""),
                    sync_direction=direction,
                    enabled=raw.get("enabled", True),
                    field_mapping=field_mapping,
                    tag_fields=raw.get("tag_fields", []),
                    index_fields=raw.get("index_fields", []),
                    extra_config={k: v for k, v in raw.items()
                                  if k not in ["rule_id", "rule_name", "mysql_table",
                                               "influxdb_measurement", "sync_direction",
                                               "enabled", "field_mapping", "tag_fields",
                                               "index_fields"]}
                )
                self._sync_rules[rule.rule_id] = rule
            except Exception as e:
                logger.warning(f"解析同步规则失败: {raw.get('rule_id')}, 错误: {e}")

    def _parse_type_mapping(self):
        """解析类型映射"""
        tm = self._raw_config.get("type_mapping", {})
        self._type_mapping_m2i = tm.get("mysql_to_influx", {})
        self._type_mapping_i2m = tm.get("influx_to_mysql", {})

    def _parse_list_configs(self):
        """解析黑白名单配置"""
        self._whitelist = self._raw_config.get("whitelist", {"enabled": False})
        self._blacklist = self._raw_config.get("blacklist", {"enabled": False})

    def _parse_schedule(self):
        """解析调度配置"""
        self._schedule_config = self._raw_config.get("sync_schedule", {
            "cron_expression": "0 */5 * * * *",
            "timezone": "Asia/Shanghai"
        })

    def reload(self):
        """重新加载配置"""
        self._sync_rules.clear()
        self._load_config()
        logger.info("同步配置已重新加载")

    def get_all_rules(self) -> List[SyncRule]:
        """获取所有同步规则"""
        return list(self._sync_rules.values())

    def get_enabled_rules(self) -> List[SyncRule]:
        """获取已启用的同步规则"""
        return [r for r in self._sync_rules.values() if r.enabled]

    def get_rule_by_id(self, rule_id: str) -> Optional[SyncRule]:
        """根据规则 ID 获取规则"""
        return self._sync_rules.get(rule_id)

    def get_rule_by_mysql_table(self, table_name: str) -> Optional[SyncRule]:
        """根据 MySQL 表名获取规则"""
        for rule in self._sync_rules.values():
            if rule.mysql_table == table_name and rule.enabled:
                return rule
        return None

    def get_rule_by_measurement(self, measurement: str) -> Optional[SyncRule]:
        """根据 InfluxDB measurement 名获取规则"""
        for rule in self._sync_rules.values():
            if rule.influxdb_measurement == measurement and rule.enabled:
                return rule
        return None

    def is_table_allowed(self, table_name: str) -> bool:
        """判断 MySQL 表是否允许同步"""
        wl_enabled = self._whitelist.get("enabled", False)
        bl_enabled = self._blacklist.get("enabled", False)

        if wl_enabled:
            wl_tables = set(self._whitelist.get("tables", []))
            if wl_tables and table_name not in wl_tables:
                logger.debug(f"表 [{table_name}] 不在白名单中, 跳过")
                return False

        if bl_enabled:
            bl_tables = set(self._blacklist.get("tables", []))
            if table_name in bl_tables:
                logger.debug(f"表 [{table_name}] 在黑名单中, 跳过")
                return False

        return True

    def is_measurement_allowed(self, measurement: str) -> bool:
        """判断 InfluxDB measurement 是否允许同步"""
        wl_enabled = self._whitelist.get("enabled", False)
        bl_enabled = self._blacklist.get("enabled", False)

        if wl_enabled:
            wl_ms = set(self._whitelist.get("measurements", []))
            if wl_ms and measurement not in wl_ms:
                logger.debug(f"measurement [{measurement}] 不在白名单中, 跳过")
                return False

        if bl_enabled:
            bl_ms = set(self._blacklist.get("measurements", []))
            if measurement in bl_ms or measurement.startswith("_"):
                logger.debug(f"measurement [{measurement}] 在黑名单中, 跳过")
                return False

        return True

    def is_field_allowed(self, field_name: str) -> bool:
        """判断字段是否允许同步"""
        bl_fields = set(self._blacklist.get("fields", []))
        if field_name.lower() in bl_fields or field_name in bl_fields:
            logger.debug(f"字段 [{field_name}] 在黑名单中, 跳过")
            return False
        return True

    def filter_allowed_tables(self, tables: List[str]) -> List[str]:
        """过滤允许同步的 MySQL 表列表"""
        result = [t for t in tables if self.is_table_allowed(t)]
        logger.info(f"表过滤: {len(tables)} -> {len(result)}")
        return result

    def filter_allowed_measurements(self, measurements: List[str]) -> List[str]:
        """过滤允许同步的 measurement 列表"""
        result = [m for m in measurements if self.is_measurement_allowed(m)]
        logger.info(f"measurement 过滤: {len(measurements)} -> {len(result)}")
        return result

    def map_mysql_type_to_influx(self, mysql_type: str) -> str:
        """MySQL 类型 -> InfluxDB 类型映射"""
        mysql_upper = mysql_type.upper().split("(")[0].strip()
        for key, value in self._type_mapping_m2i.items():
            if mysql_upper.startswith(key.upper()):
                return value
        return self._type_mapping_m2i.get("DEFAULT", "string")

    def map_influx_type_to_mysql(self, influx_type: str) -> str:
        """InfluxDB 类型 -> MySQL 类型映射"""
        return self._type_mapping_i2m.get(influx_type.lower(),
                                          self._type_mapping_i2m.get("DEFAULT", "TEXT"))

    def get_sync_tables(self) -> Set[str]:
        """获取所有配置了同步规则的 MySQL 表名"""
        return {r.mysql_table for r in self.get_enabled_rules()}

    def get_sync_measurements(self) -> Set[str]:
        """获取所有配置了同步规则的 InfluxDB measurement 名"""
        return {r.influxdb_measurement for r in self.get_enabled_rules()}

    def get_schedule_config(self) -> Dict[str, Any]:
        """获取调度配置"""
        return dict(self._schedule_config)

    def get_cron_expression(self) -> str:
        """获取 cron 表达式"""
        return self._schedule_config.get("cron_expression", "0 */5 * * * *")

    def save_rules(self, rules: Optional[List[SyncRule]] = None):
        """保存规则到配置文件"""
        if rules:
            for r in rules:
                self._sync_rules[r.rule_id] = r

        self._raw_config["sync_pairs"] = [r.to_dict() for r in self._sync_rules.values()]

        tmp_path = self.rules_path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self._raw_config, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.rules_path)
        logger.info(f"同步规则已保存: {len(self._sync_rules)} 条")

    def add_rule(self, rule: SyncRule):
        """新增同步规则"""
        self._sync_rules[rule.rule_id] = rule
        logger.info(f"新增同步规则: {rule.rule_id}")

    def remove_rule(self, rule_id: str) -> bool:
        """删除同步规则"""
        if rule_id in self._sync_rules:
            del self._sync_rules[rule_id]
            logger.info(f"删除同步规则: {rule_id}")
            return True
        return False

    def set_run_mode(self, mode: RunMode):
        """设置全局运行模式（仅内存生效，持久化到 db_config.ini）"""
        import configparser
        parser = configparser.ConfigParser()
        parser.read(self.db_config_path, encoding="utf-8")
        if "sync" not in parser:
            parser["sync"] = {}
        parser["sync"]["mode"] = mode.value
        with open(self.db_config_path, "w", encoding="utf-8") as f:
            parser.write(f)
        logger.info(f"运行模式已设置为: {mode.value}")

    def get_run_mode(self) -> RunMode:
        """获取运行模式"""
        import configparser
        parser = configparser.ConfigParser()
        parser.read(self.db_config_path, encoding="utf-8")
        mode = parser.get("sync", "mode", fallback="preview")
        try:
            return RunMode(mode)
        except ValueError:
            return RunMode.PREVIEW
