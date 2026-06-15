"""
InfluxDB 时序数据库连接适配器
负责与时序 InfluxDB 建立连接、查询测量、标签、字段元数据
支持 Flux 查询、InfluxQL 查询、写入数据点
"""

import configparser
from influxdb_client import InfluxDBClient, Point, WriteOptions, BucketsApi, QueryApi
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException
from typing import Dict, List, Optional, Any
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from contextlib import contextmanager
import json

logger = logging.getLogger(__name__)


class InfluxDBAdapter:
    """InfluxDB 时序数据库适配器"""

    def __init__(self, config_path: str = "config/db_config.ini"):
        self.config_path = config_path
        self.config = self._load_config()
        self._client: Optional[InfluxDBClient] = None
        self._write_api = None
        self._query_api: Optional[QueryApi] = None
        self._buckets_api: Optional[BucketsApi] = None

    def _load_config(self) -> Dict[str, Any]:
        """加载数据库配置"""
        parser = configparser.ConfigParser()
        parser.read(self.config_path, encoding="utf-8")
        if "influxdb" not in parser:
            raise ValueError(f"配置文件 {self.config_path} 中缺少 [influxdb] 节")
        return dict(parser["influxdb"])

    @property
    def client(self) -> InfluxDBClient:
        """创建/获取 InfluxDB 客户端"""
        if self._client is None:
            url = self.config.get("url", "http://127.0.0.1:8086")
            token = self.config.get("token", "")
            org = self.config.get("org", "")
            timeout = int(self.config.get("timeout", 30000))
            verify_ssl = self.config.get("verify_ssl", "false").lower() == "true"
            connection_pool_size = int(self.config.get("connection_pool_size", 10))

            self._client = InfluxDBClient(
                url=url,
                token=token,
                org=org,
                timeout=timeout,
                verify_ssl=verify_ssl,
                connection_pool_maxsize=connection_pool_size
            )
            logger.info(f"InfluxDB 客户端初始化成功: {url}, org={org}")
        return self._client

    @property
    def query_api(self) -> QueryApi:
        """获取查询 API"""
        if self._query_api is None:
            self._query_api = self.client.query_api()
        return self._query_api

    @property
    def buckets_api(self) -> BucketsApi:
        """获取存储桶 API"""
        if self._buckets_api is None:
            self._buckets_api = self.client.buckets_api()
        return self._buckets_api

    @property
    def write_api(self):
        """获取写入 API"""
        if self._write_api is None:
            self._write_api = self.client.write_api(
                write_options=WriteOptions(**SYNCHRONOUS)
            )
        return self._write_api

    @contextmanager
    def get_client(self):
        """获取客户端上下文"""
        try:
            yield self.client
        except Exception as e:
            logger.error(f"InfluxDB 操作异常: {e}", exc_info=True)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((ApiException, ConnectionError))
    )
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            health = self.client.health()
            logger.info(f"InfluxDB 连接测试成功: {health.status}, version={health.version}")
            return health.status == "pass"
        except Exception as e:
            logger.error(f"InfluxDB 连接测试失败: {e}", exc_info=True)
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_measurements(self, bucket: Optional[str] = None) -> List[str]:
        """获取指定 bucket 中的所有 measurement"""
        bucket = bucket or self.config.get("bucket", "")
        org = self.config.get("org", "")

        flux_query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{bucket}")
        '''
        try:
            result = self.query_api.query(flux_query, org=org)
            measurements = []
            for table in result:
                for record in table.records:
                    measurements.append(record.values.get("value", ""))
            measurements = sorted(list(set(filter(None, measurements))))
            logger.info(f"获取到 InfluxDB measurement 数量: {len(measurements)}, bucket={bucket}")
            return measurements
        except Exception as e:
            logger.error(f"获取 measurements 失败: {e}", exc_info=True)
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_measurement_schema(self, measurement: str, bucket: Optional[str] = None) -> Dict[str, Any]:
        """获取指定 measurement 的完整元数据（字段、标签、索引）"""
        bucket = bucket or self.config.get("bucket", "")
        org = self.config.get("org", "")

        tags_query = f'''
        import "influxdata/influxdb/schema"
        schema.tagKeys(bucket: "{bucket}", predicate: (r) => r._measurement == "{measurement}")
        '''
        fields_query = f'''
        import "influxdata/influxdb/schema"
        schema.fieldKeys(bucket: "{bucket}", predicate: (r) => r._measurement == "{measurement}")
        '''
        field_types_query = f'''
        import "influxdata/influxdb/schema"
        schema.fieldTypes(bucket: "{bucket}", predicate: (r) => r._measurement == "{measurement}")
        '''

        try:
            tags = self._extract_values(self.query_api.query(tags_query, org=org))
            tags = [t for t in tags if t not in ["_start", "_stop", "_time", "_measurement",
                                                  "_field", "_value", "_result", "table"]]
        except Exception as e:
            logger.warning(f"获取标签失败 [{measurement}]: {e}")
            tags = []

        try:
            fields = self._extract_values(self.query_api.query(fields_query, org=org))
        except Exception as e:
            logger.warning(f"获取字段失败 [{measurement}]: {e}")
            fields = []

        field_types = {}
        try:
            type_result = self.query_api.query(field_types_query, org=org)
            for table in type_result:
                for record in table.records:
                    field_name = record.values.get("fieldKey", "")
                    ftype = record.values.get("fieldType", "string")
                    if field_name:
                        field_types[field_name] = self._normalize_field_type(ftype)
        except Exception as e:
            logger.warning(f"获取字段类型失败 [{measurement}]: {e}")

        tag_values = {}
        for tag in tags[:20]:
            try:
                tag_values_query = f'''
                import "influxdata/influxdb/schema"
                schema.tagValues(bucket: "{bucket}", tag: "{tag}", predicate: (r) => r._measurement == "{measurement}")
                '''
                tag_values[tag] = self._extract_values(self.query_api.query(tag_values_query, org=org))[:100]
            except Exception:
                tag_values[tag] = []

        fields_info = {}
        for f in fields:
            fields_info[f] = {
                "name": f,
                "type": field_types.get(f, "string"),
                "is_tag": False,
                "cardinality": len(tag_values.get(f, []))
            }

        tags_info = {}
        for t in tags:
            tags_info[t] = {
                "name": t,
                "type": "string",
                "is_tag": True,
                "cardinality": len(tag_values.get(t, [])),
                "sample_values": tag_values.get(t, [])[:10]
            }

        schema = {
            "measurement": measurement,
            "bucket": bucket,
            "tags": tags_info,
            "fields": fields_info,
            "tag_names": sorted(tags),
            "field_names": sorted(fields),
            "primary_tag": "device_id" if "device_id" in tags else (tags[0] if tags else None)
        }
        logger.info(f"获取 measurement [{measurement}] 元数据: {len(tags)} 标签, {len(fields)} 字段")
        return schema

    def _extract_values(self, result) -> List[str]:
        """从 Flux 查询结果中提取 value 列"""
        values = []
        for table in result:
            for record in table.records:
                val = record.values.get("value", "")
                if val and val not in values:
                    values.append(val)
        return sorted(values)

    def _normalize_field_type(self, ftype: str) -> str:
        """规范化 InfluxDB 字段类型"""
        type_map = {
            "boolean": "boolean",
            "integer": "int",
            "long": "int",
            "unsignedLong": "int",
            "float": "float",
            "double": "float",
            "string": "string",
            "duration": "string",
            "time": "datetime",
            "bytes": "string"
        }
        return type_map.get(ftype.lower(), "string")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_all_measurement_schemas(self, bucket: Optional[str] = None,
                                       measurements: Optional[List[str]] = None) -> Dict[str, Dict]:
        """批量获取多个 measurement 的元数据"""
        if measurements is None:
            measurements = self.fetch_measurements(bucket)
        result = {}
        for m in measurements:
            try:
                result[m] = self.fetch_measurement_schema(m, bucket)
            except Exception as e:
                logger.warning(f"获取 measurement [{m}] 元数据失败: {e}")
        return result

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def execute_flux(self, flux_query: str, org: Optional[str] = None) -> List[Dict[str, Any]]:
        """执行 Flux 查询语句"""
        org = org or self.config.get("org", "")
        try:
            result = self.query_api.query(flux_query, org=org)
            records = []
            for table in result:
                for record in table.records:
                    records.append(dict(record.values))
            logger.debug(f"执行 Flux 查询成功: {flux_query[:100]}..., 记录数: {len(records)}")
            return records
        except Exception as e:
            logger.error(f"执行 Flux 查询失败: {flux_query[:100]}..., 错误: {e}", exc_info=True)
            raise

    def execute_flux_raw(self, flux_query: str, org: Optional[str] = None) -> str:
        """执行 Flux 查询并返回原始 CSV 字符串"""
        org = org or self.config.get("org", "")
        try:
            raw = self.query_api.query_raw(flux_query, org=org)
            logger.debug(f"执行 Flux 查询(原始)成功: {flux_query[:100]}...")
            return raw
        except Exception as e:
            logger.error(f"执行 Flux 查询(原始)失败: {flux_query[:100]}..., 错误: {e}", exc_info=True)
            raise

    def write_points(self, points: List[Point], bucket: Optional[str] = None,
                     org: Optional[str] = None) -> bool:
        """写入数据点"""
        bucket = bucket or self.config.get("bucket", "")
        org = org or self.config.get("org", "")
        try:
            self.write_api.write(bucket=bucket, org=org, record=points)
            logger.info(f"写入 InfluxDB 数据点: {len(points)} 条, bucket={bucket}")
            return True
        except Exception as e:
            logger.error(f"写入 InfluxDB 失败: {e}", exc_info=True)
            raise

    def create_bucket(self, bucket_name: str, retention_days: int = 30,
                      org: Optional[str] = None, description: str = "") -> bool:
        """创建存储桶"""
        org = org or self.config.get("org", "")
        try:
            orgs = self.client.organizations_api().find_organizations(org=org)
            if not orgs:
                logger.error(f"找不到组织: {org}")
                return False
            org_id = orgs[0].id

            retention_rules = [{
                "type": "expire",
                "everySeconds": retention_days * 24 * 3600
            }]
            self.buckets_api.create_bucket(
                bucket_name=bucket_name,
                org_id=org_id,
                retention_rules=retention_rules,
                description=description
            )
            logger.info(f"创建 InfluxDB bucket 成功: {bucket_name}, 保留{retention_days}天")
            return True
        except Exception as e:
            logger.error(f"创建 bucket 失败 [{bucket_name}]: {e}", exc_info=True)
            raise

    def delete_measurement(self, measurement: str, bucket: Optional[str] = None,
                           org: Optional[str] = None) -> bool:
        """删除指定 measurement 的所有数据"""
        bucket = bucket or self.config.get("bucket", "")
        org = org or self.config.get("org", "")
        try:
            delete_api = self.client.delete_api()
            start = "1970-01-01T00:00:00Z"
            stop = "2100-01-01T00:00:00Z"
            predicate = f'_measurement="{measurement}"'
            delete_api.delete(start, stop, predicate, bucket=bucket, org=org)
            logger.info(f"删除 measurement [{measurement}] 数据成功")
            return True
        except Exception as e:
            logger.error(f"删除 measurement 失败 [{measurement}]: {e}", exc_info=True)
            raise

    def close(self):
        """关闭客户端"""
        if self._write_api:
            self._write_api.close()
            self._write_api = None
        if self._client:
            self._client.close()
            self._client = None
            logger.info("InfluxDB 客户端已关闭")
