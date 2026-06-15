"""
MySQL 数据库连接适配器
负责与业务 MySQL 数据库建立连接、查询元数据、执行 SQL 语句
支持连接池、事务管理、错误重试
"""

import configparser
import pymysql
from pymysql.cursors import DictCursor
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from typing import Dict, List, Optional, Any, Tuple
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class MySQLAdapter:
    """MySQL 数据库适配器"""

    def __init__(self, config_path: str = "config/db_config.ini"):
        self.config_path = config_path
        self.config = self._load_config()
        self._engine: Optional[Engine] = None
        self._connection = None
        self._transaction_stack: List = []

    def _load_config(self) -> Dict[str, Any]:
        """加载数据库配置"""
        parser = configparser.ConfigParser()
        parser.read(self.config_path, encoding="utf-8")
        if "mysql" not in parser:
            raise ValueError(f"配置文件 {self.config_path} 中缺少 [mysql] 节")
        return dict(parser["mysql"])

    @property
    def engine(self) -> Engine:
        """创建/获取 SQLAlchemy 引擎"""
        if self._engine is None:
            host = self.config.get("host", "127.0.0.1")
            port = int(self.config.get("port", 3306))
            user = self.config.get("user", "root")
            password = self.config.get("password", "")
            database = self.config.get("database", "")
            charset = self.config.get("charset", "utf8mb4")
            pool_size = int(self.config.get("pool_size", 10))
            max_overflow = int(self.config.get("max_overflow", 20))
            connect_timeout = int(self.config.get("connect_timeout", 30))

            url = (
                f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
                f"?charset={charset}&connect_timeout={connect_timeout}"
            )
            self._engine = create_engine(
                url,
                pool_size=pool_size,
                max_overflow=max_overflow,
                pool_pre_ping=True,
                pool_recycle=3600,
                future=True
            )
            logger.info(f"MySQL 引擎初始化成功: {host}:{port}/{database}")
        return self._engine

    @contextmanager
    def get_connection(self):
        """获取原生 pymysql 连接上下文"""
        conn = None
        try:
            conn = pymysql.connect(
                host=self.config.get("host", "127.0.0.1"),
                port=int(self.config.get("port", 3306)),
                user=self.config.get("user", "root"),
                password=self.config.get("password", ""),
                database=self.config.get("database", ""),
                charset=self.config.get("charset", "utf8mb4"),
                connect_timeout=int(self.config.get("connect_timeout", 30)),
                cursorclass=DictCursor,
                autocommit=False
            )
            yield conn
        except Exception as e:
            logger.error(f"获取 MySQL 连接失败: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((pymysql.OperationalError, pymysql.InterfaceError))
    )
    def test_connection(self) -> bool:
        """测试数据库连接"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1 AS test")
                    result = cursor.fetchone()
                    logger.info(f"MySQL 连接测试成功: {result}")
                    return True
        except Exception as e:
            logger.error(f"MySQL 连接测试失败: {e}", exc_info=True)
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_tables(self) -> List[str]:
        """获取数据库中所有表名"""
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        logger.info(f"获取到 MySQL 表数量: {len(tables)}")
        return sorted(tables)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_table_schema(self, table_name: str) -> Dict[str, Any]:
        """获取指定表的完整元数据"""
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        primary_keys = inspector.get_pk_constraint(table_name).get("constrained_columns", [])
        indexes = inspector.get_indexes(table_name)
        foreign_keys = inspector.get_foreign_keys(table_name)

        fields = {}
        for col in columns:
            field_info = {
                "name": col["name"],
                "type": str(col["type"]).upper().split("(")[0],
                "full_type": str(col["type"]).upper(),
                "nullable": col.get("nullable", True),
                "default": str(col.get("default", "")) if col.get("default") is not None else None,
                "primary_key": col["name"] in primary_keys,
                "comment": col.get("comment", "")
            }
            fields[col["name"]] = field_info

        index_info = {}
        for idx in indexes:
            idx_name = idx.get("name", "unnamed")
            index_info[idx_name] = {
                "name": idx_name,
                "columns": idx.get("column_names", []),
                "unique": idx.get("unique", False),
                "type": "UNIQUE" if idx.get("unique") else "INDEX"
            }

        schema = {
            "table_name": table_name,
            "fields": fields,
            "primary_keys": primary_keys,
            "indexes": index_info,
            "foreign_keys": foreign_keys,
            "field_names": list(fields.keys())
        }
        logger.info(f"获取表 [{table_name}] 元数据: {len(fields)} 字段, {len(indexes)} 索引")
        return schema

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def fetch_all_schemas(self, tables: Optional[List[str]] = None) -> Dict[str, Dict]:
        """批量获取多个表的元数据"""
        if tables is None:
            tables = self.fetch_tables()
        result = {}
        for table in tables:
            try:
                result[table] = self.fetch_table_schema(table)
            except Exception as e:
                logger.warning(f"获取表 [{table}] 元数据失败: {e}")
        return result

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def execute_query(self, sql: str, params: Optional[Tuple] = None, fetch_all: bool = True) -> List[Dict]:
        """执行查询语句"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params or ())
                if fetch_all:
                    rows = [dict(row._mapping) for row in result.fetchall()]
                else:
                    row = result.fetchone()
                    rows = [dict(row._mapping)] if row else []
                logger.debug(f"执行查询 SQL: {sql[:100]}..., 影响行数: {len(rows)}")
                return rows
        except Exception as e:
            logger.error(f"执行查询失败: {sql[:100]}..., 错误: {e}", exc_info=True)
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def execute_ddl(self, ddl_sql: str) -> bool:
        """执行 DDL 语句（建表、加字段、加索引等）"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text(ddl_sql))
                conn.commit()
                logger.info(f"执行 DDL 成功: {ddl_sql[:150]}...")
                return True
        except Exception as e:
            logger.error(f"执行 DDL 失败: {ddl_sql[:150]}..., 错误: {e}", exc_info=True)
            raise

    def begin_transaction(self):
        """开始事务"""
        if not self._connection:
            self._connection = self.engine.connect()
        self._connection.begin()
        self._transaction_stack.append(True)
        logger.info(f"MySQL 事务已开启, 嵌套层级: {len(self._transaction_stack)}")

    def commit_transaction(self):
        """提交事务"""
        if self._transaction_stack:
            self._transaction_stack.pop()
            if not self._transaction_stack and self._connection:
                self._connection.commit()
                self._connection.close()
                self._connection = None
                logger.info("MySQL 事务已提交")

    def rollback_transaction(self):
        """回滚事务"""
        if self._transaction_stack:
            self._transaction_stack.clear()
            if self._connection:
                try:
                    self._connection.rollback()
                    self._connection.close()
                except Exception as e:
                    logger.warning(f"回滚事务时出现异常: {e}")
                self._connection = None
                logger.info("MySQL 事务已回滚")

    def close(self):
        """关闭所有连接"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("MySQL 连接池已关闭")
