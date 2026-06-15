"""
全流程日志与回滚模块
负责：
1. 采集、对比、执行全流程日志记录与持久化
2. 生成每个同步变更的反向回滚脚本（SQL / Flux）
3. 执行失败时自动触发全量回滚
4. 提供历史同步记录查询接口
"""

import os
import json
import uuid
import logging
import logging.handlers
from logging import Logger
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SyncPhase(str, Enum):
    """同步阶段枚举"""
    INIT = "init"
    CONNECT = "connect"
    COLLECT = "collect"
    DIFF = "diff"
    GENERATE_SCRIPT = "generate_script"
    PREVIEW = "preview"
    EXECUTE = "execute"
    EXECUTE_MYSQL = "execute_mysql"
    EXECUTE_INFLUXDB = "execute_influxdb"
    ROLLBACK = "rollback"
    COMPLETE = "complete"
    FAILED = "failed"


class LogLevel(str, Enum):
    """日志级别"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class RollbackStep:
    """单个回滚步骤"""
    step_id: str
    order: int
    target_db: str  # "mysql" or "influxdb"
    original_operation: str  # 原始操作描述
    rollback_script: str  # 回滚脚本内容（SQL 或 Flux）
    script_type: str  # "sql" or "flux"
    executed: bool = False
    execute_success: bool = False
    error_message: str = ""
    executed_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        d = {
            "step_id": self.step_id,
            "order": self.order,
            "target_db": self.target_db,
            "original_operation": self.original_operation,
            "rollback_script": self.rollback_script,
            "script_type": self.script_type,
            "executed": self.executed,
            "execute_success": self.execute_success,
            "error_message": self.error_message
        }
        if self.executed_at:
            d["executed_at"] = self.executed_at.isoformat()
        return d


@dataclass
class SyncLogEntry:
    """单条日志记录"""
    timestamp: datetime
    phase: SyncPhase
    level: LogLevel
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    rule_id: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "phase": self.phase.value,
            "level": self.level.value,
            "message": self.message,
            "details": self.details,
            "rule_id": self.rule_id
        }


@dataclass
class SyncSession:
    """一次完整的同步会话记录"""
    session_id: str
    started_at: datetime
    run_mode: str
    ended_at: Optional[datetime] = None
    status: str = "running"
    total_operations: int = 0
    success_operations: int = 0
    failed_operations: int = 0
    rollback_steps: List[RollbackStep] = field(default_factory=list)
    logs: List[SyncLogEntry] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    diff_summary: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "session_id": self.session_id,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "status": self.status,
            "run_mode": self.run_mode,
            "total_operations": self.total_operations,
            "success_operations": self.success_operations,
            "failed_operations": self.failed_operations,
            "rollback_steps": [s.to_dict() for s in self.rollback_steps],
            "log_count": len(self.logs),
            "logs_sample": [l.to_dict() for l in self.logs[-50:]],
            "errors": self.errors,
            "diff_summary": self.diff_summary
        }


class LogManager:
    """日志管理器 - 配置文件与控制台日志"""

    LOG_DIR = "logs"

    def __init__(self, log_dir: Optional[str] = None):
        self.log_dir = log_dir or self.LOG_DIR
        self._ensure_log_dir()
        self._setup_root_logger()

    def _ensure_log_dir(self):
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir, exist_ok=True)

    def _setup_root_logger(self):
        """配置全局 logging"""
        try:
            import colorlog
            has_colorlog = True
        except ImportError:
            has_colorlog = False

        root = logging.getLogger()
        root.setLevel(logging.DEBUG)
        for h in list(root.handlers):
            root.removeHandler(h)

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        if has_colorlog:
            fmt = colorlog.ColoredFormatter(
                "%(log_color)s[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "green",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red"
                }
            )
        else:
            fmt = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
        console.setFormatter(fmt)
        root.addHandler(console)

        today = datetime.now().strftime("%Y%m%d")
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=os.path.join(self.log_dir, f"sync_{today}.log"),
            when="midnight",
            backupCount=30,
            encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_fmt = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s:%(lineno)d] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_fmt)
        root.addHandler(file_handler)

        error_handler = logging.handlers.RotatingFileHandler(
            filename=os.path.join(self.log_dir, "error.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8"
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_fmt)
        root.addHandler(error_handler)

    def get_logger(self, name: str) -> Logger:
        return logging.getLogger(name)


class SessionManager:
    """同步会话与回滚管理器"""

    SESSIONS_DIR = os.path.join("logs", "sessions")

    def __init__(self, run_mode: str = "preview"):
        self.run_mode = run_mode
        self._current_session: Optional[SyncSession] = None
        self._ensure_dirs()

    def _ensure_dirs(self):
        if not os.path.exists(self.SESSIONS_DIR):
            os.makedirs(self.SESSIONS_DIR, exist_ok=True)

    def start_session(self) -> SyncSession:
        """开始一个新的同步会话"""
        sid = f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        session = SyncSession(
            session_id=sid,
            started_at=datetime.now(),
            run_mode=self.run_mode,
            status="running"
        )
        self._current_session = session
        self.log(phase=SyncPhase.INIT, level=LogLevel.INFO,
                 message=f"同步会话开始: {sid}, 运行模式={run_mode}")
        logger.info(f"同步会话创建: {sid}")
        return session

    def end_session(self, success: bool, errors: Optional[List[str]] = None):
        """结束同步会话"""
        if not self._current_session:
            return
        self._current_session.ended_at = datetime.now()
        if success:
            self._current_session.status = "success" if self._current_session.failed_operations == 0 else "partial"
        else:
            self._current_session.status = "failed"
        if errors:
            self._current_session.errors.extend(errors)
        self.log(phase=SyncPhase.COMPLETE if success else SyncPhase.FAILED,
                 level=LogLevel.INFO if success else LogLevel.ERROR,
                 message=f"同步会话结束: 状态={self._current_session.status}, "
                         f"成功={self._current_session.success_operations}, "
                         f"失败={self._current_session.failed_operations}")
        self._persist_session()
        logger.info(f"同步会话已结束 [{self._current_session.session_id}]: {self._current_session.status}")

    def log(self, phase: SyncPhase, level: LogLevel, message: str,
            details: Optional[Dict] = None, rule_id: Optional[str] = None):
        """记录一条同步日志到当前会话"""
        if not self._current_session:
            self.start_session()
        entry = SyncLogEntry(
            timestamp=datetime.now(),
            phase=phase,
            level=level,
            message=message,
            details=details or {},
            rule_id=rule_id
        )
        self._current_session.logs.append(entry)
        log_func = {
            LogLevel.DEBUG: logger.debug,
            LogLevel.INFO: logger.info,
            LogLevel.WARNING: logger.warning,
            LogLevel.ERROR: logger.error,
            LogLevel.CRITICAL: logger.critical
        }[level]
        prefix = f"[{phase.value}]"
        if rule_id:
            prefix += f"[{rule_id}]"
        log_func(f"{prefix} {message}")
        if details:
            logger.debug(f"  详情: {json.dumps(details, ensure_ascii=False, default=str)[:500]}")

    def log_operation(self, target: str, operation: str, script: str,
                      rollback_script: str, script_type: str, success: bool,
                      error: str = ""):
        """记录一次执行操作及对应的回滚步骤"""
        if not self._current_session:
            return
        session = self._current_session
        session.total_operations += 1
        order = len(session.rollback_steps) + 1
        step_id = f"rb_{session.session_id}_{order:04d}"
        step = RollbackStep(
            step_id=step_id,
            order=order,
            target_db=target,
            original_operation=operation,
            rollback_script=rollback_script,
            script_type=script_type,
            executed=False
        )
        session.rollback_steps.append(step)

        if success:
            session.success_operations += 1
        else:
            session.failed_operations += 1

    def increment_success(self):
        if self._current_session:
            self._current_session.success_operations += 1
            self._current_session.total_operations += 1

    def increment_failed(self):
        if self._current_session:
            self._current_session.failed_operations += 1
            self._current_session.total_operations += 1

    def add_rollback_step(self, step: RollbackStep):
        if self._current_session:
            self._current_session.rollback_steps.append(step)

    def set_diff_summary(self, summary: Dict):
        if self._current_session:
            self._current_session.diff_summary = summary

    @property
    def current_session(self) -> Optional[SyncSession]:
        return self._current_session

    def get_recent_sessions(self, limit: int = 20) -> List[Dict]:
        """获取最近的同步会话列表"""
        sessions = []
        for fname in sorted(os.listdir(self.SESSIONS_DIR), reverse=True):
            if fname.endswith(".json"):
                try:
                    with open(os.path.join(self.SESSIONS_DIR, fname), "r", encoding="utf-8") as f:
                        sessions.append(json.load(f))
                except Exception:
                    pass
                if len(sessions) >= limit:
                    break
        return sessions

    def get_session(self, session_id: str) -> Optional[Dict]:
        """获取指定会话的详细记录"""
        path = os.path.join(self.SESSIONS_DIR, f"{session_id}.json")
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"读取会话记录失败 {session_id}: {e}")
        return None

    def _persist_session(self):
        """将当前会话持久化到磁盘"""
        if not self._current_session:
            return
        path = os.path.join(self.SESSIONS_DIR, f"{self._current_session.session_id}.json")
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._current_session.to_dict(), f, ensure_ascii=False, indent=2, default=str)
            os.replace(tmp, path)
        except Exception as e:
            logger.error(f"持久化会话记录失败: {e}", exc_info=True)

    @contextmanager
    def phase(self, phase_name: SyncPhase, rule_id: Optional[str] = None):
        """阶段上下文管理器，自动记录开始/结束/异常"""
        self.log(phase=phase_name, level=LogLevel.INFO,
                 message=f"[BEGIN] 进入阶段: {phase_name.value}", rule_id=rule_id)
        try:
            yield
            self.log(phase=phase_name, level=LogLevel.INFO,
                     message=f"[END] 阶段完成: {phase_name.value}", rule_id=rule_id)
        except Exception as e:
            self.log(phase=phase_name, level=LogLevel.ERROR,
                     message=f"[ERROR] 阶段异常: {phase_name.value} - {e}",
                     details={"error_type": type(e).__name__}, rule_id=rule_id)
            raise


class RollbackEngine:
    """回滚执行引擎"""

    def __init__(self, session_manager: SessionManager,
                 mysql_executor: Optional[Callable] = None,
                 influx_executor: Optional[Callable] = None):
        self.sm = session_manager
        self.mysql_executor = mysql_executor
        self.influx_executor = influx_executor

    def register_executors(self, mysql_executor: Callable, influx_executor: Callable):
        self.mysql_executor = mysql_executor
        self.influx_executor = influx_executor

    def execute_rollback(self) -> bool:
        """执行当前会话的全量回滚（按操作逆序）"""
        session = self.sm.current_session
        if not session:
            logger.warning("没有活跃会话，无法回滚")
            return False

        steps = sorted(session.rollback_steps, key=lambda s: -s.order)
        self.sm.log(phase=SyncPhase.ROLLBACK, level=LogLevel.WARNING,
                    message=f"开始回滚，共 {len(steps)} 个步骤")

        all_success = True
        for step in steps:
            if not step.rollback_script.strip():
                step.executed = True
                step.execute_success = True
                continue
            try:
                self.sm.log(phase=SyncPhase.ROLLBACK, level=LogLevel.INFO,
                            message=f"执行回滚步骤 [{step.order}] {step.original_operation[:50]}")
                if step.script_type == "sql" and self.mysql_executor:
                    self.mysql_executor(step.rollback_script)
                elif step.script_type == "flux" and self.influx_executor:
                    self.influx_executor(step.rollback_script)
                else:
                    logger.warning(f"无对应执行器，跳过回滚步骤 [{step.step_id}]: {step.script_type}")
                step.executed = True
                step.execute_success = True
                step.executed_at = datetime.now()
            except Exception as e:
                all_success = False
                step.executed = True
                step.execute_success = False
                step.error_message = str(e)
                step.executed_at = datetime.now()
                self.sm.log(phase=SyncPhase.ROLLBACK, level=LogLevel.ERROR,
                            message=f"回滚步骤失败 [{step.order}]: {e}",
                            details={"script": step.rollback_script[:300]})

        status = "成功" if all_success else "部分失败"
        self.sm.log(phase=SyncPhase.ROLLBACK, level=LogLevel.WARNING,
                    message=f"回滚完成，状态: {status}")
        return all_success

    def generate_rollback_report(self) -> Dict:
        """生成回滚报告"""
        session = self.sm.current_session
        if not session:
            return {}
        steps = [s.to_dict() for s in session.rollback_steps]
        executed = [s for s in steps if s["executed"]]
        success = [s for s in executed if s["execute_success"]]
        failed = [s for s in executed if not s["execute_success"]]
        return {
            "session_id": session.session_id,
            "total_steps": len(steps),
            "executed_steps": len(executed),
            "success_steps": len(success),
            "failed_steps": len(failed),
            "failed_details": [{"step_id": s["step_id"], "order": s["order"],
                                "error": s["error_message"],
                                "rollback_script": s["rollback_script"][:500]}
                               for s in failed]
        }
