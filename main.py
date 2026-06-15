"""
智能制造数据中台 - MySQL 与 InfluxDB 双向同步主入口
功能：整合所有模块，提供 CLI 命令行入口
运行模式：
  python main.py preview    - 预览模式，仅生成同步脚本不执行
  python main.py execute    - 执行模式，实际执行同步变更
  python main.py status     - 查看最近同步状态
  python main.py show <id>  - 查看指定同步会话详情
"""

import sys
import os
import json
import argparse
import logging
from typing import Optional
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sync_core.db_adapters.mysql_adapter import MySQLAdapter
from sync_core.db_adapters.influxdb_adapter import InfluxDBAdapter
from sync_core.config_manager import ConfigManager, RunMode, SyncRule, SyncDirection, FieldMapping
from sync_core.metadata_collector import MetadataCollector, CollectResult
from sync_core.diff_engine import DiffEngine, DiffAnalysisResult
from sync_core.log_rollback import (LogManager, SessionManager, RollbackEngine,
                                    SyncPhase, LogLevel)
from sync_core.sync_executor import (ScriptGenerator, GenerateResult,
                                     SyncExecutor, ExecuteResult)
from sync_core.sync_filter import (SyncFilter, FilterTarget, resolve_filter,
                                   apply_filter_to_diff, apply_filter_to_generate,
                                   build_filter_diagnostic)
from sync_core.sync_capacity_checker import (CapacityChecker, CapacityCheckResult,
                                             CapacityThresholds, CapacityStatus)

logger = logging.getLogger("main")


class SyncOrchestrator:
    """同步流程编排器"""

    def __init__(self, run_mode: Optional[RunMode] = None,
                 sync_filter: Optional[SyncFilter] = None,
                 skip_capacity_check: bool = False,
                 capacity_threshold_pct: Optional[float] = None,
                 db_config_path: str = "config/db_config.ini",
                 rules_path: str = "config/sync_rules.json"):
        self.db_config_path = db_config_path
        self.rules_path = rules_path
        self.sync_filter = sync_filter
        self.skip_capacity_check = skip_capacity_check
        self.capacity_threshold_pct = capacity_threshold_pct

        self.log_mgr = LogManager()
        self.config = ConfigManager(rules_path, db_config_path)

        effective_mode = run_mode or self.config.get_run_mode()
        self.run_mode = effective_mode

        self.session_mgr = SessionManager(run_mode=effective_mode.value)
        self.rollback_engine = RollbackEngine(self.session_mgr)

        self.mysql: Optional[MySQLAdapter] = None
        self.influx: Optional[InfluxDBAdapter] = None
        self.collector: Optional[MetadataCollector] = None
        self.diff_engine = DiffEngine(self.config)
        self.script_gen = ScriptGenerator(self.config)
        self.executor: Optional[SyncExecutor] = None

    def _init_connections(self):
        """初始化双边数据库连接"""
        with self.session_mgr.phase(SyncPhase.CONNECT):
            logger.info("初始化数据库连接适配器...")
            self.mysql = MySQLAdapter(self.db_config_path)
            self.influx = InfluxDBAdapter(self.db_config_path)

            mysql_ok = self.mysql.test_connection()
            influx_ok = self.influx.test_connection()

            self.session_mgr.log(
                phase=SyncPhase.CONNECT,
                level=LogLevel.INFO if (mysql_ok and influx_ok) else LogLevel.ERROR,
                message=f"数据库连接: MySQL={'OK' if mysql_ok else 'FAIL'}, "
                        f"InfluxDB={'OK' if influx_ok else 'FAIL'}"
            )
            if not mysql_ok or not influx_ok:
                raise ConnectionError("数据库连接失败，请检查配置")

            self.collector = MetadataCollector(self.mysql, self.influx, self.config)
            self.executor = SyncExecutor(
                self.mysql, self.influx,
                self.session_mgr, self.rollback_engine,
                run_mode=self.run_mode
            )

    def run_sync(self, export_scripts: bool = True) -> dict:
        """执行完整同步流程（含过滤层）"""
        session = self.session_mgr.start_session()
        success = False
        errors = []
        active_filter = self.sync_filter

        filter_desc = active_filter.describe() if active_filter else "无过滤(全量)"
        self.session_mgr.log(
            phase=SyncPhase.INIT, level=LogLevel.INFO,
            message=f"同步过滤条件: {filter_desc}")

        try:
            self._init_connections()
            capacity_result: Optional[CapacityCheckResult] = None

            with self.session_mgr.phase(SyncPhase.CAPACITY_CHECK):
                capacity_checker = CapacityChecker(
                    self.mysql, self.influx, self.config,
                    metadata_collector=self.collector,
                    sync_filter=self.sync_filter
                )
                thresholds = CapacityThresholds.from_args(self.capacity_threshold_pct)
                capacity_result = capacity_checker.check(
                    thresholds=thresholds,
                    skip_check=self.skip_capacity_check,
                    log_fn=self.session_mgr.log
                )
                if not capacity_result.can_proceed:
                    block_msg = (
                        f"容量校验拦截，原因: {len(capacity_result.blocked_reasons)} 条。 "
                        f"如需跳过请使用 --skip-capacity-check"
                    )
                    self.session_mgr.log(
                        phase=SyncPhase.FAILED, level=LogLevel.CRITICAL,
                        message=block_msg
                    )
                    success = False
                    errors.extend(capacity_result.blocked_reasons)
                    return {
                        "session_id": session.session_id,
                        "status": "blocked_by_capacity",
                        "capacity_check": capacity_result.to_dict(),
                        "message": block_msg,
                        "blocked_reasons": capacity_result.blocked_reasons
                    }

            with self.session_mgr.phase(SyncPhase.COLLECT):
                collect_result: CollectResult = self.collector.collect_all()
                if collect_result.errors:
                    errors.extend(collect_result.errors)

            with self.session_mgr.phase(SyncPhase.DIFF):
                diff_result: DiffAnalysisResult = self.diff_engine.analyze(collect_result)
                self.session_mgr.set_diff_summary(diff_result.summary())
                if diff_result.errors:
                    errors.extend(diff_result.errors)

            with self.session_mgr.phase(SyncPhase.DIFF):
                if active_filter and not active_filter.is_empty:
                    diff_before = diff_result.summary()
                    diff_result = apply_filter_to_diff(diff_result, active_filter)
                    diff_after = diff_result.summary()
                    self.session_mgr.log(
                        phase=SyncPhase.DIFF, level=LogLevel.INFO,
                        message=f"差异过滤: 对数 {diff_before.get('total_pairs')}->{diff_after.get('total_pairs')}, "
                                f"差异数 {diff_before.get('total_diffs')}->{diff_after.get('total_diffs')}")
                    self.session_mgr.set_diff_summary(diff_after)

            with self.session_mgr.phase(SyncPhase.GENERATE_SCRIPT):
                gen_result: GenerateResult = self.script_gen.generate_all(diff_result)

                if active_filter and not active_filter.is_empty:
                    gen_before = gen_result.summary()
                    gen_result = apply_filter_to_generate(gen_result, active_filter)
                    gen_after = gen_result.summary()
                    self.session_mgr.log(
                        phase=SyncPhase.GENERATE_SCRIPT, level=LogLevel.INFO,
                        message=f"脚本过滤: {gen_before.get('total')}->{gen_after.get('total')}")

                if export_scripts:
                    try:
                        paths = self.script_gen.export_scripts_to_disk(gen_result)
                        self.session_mgr.log(
                            phase=SyncPhase.GENERATE_SCRIPT,
                            level=LogLevel.INFO,
                            message=f"脚本已导出到磁盘: {list(paths.keys())}"
                        )
                    except Exception as e:
                        logger.warning(f"导出脚本失败: {e}")

            if not gen_result.all_scripts and not gen_result.create_table_scripts:
                msg = "未发现需要同步的差异，流程结束"
                diag = None
                if active_filter and not active_filter.is_empty:
                    diag = build_filter_diagnostic(
                        self.diff_engine.analyze(collect_result), active_filter)
                    msg = (f"过滤后无匹配差异。过滤条件: {filter_desc}。"
                           f"过滤前共 {diag.get('total_pairs_before_filter', 0)} 对, "
                           f"过滤后 0 对。")
                    mismatch_details = diag.get("filter_match_details", [])
                    if mismatch_details:
                        msg += f" 排除原因示例: {mismatch_details[0].get('mismatch_reasons', [])[:2]}"

                self.session_mgr.log(
                    phase=SyncPhase.COMPLETE, level=LogLevel.INFO,
                    message=msg)
                success = True
                return {
                    "session_id": session.session_id,
                    "status": "no_diff",
                    "capacity_check": capacity_result.to_dict(),
                    "collect_stats": collect_result.stats,
                    "diff_summary": diff_result.summary(),
                    "filter": filter_desc,
                    "filter_diagnostic": diag,
                    "message": msg
                }

            with self.session_mgr.phase(
                    SyncPhase.PREVIEW if self.run_mode == RunMode.PREVIEW else SyncPhase.EXECUTE):
                exec_result: ExecuteResult = self.executor.execute(gen_result)

            success = exec_result.failed == 0 and not exec_result.rollback_triggered
            return {
                "session_id": session.session_id,
                "status": "success" if success else ("rollback" if exec_result.rollback_triggered else "partial"),
                "run_mode": self.run_mode.value,
                "filter": filter_desc,
                "capacity_check": capacity_result.to_dict(),
                "collect_stats": collect_result.stats,
                "diff_summary": diff_result.summary(),
                "script_summary": gen_result.summary(),
                "execute_result": exec_result.to_dict(),
                "message": f"同步流程结束，成功={exec_result.success}，失败={exec_result.failed}"
            }

        except Exception as e:
            errors.append(f"致命错误: {type(e).__name__}: {e}")
            self.session_mgr.log(
                phase=SyncPhase.FAILED, level=LogLevel.CRITICAL,
                message=f"同步流程致命错误: {e}", exc_info=True
            )
            try:
                if self.executor and self.run_mode == RunMode.EXECUTE:
                    self.rollback_engine.execute_rollback()
            except Exception as rbe:
                errors.append(f"回滚过程也发生错误: {rbe}")
            return {
                "session_id": session.session_id if session else None,
                "status": "failed",
                "run_mode": self.run_mode.value,
                "filter": filter_desc,
                "capacity_check": capacity_result.to_dict() if capacity_result else None,
                "errors": errors,
                "message": f"同步失败: {e}"
            }
        finally:
            try:
                self.session_mgr.end_session(success=success, errors=errors)
            except Exception:
                pass
            try:
                if self.mysql:
                    self.mysql.close()
            except Exception:
                pass
            try:
                if self.influx:
                    self.influx.close()
            except Exception:
                pass


def cmd_preview(args):
    """预览模式命令"""
    logger.info("=" * 60)
    logger.info("运行模式: PREVIEW (仅预览，不实际执行变更)")
    logger.info("=" * 60)
    sf = resolve_filter(args)
    orch = SyncOrchestrator(
        run_mode=RunMode.PREVIEW,
        sync_filter=sf,
        skip_capacity_check=getattr(args, 'skip_capacity_check', False),
        capacity_threshold_pct=getattr(args, 'capacity_threshold_pct', None)
    )
    result = orch.run_sync(export_scripts=True)
    print_result(result)
    return 0 if result["status"] not in ("failed", "blocked_by_capacity") else 1


def cmd_execute(args):
    """执行模式命令"""
    logger.info("=" * 60)
    logger.info("运行模式: EXECUTE (实际执行同步变更，失败将自动回滚)")
    logger.info("=" * 60)
    if not args.yes and not args.y:
        confirm = input("确认要执行同步变更吗？此操作会修改数据库结构！(yes/no): ").strip().lower()
        if confirm not in ("y", "yes"):
            logger.info("用户取消操作")
            return 0
    sf = resolve_filter(args)
    orch = SyncOrchestrator(
        run_mode=RunMode.EXECUTE,
        sync_filter=sf,
        skip_capacity_check=getattr(args, 'skip_capacity_check', False),
        capacity_threshold_pct=getattr(args, 'capacity_threshold_pct', None)
    )
    result = orch.run_sync(export_scripts=True)
    print_result(result)
    return 0 if result["status"] in ("success", "no_diff") else 1


def cmd_status(args):
    """查看最近同步状态"""
    sm = SessionManager()
    sessions = sm.get_recent_sessions(args.limit)
    print(f"\n最近 {len(sessions)} 次同步会话记录：")
    print("-" * 90)
    print(f"{'会话ID':<38} {'开始时间':<20} {'模式':<8} {'状态':<10} {'成功/总数':<12}")
    print("-" * 90)
    for s in sessions:
        sid = s.get("session_id", "")
        started = (s.get("started_at", "") or "")[:19].replace("T", " ")
        mode = s.get("run_mode", "")
        status = s.get("status", "")
        succ = s.get("success_operations", 0)
        total = s.get("total_operations", 0)
        print(f"{sid:<38} {started:<20} {mode:<8} {status:<10} {succ}/{total:<12}")
    print("-" * 90)
    if not sessions:
        print("暂无同步会话记录。")
    return 0


def cmd_show(args):
    """查看指定会话详情"""
    sm = SessionManager()
    session = sm.get_session(args.session_id)
    if not session:
        print(f"未找到会话: {args.session_id}")
        return 1
    print(f"\n========== 同步会话详情: {session.get('session_id')} ==========")
    print(f"开始时间:    {session.get('started_at')}")
    print(f"结束时间:    {session.get('ended_at')}")
    print(f"运行模式:    {session.get('run_mode')}")
    print(f"状态:        {session.get('status')}")
    print(f"总操作数:    {session.get('total_operations')}")
    print(f"成功:        {session.get('success_operations')}")
    print(f"失败:        {session.get('failed_operations')}")
    print(f"错误数:      {len(session.get('errors', []))}")
    print()
    print("差异摘要:")
    print(json.dumps(session.get('diff_summary', {}), ensure_ascii=False, indent=2))
    if session.get("errors"):
        print("\n错误列表:")
        for e in session["errors"]:
            print(f"  - {e}")
    print("\n回滚步骤:")
    for step in session.get("rollback_steps", [])[:20]:
        mark = "✓" if step["execute_success"] else ("✗" if step["executed"] else "○")
        print(f"  [{mark}] #{step['order']} [{step['target_db']}] {step['original_operation'][:60]}")
    if args.full:
        print("\n完整日志(最近100条):")
        for log in session.get("logs_sample", [])[-100:]:
            ts = log["timestamp"][11:19]
            print(f"  [{ts}] [{log['level']}] [{log['phase']}] {log['message'][:120]}")
    return 0


def cmd_set_mode(args):
    """设置默认运行模式"""
    cm = ConfigManager()
    mode = RunMode(args.mode)
    cm.set_run_mode(mode)
    print(f"默认运行模式已设置为: {mode.value}")
    print(f"  - preview: 仅生成脚本，不执行（安全）")
    print(f"  - execute: 生成并实际执行同步变更")
    return 0


def cmd_test_conn(args):
    """测试数据库连接"""
    LogManager()
    try:
        mysql = MySQLAdapter()
        ok1 = mysql.test_connection()
        print(f"MySQL 连接:  {'✅ 成功' if ok1 else '❌ 失败'}")
        mysql.close()
    except Exception as e:
        print(f"MySQL 连接:  ❌ 异常 - {e}")
        ok1 = False
    try:
        influx = InfluxDBAdapter()
        ok2 = influx.test_connection()
        print(f"InfluxDB 连接: {'✅ 成功' if ok2 else '❌ 失败'}")
        influx.close()
    except Exception as e:
        print(f"InfluxDB 连接: ❌ 异常 - {e}")
        ok2 = False
    return 0 if (ok1 and ok2) else 1


def cmd_list_rules(args):
    """列出当前同步规则"""
    cm = ConfigManager()
    rules = cm.get_all_rules()
    print(f"\n共 {len(rules)} 条同步规则：")
    print("-" * 110)
    header = f"{'规则ID':<16} {'MySQL表':<22} {'InfluxDB测量':<22} {'方向':<18} {'状态':<6} {'标签数':<6}"
    print(header)
    print("-" * 110)
    for r in rules:
        direction_map = {
            SyncDirection.MYSQL_TO_INFLUX: "MySQL→InfluxDB",
            SyncDirection.INFLUX_TO_MYSQL: "InfluxDB→MySQL",
            SyncDirection.BIDIRECTIONAL: "双向"
        }
        status = "✅启用" if r.enabled else "❌停用"
        print(f"{r.rule_id:<16} {r.mysql_table:<22} {r.influxdb_measurement:<22} "
              f"{direction_map.get(r.sync_direction, r.sync_direction):<18} "
              f"{status:<6} {len(r.tag_fields):<6}")
    print("-" * 110)

    wl = cm._whitelist
    bl = cm._blacklist
    print(f"\n白名单: {'启用' if wl.get('enabled') else '禁用'}  黑名单: {'启用' if bl.get('enabled') else '禁用'}")
    if bl.get("enabled"):
        print(f"  黑名单表: {bl.get('tables', [])}")
        print(f"  黑名单测量: {bl.get('measurements', [])}")
        print(f"  黑名单字段: {bl.get('fields', [])}")
    return 0


def print_result(result: dict):
    """打印执行结果摘要"""
    print("\n" + "=" * 70)
    print("  同步流程执行报告")
    print("=" * 70)
    print(f"  会话ID:     {result.get('session_id')}")
    print(f"  最终状态:   {result.get('status')}")
    print(f"  运行模式:   {result.get('run_mode', 'N/A')}")
    if result.get("filter"):
        print(f"  过滤条件:   {result['filter']}")
    if result.get("message"):
        print(f"  说明:       {result['message']}")

    if result.get("capacity_check"):
        print("\n  --- 容量校验 ---")
        cc = result["capacity_check"]
        status_map = {
            "safe": "✅ 安全",
            "warning": "⚠️  警告",
            "blocked": "🚫 拦截",
            "skipped": "⏭  跳过",
            "error": "❌ 错误"
        }
        status = status_map.get(cc.get("status"), cc.get("status"))
        print(f"    校验状态:   {status}")
        print(f"    可执行:     {'是' if cc.get('can_proceed') else '否'}")
        print(f"    校验耗时:   {cc.get('check_duration_ms', 0)} ms")
        du = cc.get("disk_usage", {})
        print(f"    磁盘使用:   {du.get('estimated_usage_gb', 'N/A')} GB / "
              f"{du.get('quota_gb', '∞')} GB "
              f"({du.get('usage_percent', 'N/A')}%)")
        th = cc.get("thresholds", {})
        print(f"    软阈值:     {th.get('soft_warning_pct')}%")
        print(f"    硬阈值:     {th.get('hard_block_pct')}%")
        if cc.get("total_estimated_increment_gb", 0) > 0:
            print(f"    同步增量:   {cc.get('total_estimated_increment_gb'):.6f} GB")
            print(f"    同步后预估: {cc.get('usage_percent_after_sync'):.2f}%")
        if cc.get("warnings"):
            print(f"    警告 ({len(cc['warnings'])} 条):")
            for w in cc["warnings"][:5]:
                print(f"      ⚠️  {w[:120]}")
        if cc.get("blocked_reasons"):
            print(f"    拦截原因 ({len(cc['blocked_reasons'])} 条):")
            for r in cc["blocked_reasons"][:10]:
                print(f"      🚫 {r[:120]}")
        if cc.get("table_stats"):
            print(f"    明细 ({len(cc['table_stats'])} 个同步对规模):")
            for ts in cc["table_stats"][:8]:
                direction_map = {
                    "mysql_to_influx": "→",
                    "influx_to_mysql": "←",
                    "bidirectional": "⇄"
                }
                d = direction_map.get(ts.get("sync_direction"), "?")
                src = ts.get("source_type", "")
                name = ts.get("name", "")
                paired = ts.get("paired_name", "")
                rows = ts.get("row_count", 0)
                size_gb = ts.get("size_gb", 0)
                inc_gb = ts.get("estimated_increment_gb", 0)
                print(f"      · [{src}] {name} {d} {paired}: "
                      f"{rows:,} 行, "
                      f"{size_gb:.6f} GB, "
                      f"增量 {inc_gb:.6f} GB")

    if result.get("collect_stats"):
        print("\n  --- 元数据采集 ---")
        for k, v in result["collect_stats"].items():
            print(f"    {k}: {v}")

    if result.get("diff_summary"):
        print("\n  --- 差异分析 ---")
        ds = result["diff_summary"]
        print(f"    总同步对:       {ds.get('total_pairs')}")
        print(f"    有差异的对数:   {ds.get('pairs_with_diff')}")
        print(f"    总差异数:       {ds.get('total_diffs')}")
        print(f"    新建目标数:     {ds.get('unpaired_targets')}")
        for ps in ds.get("pair_summaries", []):
            if ps.get("has_diff"):
                print(f"      · [{ps['mysql_table']}⇄{ps['influx_measurement']}]: "
                      f"{ps.get('field_diffs', 0)} 字段差, "
                      f"{ps.get('index_diffs', 0)} 索引差")

    if result.get("script_summary"):
        print("\n  --- 脚本生成 ---")
        ss = result["script_summary"]
        print(f"    总计:           {ss.get('total')}")
        print(f"    MySQL侧脚本:    {ss.get('mysql_count')}")
        print(f"    InfluxDB侧脚本: {ss.get('influxdb_count')}")
        bd = ss.get("breakdown", {})
        if bd:
            print("    细目:")
            for k, v in bd.items():
                print(f"      {k}: {v}")

    if result.get("execute_result"):
        print("\n  --- 执行结果 ---")
        er = result["execute_result"]
        mark = "✅" if er.get("rollback_triggered") is False and er.get("failed") == 0 else "⚠️"
        print(f"    {mark} 成功:   {er.get('success')}")
        print(f"    ❌ 失败:   {er.get('failed')}")
        print(f"    ⏭  跳过:  {er.get('skipped')}")
        if er.get("rollback_triggered"):
            print(f"    ↩ 自动回滚: 已触发")
            rr = er.get("rollback_report", {})
            print(f"        回滚步骤成功: {rr.get('success_steps', 'N/A')}")
            print(f"        回滚步骤失败: {rr.get('failed_steps', 'N/A')}")
        for fi in er.get("failed_items", [])[:10]:
            print(f"      · 失败项 [{fi.get('target_db')}] {fi.get('operation')[:60]}: {fi.get('error')[:80]}")

    if result.get("errors"):
        print("\n  --- 错误列表 ---")
        for e in result["errors"]:
            print(f"    ❌ {e}")

    if result.get("filter_diagnostic"):
        print("\n  --- 过滤诊断（为何无匹配结果）---")
        diag = result["filter_diagnostic"]
        print(f"    过滤前同步对数: {diag.get('total_pairs_before_filter', 0)}")
        print(f"    过滤前未配对数: {diag.get('total_unpaired_before_filter', 0)}")
        for detail in diag.get("filter_match_details", [])[:5]:
            print(f"    · [{detail.get('mysql_table')} ⇄ {detail.get('influx_measurement')}]"
                  f" 规则={detail.get('rule_id')}")
            for reason in detail.get("mismatch_reasons", [])[:3]:
                print(f"      ✗ {reason}")

    print("=" * 70)


def _add_filter_args(parser: argparse.ArgumentParser):
    """为子命令添加统一的过滤参数组"""
    fg = parser.add_argument_group("过滤参数", "多条件组合为 AND 关系，同条件多值为 OR 关系")
    fg.add_argument(
        "--rule", "-r", action="append", default=[], metavar="RULE_ID",
        help="按规则ID过滤（可重复，逗号分隔），优先级最高")
    fg.add_argument(
        "--table", "-t", action="append", default=[], metavar="TABLE",
        help="按MySQL表名过滤（可重复，逗号分隔）")
    fg.add_argument(
        "--measurement", "-m", action="append", default=[], metavar="MEASUREMENT",
        help="按InfluxDB measurement名过滤（可重复，逗号分隔）")
    fg.add_argument(
        "--direction", "-d", choices=["mysql_to_influx", "influx_to_mysql", "bidirectional"],
        help="按同步方向过滤")
    fg.add_argument(
        "--target", choices=["mysql", "influxdb", "both"], default="both",
        help="按脚本目标库过滤: mysql仅MySQL侧, influxdb仅InfluxDB侧 (默认both)")
    fg.add_argument(
        "--diff-type", action="append", default=[], metavar="DIFF_TYPE",
        help="按差异类型过滤（可重复，逗号分隔）。"
             "可选: field_add,field_drop,field_type_change,field_nullable_change,"
             "tag_add,tag_drop,index_add,index_drop,index_column_change,primary_key_change")

    cg = parser.add_argument_group("容量校验参数", "同步前置时序数据容量校验，防止磁盘溢出")
    cg.add_argument(
        "--skip-capacity-check", action="store_true", default=False,
        help="跳过容量校验（慎用！可能导致磁盘溢出）")
    cg.add_argument(
        "--capacity-threshold-pct", type=float, default=None, metavar="PCT",
        help="自定义磁盘使用率硬拦截阈值(百分比，默认90)。"
             "例如 --capacity-threshold-pct 85 表示使用率超过85%时拦截")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="sync",
        description="智能制造数据中台 - MySQL 与 InfluxDB 双向同步系统",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python main.py preview                                  预览全量同步变更
  python main.py preview --rule RULE_DEVICE_001           仅预览指定规则
  python main.py preview --table device_info,device_metrics  按MySQL表名过滤
  python main.py preview --direction bidirectional        仅预览双向同步
  python main.py preview --target mysql --diff-type field_add  MySQL侧新增字段
  python main.py preview -r R1 -r R2 --target influxdb   多规则+目标库组合过滤
  python main.py execute -y                               执行全量同步（跳过确认）
  python main.py execute --rule RULE_METRICS_002 -y       仅执行指定规则
  python main.py status                                   查看最近同步状态
  python main.py show <session_id>                        查看指定同步会话详情
  python main.py list-rules                               列出同步规则
  python main.py test-conn                                测试数据库连接
  python main.py set-mode preview                         设置默认运行模式为预览

过滤参数优先级（从高到低）:
  --rule > --table/--measurement > --direction > --target > --diff-type
  多条件之间 AND 组合，同条件内多值 OR 组合
        """
    )
    sub = p.add_subparsers(dest="command", required=True)

    p_prev = sub.add_parser("preview", help="预览同步变更（生成脚本不执行）")
    _add_filter_args(p_prev)
    p_prev.set_defaults(func=cmd_preview)

    p_exec = sub.add_parser("execute", help="执行同步变更（失败自动回滚）")
    p_exec.add_argument("-y", "--yes", action="store_true", help="跳过交互式确认")
    _add_filter_args(p_exec)
    p_exec.set_defaults(func=cmd_execute)

    p_stat = sub.add_parser("status", help="查看最近同步状态")
    p_stat.add_argument("-n", "--limit", type=int, default=20, help="显示最近N条（默认20）")
    p_stat.set_defaults(func=cmd_status)

    p_show = sub.add_parser("show", help="查看指定同步会话详情")
    p_show.add_argument("session_id", help="会话ID")
    p_show.add_argument("-f", "--full", action="store_true", help="显示完整日志")
    p_show.set_defaults(func=cmd_show)

    p_mode = sub.add_parser("set-mode", help="设置默认运行模式")
    p_mode.add_argument("mode", choices=["preview", "execute"], help="运行模式")
    p_mode.set_defaults(func=cmd_set_mode)

    p_test = sub.add_parser("test-conn", help="测试数据库连接")
    p_test.set_defaults(func=cmd_test_conn)

    p_list = sub.add_parser("list-rules", help="列出同步规则")
    p_list.set_defaults(func=cmd_list_rules)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    try:
        rc = args.func(args)
        sys.exit(rc if isinstance(rc, int) else 0)
    except KeyboardInterrupt:
        logger.warning("用户中断操作")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"程序异常退出: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
