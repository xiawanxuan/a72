"""
定时任务专用入口（crontab / 计划任务 调用）
特点：
  - 非交互式
  - 自动执行（基于 db_config.ini 的 sync.mode）
  - 可通过命令行参数覆盖模式
  - 输出机器可读的 JSON 结果供调度器解析
  - 退出码：0=成功 1=失败 2=部分成功
用法：
  python cron_sync.py
  python cron_sync.py --mode preview
  python cron_sync.py --mode execute --output result.json
"""

import sys
import os
import json
import argparse
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import SyncOrchestrator
from sync_core.config_manager import RunMode
from sync_core.log_rollback import LogManager


def run():
    parser = argparse.ArgumentParser(description="MySQL-InfluxDB 双向同步 - 定时任务入口")
    parser.add_argument("--mode", choices=["preview", "execute", "auto"], default="auto",
                        help="运行模式（auto=读取配置文件中的默认模式）")
    parser.add_argument("--output", "-o", default=None,
                        help="将结果 JSON 输出到指定文件")
    parser.add_argument("--quiet", "-q", action="store_true",
                        help="静默模式，减少控制台输出")
    args = parser.parse_args()

    LogManager()
    logger = logging.getLogger("cron_sync")

    if args.mode == "auto":
        from sync_core.config_manager import ConfigManager
        run_mode = ConfigManager().get_run_mode()
    else:
        run_mode = RunMode(args.mode)

    logger.info(f"[CRON] 定时同步任务启动: 模式={run_mode.value}, 时间={datetime.now().isoformat()}")

    try:
        orch = SyncOrchestrator(run_mode=run_mode)
        result = orch.run_sync(export_scripts=True)
    except Exception as e:
        logger.critical(f"[CRON] 同步任务异常: {e}", exc_info=True)
        result = {"status": "failed", "error": str(e), "errors": [str(e)]}

    status = result.get("status", "failed")
    if args.output:
        try:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"[CRON] 结果已写入: {args.output}")
        except Exception as e:
            logger.error(f"[CRON] 写入结果文件失败: {e}")

    if not args.quiet:
        summary = {
            "session_id": result.get("session_id"),
            "status": status,
            "success": result.get("execute_result", {}).get("success", 0),
            "failed": result.get("execute_result", {}).get("failed", 0),
            "total_diffs": result.get("diff_summary", {}).get("total_diffs", 0),
            "errors": result.get("errors", [])
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))

    logger.info(f"[CRON] 任务结束: 状态={status}")

    if status == "success" or status == "no_diff":
        return 0
    elif status == "partial":
        return 2
    else:
        return 1


if __name__ == "__main__":
    sys.exit(run())
