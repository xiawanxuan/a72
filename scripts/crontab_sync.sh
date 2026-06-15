#!/bin/bash
# ============================================================
# 智能制造数据中台 - MySQL 与 InfluxDB 双向同步
# Linux Crontab 定时任务启动脚本
# ============================================================
# 使用方法:
#   chmod +x crontab_sync.sh
#   crontab -e
#   # 添加以下行（每5分钟执行一次，预览模式）:
#   */5 * * * * /path/to/crontab_sync.sh preview >> /var/log/sync_cron.log 2>&1
#   # 执行模式（实际执行变更）:
#   */5 * * * * /path/to/crontab_sync.sh execute >> /var/log/sync_cron.log 2>&1
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MODE="${1:-auto}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${VENV_DIR:-venv}"
RESULT_DIR="${RESULT_DIR:-logs/cron_results}"
LOG_FILE="logs/cron_$(date +%Y%m%d).log"

mkdir -p logs "$RESULT_DIR"

if [ -d "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
fi

PY_CMD="$PYTHON_BIN"
if ! command -v "$PY_CMD" >/dev/null 2>&1; then
    PY_CMD="python"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== cron_sync 开始 (模式=$MODE) ==========" | tee -a "$LOG_FILE"

RESULT_FILE="$RESULT_DIR/result_$(date +%Y%m%d_%H%M%S).json"
EXIT_CODE=0
"$PY_CMD" cron_sync.py --mode "$MODE" --output "$RESULT_FILE" || EXIT_CODE=$?

STATUS_MSG="成功"
if [ "$EXIT_CODE" -eq 2 ]; then
    STATUS_MSG="部分成功"
elif [ "$EXIT_CODE" -ne 0 ]; then
    STATUS_MSG="失败"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== cron_sync 结束: $STATUS_MSG (exit=$EXIT_CODE, 结果=$RESULT_FILE) ==========" | tee -a "$LOG_FILE"

exit $EXIT_CODE
