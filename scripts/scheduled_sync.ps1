# ============================================================
# 智能制造数据中台 - MySQL 与 InfluxDB 双向同步
# Windows 计划任务启动脚本 (PowerShell 5+)
# ============================================================
# 使用方法:
#   1. 右键 -> 使用 PowerShell 运行 (测试)
#   2. 任务计划程序 -> 创建基本任务:
#      - 程序: powershell.exe
#      - 参数: -ExecutionPolicy Bypass -File "D:\path\to\scripts\scheduled_sync.ps1" execute
#      - 触发器: 每天 / 每5分钟
# ============================================================

param(
    [ValidateSet("preview", "execute", "auto")]
    [string]$Mode = "auto"
)

$ErrorActionPreference = "Continue"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

$LogDir = Join-Path $ProjectRoot "logs"
$ResultDir = Join-Path $LogDir "cron_results"
New-Item -ItemType Directory -Force -Path $LogDir, $ResultDir | Out-Null

$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
$DateStamp = Get-Date -Format "yyyyMMdd"
$LogFile = Join-Path $LogDir "cron_$DateStamp.log"
$ResultFile = Join-Path $ResultDir "result_$(Get-Date -Format 'yyyyMMdd_HHmmss').json"

Write-Host "[$Timestamp] ========== scheduled_sync 开始 (模式=$Mode) ==========" -ForegroundColor Cyan
"[$Timestamp] ========== scheduled_sync 开始 (模式=$Mode) ==========" | Out-File -Append -FilePath $LogFile -Encoding UTF8

$PythonCmd = "python"
try {
    $PythonCmd = (Get-Command python -ErrorAction Stop).Source
} catch {
    try {
        $PythonCmd = (Get-Command py -ErrorAction Stop).Source
    } catch {
        Write-Error "未找到 Python 解释器"
        exit 1
    }
}

$VenvDir = Join-Path $ProjectRoot "venv"
if (Test-Path $VenvDir) {
    $VenvPy = Join-Path $VenvDir "Scripts\python.exe"
    if (Test-Path $VenvPy) {
        $PythonCmd = $VenvPy
    }
}

$Args = @(
    (Join-Path $ProjectRoot "cron_sync.py"),
    "--mode", $Mode,
    "--output", $ResultFile
)

$ExitCode = 0
try {
    & $PythonCmd @Args 2>&1 | Tee-Object -FilePath $LogFile -Append
    $ExitCode = $LASTEXITCODE
} catch {
    "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] 执行异常: $_" | Out-File -Append -FilePath $LogFile -Encoding UTF8
    Write-Error $_
    $ExitCode = 1
}

$Status = switch ($ExitCode) {
    0 { "成功" }
    2 { "部分成功" }
    default { "失败" }
}

$Color = switch ($ExitCode) {
    0 { "Green" }
    2 { "Yellow" }
    default { "Red" }
}

$EndTimestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Host "[$EndTimestamp] ========== scheduled_sync 结束: $Status (exit=$ExitCode, 结果=$ResultFile) ==========" -ForegroundColor $Color
"[$EndTimestamp] ========== scheduled_sync 结束: $Status (exit=$ExitCode, 结果=$ResultFile) ==========" | Out-File -Append -FilePath $LogFile -Encoding UTF8

exit $ExitCode
