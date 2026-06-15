"""
安装与语法检查脚本
用途：
  1. 验证所有模块可正常导入
  2. 验证核心类可实例化
  3. 不依赖真实数据库，仅做结构自检
运行: python test_install.py
"""

import sys
import os
import importlib
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

MODULES = [
    ("sync_core", "核心包"),
    ("sync_core.config_manager", "配置管理器"),
    ("sync_core.db_adapters", "数据库适配器包"),
    ("sync_core.db_adapters.mysql_adapter", "MySQL适配器"),
    ("sync_core.db_adapters.influxdb_adapter", "InfluxDB适配器"),
    ("sync_core.metadata_collector", "元数据采集器"),
    ("sync_core.diff_engine", "差异对比引擎"),
    ("sync_core.log_rollback", "日志与回滚模块"),
    ("sync_core.sync_executor", "同步脚本生成执行器"),
]

REQUIRED_FILES = [
    "config/db_config.ini",
    "config/sync_rules.json",
    "requirements.txt",
    "main.py",
    "cron_sync.py",
    "scripts/crontab_sync.sh",
    "scripts/scheduled_sync.ps1",
    "scripts/crontab.example",
    "sql/init_demo_database.sql",
    "flux/init_demo_influx.flux",
]

print("=" * 70)
print("  智能制造数据中台 - MySQL ↔ InfluxDB 双向同步系统")
print("  安装自检脚本")
print("=" * 70)

errors = []
warnings = []

print("\n[1/3] 检查必要文件...")
for rel_path in REQUIRED_FILES:
    full = os.path.join(os.path.dirname(os.path.abspath(__file__)), rel_path)
    if os.path.exists(full):
        size = os.path.getsize(full)
        print(f"  ✅ {rel_path:<40} ({size:>8} bytes)")
    else:
        print(f"  ❌ {rel_path:<40} (缺失!)")
        errors.append(f"缺失文件: {rel_path}")

print("\n[2/3] 导入核心模块...")
for mod_name, description in MODULES:
    try:
        importlib.import_module(mod_name)
        print(f"  ✅ {description:<20} -> {mod_name}")
    except Exception as e:
        print(f"  ❌ {description:<20} -> {mod_name}")
        print(f"       错误: {e}")
        errors.append(f"模块导入失败: {mod_name} -> {e}")

print("\n[3/3] 实例化核心类（不连接数据库）...")
checks = [
    ("ConfigManager 加载配置", lambda: __import__("sync_core.config_manager", fromlist=["ConfigManager"]).ConfigManager("config/sync_rules.json", "config/db_config.ini")),
    ("RunMode 枚举", lambda: __import__("sync_core.config_manager", fromlist=["RunMode"]).RunMode.PREVIEW),
    ("SyncDirection 枚举", lambda: __import__("sync_core.config_manager", fromlist=["SyncDirection"]).SyncDirection.BIDIRECTIONAL),
    ("DiffType 枚举", lambda: __import__("sync_core.diff_engine", fromlist=["DiffType"]).DiffType.FIELD_ADD),
    ("DiffEngine 实例化", lambda: __import__("sync_core.diff_engine", fromlist=["DiffEngine"]).DiffEngine()),
    ("LogManager 初始化", lambda: __import__("sync_core.log_rollback", fromlist=["LogManager"]).LogManager()),
    ("SessionManager 实例化", lambda: __import__("sync_core.log_rollback", fromlist=["SessionManager"]).SessionManager()),
    ("RollbackEngine 实例化", lambda: __import__("sync_core.log_rollback", fromlist=["RollbackEngine"]).RollbackEngine(
        __import__("sync_core.log_rollback", fromlist=["SessionManager"]).SessionManager()
    )),
    ("ScriptGenerator 实例化", lambda: __import__("sync_core.sync_executor", fromlist=["ScriptGenerator"]).ScriptGenerator(
        __import__("sync_core.config_manager", fromlist=["ConfigManager"]).ConfigManager()
    )),
]

for name, fn in checks:
    try:
        result = fn()
        print(f"  ✅ {name:<30} -> OK ({type(result).__name__})")
    except Exception as e:
        print(f"  ❌ {name:<30} -> FAIL")
        print(f"       错误: {e}")
        traceback.print_exc(limit=2)
        errors.append(f"类实例化失败: {name} -> {e}")

# ConfigManager 深度检查
try:
    from sync_core.config_manager import ConfigManager
    cm = ConfigManager()
    rules = cm.get_all_rules()
    print(f"\n  ℹ️  已加载同步规则: {len(rules)} 条")
    for r in rules:
        direction_map = {"mysql_to_influx": "→", "influx_to_mysql": "←", "bidirectional": "⇄"}
        arrow = direction_map.get(r.sync_direction.value, "?")
        status = "✅" if r.enabled else "⏸"
        print(f"       {status} [{r.rule_id}] {r.mysql_table} {arrow} {r.influxdb_measurement} "
              f"({len(r.field_mapping)} 字段, {len(r.tag_fields)} 标签)")
    wl = cm._whitelist.get("enabled")
    bl = cm._blacklist.get("enabled")
    print(f"  ℹ️  白名单: {'启用' if wl else '禁用'} | 黑名单: {'启用' if bl else '禁用'}")
except Exception as e:
    errors.append(f"ConfigManager 深度检查失败: {e}")

print()
print("=" * 70)
if errors:
    print(f"  ❌ 自检失败: 发现 {len(errors)} 个错误")
    for i, e in enumerate(errors, 1):
        print(f"     {i}. {e}")
    print()
    print("  建议:")
    print("    1. 运行: pip install -r requirements.txt")
    print("    2. 确认所有文件路径正确")
    print("    3. 确认配置文件格式正确")
    sys.exit(1)
else:
    print(f"  ✅ 自检通过! 共 {len(MODULES)} 模块, {len(REQUIRED_FILES)} 文件, {len(checks)} 项实例化")
    print()
    print("  下一步:")
    print("    1. 配置 config/db_config.ini 填写真实数据库信息")
    print("    2. 执行: python main.py test-conn        # 测试数据库连接")
    print("    3. 执行: python main.py list-rules       # 查看同步规则")
    print("    4. 执行: python main.py preview          # 预览同步脚本")
    print("    5. 确认无误后: python main.py execute    # 执行同步变更")
    print()
    print("  定时任务:")
    print("    Linux:   ./scripts/crontab_sync.sh preview/execute")
    print("    Windows: powershell scripts/scheduled_sync.ps1 execute")
    sys.exit(0)
