// ============================================================
// 智能制造数据中台 - InfluxDB 演示初始化 Flux 脚本
// 用途：写入示例数据点，确保 InfluxDB 侧有可同步的 measurement
// 使用方式：
//   1. 登录 InfluxDB Data Explorer
//   2. 打开 Script Editor
//   3. 粘贴本脚本内容并执行（分段执行）
//   4. 或使用 influx CLI: influx query -f init_demo_influx.flux
// ============================================================

// ---------------- 0. 环境变量占位 (需替换为实际 bucket/org) ----------------
// bucket = "device_metrics"
// org    = "smart_factory"

// ---------------- 1. 写入 device_info (设备基础信息) ----------------
import "array"
import "experimental"

deviceInfoData = array.from(rows: [
  {_time: now(), _measurement: "device_info",
    device_id: "DEV-CNC-001", device_name: "CNC加工中心A1", device_type: "CNC",
    device_status: "RUNNING", location: "车间A-产线1-工位1",
    install_date: "2023-06-15T10:00:00Z", manufacturer: "FANUC",
    model: "α-D14MiA5", operator: "张三"},
  {_time: now(), _measurement: "device_info",
    device_id: "DEV-CNC-002", device_name: "CNC加工中心A2", device_type: "CNC",
    device_status: "IDLE", location: "车间A-产线1-工位2",
    install_date: "2023-06-15T10:00:00Z", manufacturer: "FANUC",
    model: "α-D14MiA5", operator: "李四"},
  {_time: now(), _measurement: "device_info",
    device_id: "DEV-ROB-001", device_name: "焊接机器人B1", device_type: "ROBOT",
    device_status: "RUNNING", location: "车间B-产线3-工位1",
    install_date: "2023-08-20T09:00:00Z", manufacturer: "KUKA",
    model: "KR 500 R2830", operator: "王五"},
  {_time: now(), _measurement: "device_info",
    device_id: "DEV-PLC-001", device_name: "主控制柜PLC", device_type: "PLC",
    device_status: "RUNNING", location: "中央控制室",
    install_date: "2023-01-10T08:00:00Z", manufacturer: "SIEMENS",
    model: "S7-1500", operator: "赵六"},
  {_time: now(), _measurement: "device_info",
    device_id: "DEV-SEN-001", device_name: "温湿度传感器组1", device_type: "SENSOR",
    device_status: "RUNNING", location: "车间A-产线1",
    install_date: "2023-09-01T12:00:00Z", manufacturer: "OMRON",
    model: "E5CC-RX2ASM", operator: ""},
  {_time: now(), _measurement: "device_info",
    device_id: "DEV-CNC-003", device_name: "CNC加工中心A3", device_type: "CNC",
    device_status: "FAULT", location: "车间A-产线2-工位1",
    install_date: "2024-01-05T10:00:00Z", manufacturer: "MAZAK",
    model: "INTEGREX i-200", operator: "孙七"}
])

// 执行写入（请取消下一行注释）
// deviceInfoData |> to(bucket: "device_metrics", org: "smart_factory")

// ---------------- 2. 写入 device_metrics (设备运行指标) ----------------
metricsData = array.from(rows: [
  {_time: now() - 10s, _measurement: "device_metrics", _field: "metric_value",
    metric_id: "METRIC_001", device_id: "DEV-CNC-001", metric_type: "SPINDLE_SPEED",
    metric_unit: "rpm", quality_flag: "GOOD", _value: 8500.0},
  {_time: now() - 10s, _measurement: "device_metrics", _field: "metric_value",
    metric_id: "METRIC_002", device_id: "DEV-CNC-001", metric_type: "TEMPERATURE",
    metric_unit: "°C", quality_flag: "GOOD", _value: 42.5},
  {_time: now() - 10s, _measurement: "device_metrics", _field: "metric_value",
    metric_id: "METRIC_003", device_id: "DEV-CNC-001", metric_type: "CURRENT",
    metric_unit: "A", quality_flag: "GOOD", _value: 15.3},
  {_time: now() - 5s,  _measurement: "device_metrics", _field: "metric_value",
    metric_id: "METRIC_004", device_id: "DEV-ROB-001", metric_type: "TEMPERATURE",
    metric_unit: "°C", quality_flag: "GOOD", _value: 38.9},
  {_time: now(),      _measurement: "device_metrics", _field: "metric_value",
    metric_id: "METRIC_005", device_id: "DEV-SEN-001", metric_type: "TEMPERATURE",
    metric_unit: "°C", quality_flag: "GOOD", _value: 25.6},
  {_time: now(),      _measurement: "device_metrics", _field: "metric_value",
    metric_id: "METRIC_006", device_id: "DEV-SEN-001", metric_type: "HUMIDITY",
    metric_unit: "%RH", quality_flag: "GOOD", _value: 48.2}
])

// 执行写入（请取消下一行注释）
// metricsData |> to(bucket: "device_metrics", org: "smart_factory")

// ---------------- 3. 写入 device_alerts (设备告警) ----------------
alertsData = array.from(rows: [
  {_time: now() - 1h,  _measurement: "device_alerts",
    alert_id: "ALT-20240615-0001", device_id: "DEV-CNC-003",
    alert_type: "TEMP_HIGH", alert_level: "CRITICAL",
    alert_message: "主轴温度达到65°C，超过阈值60°C",
    acknowledged: "true", resolved: "false", operator: "赵六"},
  {_time: now() - 6h,  _measurement: "device_alerts",
    alert_id: "ALT-20240615-0002", device_id: "DEV-CNC-001",
    alert_type: "MAINT_DUE", alert_level: "WARNING",
    alert_message: "设备运行累计2000小时，建议更换切削液",
    acknowledged: "true", resolved: "true", operator: "李四"},
  {_time: now() - 30m, _measurement: "device_alerts",
    alert_id: "ALT-20240615-0003", device_id: "DEV-PLC-001",
    alert_type: "COMM_ERROR", alert_level: "INFO",
    alert_message: "PLC心跳丢失1次后自动恢复",
    acknowledged: "false", resolved: "true", operator: "赵六"},
  {_time: now() - 10m, _measurement: "device_alerts",
    alert_id: "ALT-20240615-0004", device_id: "DEV-SEN-001",
    alert_type: "UNCALIBRATED", alert_level: "WARNING",
    alert_message: "传感器连续运行30天未校准",
    acknowledged: "false", resolved: "false", operator: "孙七"},
  {_time: now() - 2m,  _measurement: "device_alerts",
    alert_id: "ALT-20240615-0005", device_id: "DEV-ROB-001",
    alert_type: "CURRENT_OVERLOAD", alert_level: "WARNING",
    alert_message: "焊接瞬间电流超过额定值115%",
    acknowledged: "false", resolved: "false", operator: "王五"}
])

// 执行写入（请取消下一行注释）
// alertsData |> to(bucket: "device_metrics", org: "smart_factory")

// ---------------- 4. 验证查询 ----------------
// import "influxdata/influxdb/schema"
// schema.measurements(bucket: "device_metrics")
// schema.fieldKeys(bucket: "device_metrics")
// schema.tagKeys(bucket: "device_metrics")
//
// from(bucket: "device_metrics")
//   |> range(start: -1h)
//   |> filter(fn: (r) => r._measurement == "device_info")
//   |> limit(n: 10)
