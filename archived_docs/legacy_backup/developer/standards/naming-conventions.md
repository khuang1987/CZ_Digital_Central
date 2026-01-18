# 命名规范

本文档定义项目中文件、字段、变量的命名规则。

---

## 文件命名

### ETL 脚本

| 类型 | 格式 | 示例 |
|------|------|------|
| 原始数据ETL | `etl_{source}_{entity}_raw.py` | `etl_mes_batch_output_raw.py` |
| 清洗脚本 | `etl_dataclean_{source}_{entity}.py` | `etl_dataclean_mes_batch_report.py` |
| 配置文件 | `config_{source}_{entity}.yaml` | `config_mes_batch_report.yaml` |

### Power Query 文件

| 格式 | 示例 |
|------|------|
| `{source}_{entity}_{type}.m` | `sfc_equipment_repair_records.m` |

### 文档文件

| 类型 | 格式 | 示例 |
|------|------|------|
| 技术文档 | `{topic}.md` (小写连字符) | `data-architecture.md` |
| 报告文档 | `{TOPIC}_REPORT_{DATE}.md` | `ETL_OPTIMIZATION_REPORT_20251209.md` |

---

## 数据库命名

### 表名

| 层级 | 前缀 | 示例 |
|------|------|------|
| 原始层 (ODS) | `raw_` | `raw_mes`, `raw_sfc`, `raw_sap_routing` |
| 计算层 (DWD) | `v_` (视图) | `v_mes_metrics` |
| 状态表 | `etl_` | `etl_file_status`, `etl_run_log` |

### 字段名

| 规则 | 示例 |
|------|------|
| 使用 PascalCase | `BatchNumber`, `TrackOutTime` |
| 时间字段以 `Time` 或 `Date` 结尾 | `TrackInTime`, `TrackOutDate` |
| 数量字段以 `Qty` 或 `Quantity` 结尾 | `TrackOutQty`, `StepInQuantity` |
| 操作员字段以 `Operator` 结尾 | `TrackInOperator`, `TrackOutOperator` |
| 计算字段使用括号标注单位 | `LT(d)`, `PT(d)`, `EH_machine` |

### 字段命名对照

| 业务概念 | 标准字段名 | 说明 |
|---------|-----------|------|
| 批次号 | BatchNumber | - |
| 工序号 | Operation | - |
| 机台号 | Machine | - |
| 工厂代码 | Plant | 原 ERPCode |
| 物料号 | ProductNumber | M开头代码 |
| 图纸号 | CFN | Customer Facing Number |
| 工艺组 | Group | - |
| TrackIn时间 | TrackInTime | 原 CheckinTime |
| 报工时间 | TrackOutTime | - |
| 报工人 | TrackOutOperator | 原 Operator |
| 开工人 | TrackInOperator | 原 CheckinUser |
| 单件机器工时 | EH_machine | 秒 |
| 单件人工工时 | EH_labor | 秒 |

---

## 变量命名

### Python

| 类型 | 风格 | 示例 |
|------|------|------|
| 变量 | snake_case | `batch_number`, `track_out_time` |
| 常量 | UPPER_SNAKE_CASE | `CONFIG_PATH`, `DB_PATH` |
| 函数 | snake_case | `clean_mes_data()`, `save_to_database()` |
| 类 | PascalCase | `DatabaseManager`, `ETLRunner` |
| DataFrame | snake_case + `_df` | `mes_df`, `result_df` |

### SQL

| 类型 | 风格 | 示例 |
|------|------|------|
| 表名 | snake_case | `raw_mes`, `raw_sfc` |
| 字段名 | PascalCase | `BatchNumber`, `TrackOutTime` |
| 别名 | 小写单字母 | `m` (mes), `s` (sfc), `r` (routing) |

---

## 目录命名

| 类型 | 风格 | 示例 |
|------|------|------|
| 数据源目录 | 小写 | `mes/`, `sfc/`, `sap/` |
| 功能目录 | 小写 | `etl/`, `config/`, `queries/` |
| 文档目录 | 小写连字符 | `data-dictionary/`, `architecture/` |

---

## 版本记录

| 日期 | 更新内容 |
|------|----------|
| 2025-12-13 | 初始版本 |
