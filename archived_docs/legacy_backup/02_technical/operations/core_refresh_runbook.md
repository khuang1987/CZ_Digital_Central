# Core Refresh / Full Refresh Runbook（SQL Server Only）

## 1. 目标

- 数据库以 **SQL Server** 为唯一数据源：`localhost\SQLEXPRESS` / `mddap_v2`
- 日常运行通过批处理脚本刷新核心数据并导出 A1 Parquet
- Parquet 输出以 `scripts/export_core_to_a1.py` 为统一出口，默认使用 reconcile 策略：
  - 缺失分区自动补齐
  - 最近 N 个月数据变化自动重导

---

## 2. 关键脚本

- 日常核心刷新：`scripts/refresh_core_data.bat`
- 全量刷新（含低频表，如 planner/calendar）：`scripts/refresh_all_data.bat`
- Parquet 导出：`scripts/export_core_to_a1.py`

日志目录：`shared_infrastructure/logs/`

---

## 3. 日常运行（推荐）

### 3.0 core vs all 的区别（是否可以交替运行）

- **refresh_core_data.bat（core）**
  - 目标：日常高频刷新“业务核心表”（SFC/MES/监控/KPI + SAP Labor Hours）并导出 Parquet
  - 特点：更快、适合每天/任务计划

- **refresh_all_data.bat（all）**
  - 目标：在 core 的基础上，补齐低频维度与规划类数据（如 `calendar`、`planner_tasks`、`sap_routing`），并做全量 reconcile 导出
  - 特点：更慢、适合每周/每月手动运行或夜间运行

- **是否可以交替运行，会不会把数据搞乱？**
  - 结论：**不会**。
  - 原因：两者都以 SQL Server 表为唯一权威数据源，ETL 写入是增量/去重（基于主键或 `record_hash`），重复跑只会“跳过已处理/无变化的数据”，不会回滚或覆盖成旧数据。
  - 注意：如果你在 core 中设置了测试参数（如 `MAX_ROWS_PER_FILE=500`），可能导致某些表的“本次增量”被限制；all 脚本默认不带该限制。

### 3.1 运行 core refresh

运行：

- `scripts/refresh_core_data.bat`

可选环境变量：

- `MAX_NEW_FILES`：默认 20（文件型 ETL 每次最多处理的新文件数）
- `MAX_ROWS_PER_FILE`：默认 500（测试模式，避免一次性读入超大文件）
- `MES_EXTRA_ARGS`：传递给 MES ETL 的额外参数
- `RECONCILE_LAST_N`：默认 3（导出时仅对最近 N 个月做变更检测；缺失补齐仍会扫描全历史月份）

### 3.2 SAP Labor Hours（日常增量）

- core refresh 内会运行：`data_pipelines/sources/sap/etl/etl_sap_labor_hours.py --ypp`
- 数据源目录：
  - `C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\40-SAP工时`
- 日常增量逻辑：读取最新的 `YPP*.xlsx` 文件并写入 `dbo.raw_sap_labor_hours`

---

## 4. 全量运行（手动/低频）

运行：

- `scripts/refresh_all_data.bat`

包含：

- SAP routing
- planner tasks
- calendar
- SAP labor hours（默认 `--ypp`，如需全量请手动运行见下）

---

## 5. Parquet 导出与 reconcile

### 5.1 日常导出（core refresh 已内置）

- `python scripts/export_core_to_a1.py --mode partitioned --reconcile --reconcile-last-n N`

行为：

- 缺失/损坏 parquet：自动补齐对应月份
- 变更检测：仅对最近 N 个月比对（行数 + 可选 hash 摘要），变化则重导

### 5.2 首次治理/全历史对账（手动）

- `python scripts/export_core_to_a1.py --mode partitioned --reconcile --reconcile-all`

### 5.3 导出元数据（用于 reconcile 自动比对）

- 默认：`export_core_to_a1.py` 将 reconcile 元数据写入 SQL Server：`dbo.export_partition_meta`（**不会在分区目录落地** `*.meta.json`）
- 如需兼容/调试：可用 `--meta-store json` 或 `--meta-store both`

---

## 6. 常见问题排查

### 6.1 SAP 工时只导出到 12 月

判断方法：

- 看 `dbo.raw_sap_labor_hours` 的 `max(PostingDate)`
- 目录中 `YPP*.xlsx` 的 `PostingDate` 可能已到 1 月，但 SQL Server 若未导入则导出会显示空

处理：

- 先运行一次：`python data_pipelines/sources/sap/etl/etl_sap_labor_hours.py --ypp`
- 再运行：`python scripts/export_core_to_a1.py --mode partitioned --reconcile --reconcile-last-n 3 --datasets sap_labor_hours`

### 6.2 中文日志乱码

- `refresh_core_data.bat` 已设置 `chcp 65001`、`PYTHONUTF8=1`、`PYTHONIOENCODING=utf-8`
- `refresh_all_data.bat` 同样已设置上述环境变量，并在首次创建日志时写入 UTF-8 BOM
- 若仍乱码，优先检查：
  - 任务计划程序的运行账户是否有权限写日志目录
  - 终端/查看器是否按 UTF-8 打开 `.log`
