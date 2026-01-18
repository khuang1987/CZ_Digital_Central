# 数据处理技术路线总览 & 脚本清理建议

## 1. 总体目标与原则

- 数据库以 **SQL Server** 为唯一数据源（`localhost\SQLEXPRESS` / `mddap_v2`）。
- BI 查询以 `dbo.v_mes_metrics` 为主入口；该视图当前为 **物化快照读取**（避免实时重算导致超时）。
- Parquet 输出以 `scripts/export_core_to_a1.py` 为统一出口，输出到 `A1_ETL_Output`。
- 清理文件时遵循：
  - 不直接删除：先做候选清单 + 风险说明 + 你确认后再删。
  - 以“是否仍被 `.bat`/主脚本引用”为第一判据。

---

## 2. 数据处理技术路线（从获取到 Parquet）

### 2.1 数据获取（Excel / SharePoint / 本地路径）

主要由各数据源 ETL 在 `data_pipelines/sources/*/etl/` 中读取。

- **MES**：`data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py`
- **SFC 批次报工**：`data_pipelines/sources/sfc/etl/etl_sfc_batch_output_raw.py`
- **SFC 产品检验**：`data_pipelines/sources/sfc/etl/etl_sfc_inspection_raw.py`
- **SAP Routing**：`data_pipelines/sources/sap/etl/etl_sap_routing_raw.py`
- **Planner**：`data_pipelines/sources/planner/etl/etl_planner_tasks_raw.py`
- **维度表**：`data_pipelines/sources/dimension/etl/etl_calendar.py` 等

### 2.2 清洗（字段标准化/类型修正/去重/增量识别）

- 清洗逻辑通常在各 ETL 内完成：
  - 字段中英文映射为统一英文（如 `BatchNumber`、`Operation`、`TrackOutTime`）
  - 基本类型转换（日期、数值）
  - 增量识别：通过 ETL 内部的 `is_file_changed/filter_changed_files`（当前部分脚本仍使用 Dual/SQLite 的状态机制，见“清理建议-需迁移项”）

### 2.3 保存到 SQL Server（ODS 原始层）

写入目标一般为 `dbo.raw_*` 表：

- `dbo.raw_mes`
- `dbo.raw_sfc`
- `dbo.raw_sfc_inspection`
- `dbo.raw_sap_routing`
- `dbo.planner_tasks` / `dbo.planner_task_labels`

> 注意：当前代码里仍可见 `mddap_v2.db`（SQLite）路径的残留，属于“历史/迁移期实现”。最终目标是：增量状态也迁移到 SQL Server。

### 2.4 MES 指标计算（物化快照 / BI 秒开）

现行路线：

- 初始化一次：
  - `scripts/_execute_create_view_v2.py`（执行 `scripts/_init_mes_metrics_materialized.sql`）
- 日常刷新：
  - `scripts/_refresh_mes_metrics_materialized.py`（执行 `scripts/_compute_mes_metrics_materialized.sql`）

数据库对象：

- `dbo.mes_metrics_snapshot_a` / `dbo.mes_metrics_snapshot_b`
- `dbo.mes_metrics_current`（synonym，指向当前快照）
- `dbo.v_mes_metrics`（只读 `mes_metrics_current`，供 BI 连接）

详细说明见：
- `docs/02_technical/database/mes_metrics_materialization.md`

### 2.5 导出 Parquet（A1 输出）

统一出口：

- `scripts/export_core_to_a1.py`

输出目录：

- `A1_ETL_Output/01_CURATED_STATIC/`
- `A1_ETL_Output/02_CURATED_PARTITIONED/{dataset}/{YYYY}/{prefix}_{YYYYMM}.parquet`

分区命名规则说明：
- `docs/02_technical/operations/a1_etl_output_partition_naming.md`

---

## 3. scripts/ 与 scripts/validation/ 清理建议（候选清单 + 依据）

### 3.1 明确仍在用（建议保留）

- `scripts/export_core_to_a1.py`：统一 Parquet 导出入口（SQL Server 读）
- `scripts/_execute_create_view_v2.py`：初始化 MES 物化对象（快照表 + synonym + 轻量 view）
- `scripts/_refresh_mes_metrics_materialized.py`：刷新 MES 物化快照（A/B 轮换 + 原子切换）
- `scripts/_init_mes_metrics_materialized.sql` / `scripts/_compute_mes_metrics_materialized.sql`：物化 SQL
- `scripts/_check_view_metrics.py`：v_mes_metrics 轻量质量检查
- `scripts/_check_mes_count.py`：raw_mes 行数/工厂分布快速检查
- `scripts/_test_sqlserver_connection.py`：SQL Server 连通性检查

### 3.2 明显“SQLite-only”旧路线（高概率可删候选，需你确认）

这些脚本/批处理明确写着 SQLite / `mddap_v2.db`，且会误导新路线：

- `scripts/refresh_all_data_sqlite.bat`：全量刷新 SQLite
- `scripts/refresh_full_data.bat`：标题标注 SQLite，输出 SQLite

> 风险：如果你仍需要“历史 SQLite 对照验证”，可以先移入 `scripts/_archive_sqlite/`（或保持不删）。

### 3.3 迁移期/验证期工具（不建议日常运行，建议保留到迁移彻底完成后再处理）

位于 `scripts/validation/`：

- `scripts/validation/backfill_sqlserver_from_sqlite.py`：从 SQLite 回填 SQL Server（DELETE + full reload）
- `scripts/validation/compare_row_counts_dual_db.py`：对比 SQLite vs SQL Server 行数
- 其它 verify/validate：多为“历史口径对照”用途

> 建议：迁移完全确认后，这批可以整体归档到 `docs/04_reports_archive/` 对应报告里留结论，然后脚本移入 `scripts/_archive_validation/` 或删除。

### 3.4 临时调试/一次性脚本（可删候选，但需要逐个确认）

典型特征：

- 文件名含 `_temp_`、`_debug_`、`_verify_`、`_trace_`
- 只用于某一次 bug 排查

示例（来自当前目录列表，未逐个审计调用关系）：

- `scripts/_temp_check_routing_fields.py`
- `scripts/_temp_check_routing_match.py`
- `scripts/_temp_purge_all_mes.py`
- `scripts/_temp_test_routing_match_fixed.py`
- `scripts/_debug_float_issue.py`
- `scripts/_debug_insert_issue.py`
- `scripts/_verify_2025_data.py`（注意列名旧逻辑，可能已过时）

> 这类脚本建议你确认：“是否还会复用做日常巡检？”如果不会，优先归档或删除。

---

## 4. 下一步建议（你确认后我再动手）

1) 你确认清理策略：
- **策略A：只删明确 SQLite-only 的 `.bat`**
- **策略B：SQLite-only + `_temp/_debug/_verify/_trace` 全部移到归档目录**
- **策略C：最激进：删除 scripts/validation 中所有 SQLite 对照工具**（不推荐，除非迁移完全结束）

2) 我会输出一个“最终删除清单”并等你确认后再执行删除/移动。

---

## 版本记录

- 2026-01-08：首次整理数据处理路线与 scripts 清理建议（待你确认删除范围）。
