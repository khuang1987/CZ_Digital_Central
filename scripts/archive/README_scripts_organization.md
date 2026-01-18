# scripts/ 目录整理规范（SQL Server Only）

## 目标

- 保留 2 个入口：每日核心刷新（core）+ 全量刷新（all）
- 根目录只放“入口脚本 / 核心运行脚本 / 核心 SQL”，其余检查/临时/历史脚本统一归档，避免混乱
- 不破坏现有引用关系（保留 `scheduled_refresh.bat` 作为兼容入口）

## 根目录必须保留（不要移动）

- `refresh_core_data.bat`：每日核心数据刷新（计划任务入口）
- `refresh_all_data.bat`：全量/低频数据刷新（人工运行）
- `scheduled_refresh.bat`：历史兼容入口（建议后续逐步弃用，统一到 core/all）
- `setup_scheduled_task.ps1`：创建/更新 Windows 任务计划
- `export_core_to_a1.py`：统一 Parquet 导出
- `_refresh_mes_metrics_materialized.py`：刷新 MES 指标物化快照
- `_execute_create_view_v2.py`：初始化/重建数据库对象（通常人工运行）
- `_init_mes_metrics_materialized.sql` / `_compute_mes_metrics_materialized.sql`：物化 SQL
- `README_refresh_data.md`：运维说明
- `validation/`：校验脚本目录（例如 `validation/sqlserver_postcheck.py`）

## 建议目录结构

- `scripts/`
  - `refresh_core_data.bat`
  - `refresh_all_data.bat`
  - `scheduled_refresh.bat`
  - `setup_scheduled_task.ps1`
  - `export_core_to_a1.py`
  - `_refresh_mes_metrics_materialized.py`
  - `_execute_create_view_v2.py`
  - `sql/`（可选：保留仍有效 SQL）
  - `checks/`（只读检查脚本：`_check_*`、`check_tables.py` 等）
  - `maintenance/`（可写操作/修复脚本：`_clear_*`、`purge_*`、`refresh_mes_full.py` 等）
  - `archive/`（历史/临时/一次性脚本：`_temp_*`、`_debug_*`、`_verify_*`、旧 bat、旧 sql 等）
  - `debug/`（保留现有：更细的调试脚本）
  - `migrate/`（保留现有：迁移工具）
  - `validation/`（保留现有：校验工具）

## 本次整理的分类规则

- **checks/**：只读、不会写库/删数据，主要输出诊断信息
- **maintenance/**：会写库/删数据/强制刷新等运维动作
- **archive/**：临时/调试/历史方案脚本，默认不再用于日常运行

## 默认移动建议（概要）

- `_check_*.py` + `check_tables.py` -> `checks/`
- `_clear_*.py`、`purge_*.py`、`refresh_mes_full.py`、`test_db_write.py`、`test_mes_raw_etl.py` -> `maintenance/`
- `_temp_*.py`、`_debug_*.py`、`_verify_*.py`、`_trace_*.py` -> `archive/`
- `refresh_full_data.bat`、`refresh_all_data_sqlite.bat` 等旧 bat -> `archive/bat_deprecated/`

> 说明：目前保留两个入口 bat（core/all）。`scheduled_refresh.bat` 仅用于兼容历史引用。
- `_create_mes_view_sqlserver*.sql` -> `archive/sql_deprecated/`
- `_export_mes_to_parquet.py` -> `archive/`（已由 `export_core_to_a1.py` 统一替代）

> 注意：整理后若有引用断裂（例如某脚本硬编码相对路径），需要同步修正引用；本次策略优先不移动核心入口依赖。
