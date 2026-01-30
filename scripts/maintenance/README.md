# MDDAP 系统维护与迁移工具 (Maintenance & Migration Tools)

本文件夹包含了用于系统部署、路径诊断、数据库迁移以及指标计算的辅助脚本。

## 1. 环境与路径诊断 (Environment & Path Diagnostics)

### `check_server_paths.py`
*   **用途**: 检查项目根目录、OneDrive 根目录以及配置文件加载情况。
*   **用法**: `python scripts/maintenance/check_server_paths.py`

### `check_db_connection.py`
*   **用途**: 基础数据库连接测试，检查当前环境是否能连接到 SQL Server。

### `portable_db_test.py`
*   **用途**: **便携式**数据库测试工具，带有交互式界面，可在未配置 `.env` 的机器上快速测试连接。

### `show_urls.py`
*   **用途**: 探测当前机器的局域网 IP，并显示 Dashboard 和 Docs 的访问地址，方便在其他电脑访问服务器。

---

## 2. 数据库迁移 (SQL Server Migration)

### `sql_step1_backup.sql`
*   **用途**: 在**本地开发机**执行，将 `mddap_v2` 备份到 `C:\Temp\mddap_v2.bak`。

### `sql_step2_restore.sql`
*   **用途**: 在**服务器**执行，从同步过来的 `.bak` 文件还原数据库。

### `sqlserver_postcheck.py`
*   **用途**: 迁移后的自动校验，检查连接、表结构和最新数据点。

---

## 3. 任务自动化与管理 (Automation & Management)

### `install_mddap_task.bat`
*   **用途**: **一键安装 Windows 计划任务**。它会自动请求管理员权限并调用 `setup_mddap_task.ps1`。
*   **功能**: 让数据采集程序在后台按计划自动运行。

### `setup_mddap_task.ps1`
*   **用途**: PowerShell 脚本，负责具体的 Windows Task Scheduler 注册逻辑。

### `clear_mes_raw_full.bat`
*   **用途**: **快速清理 MES 原始数据表** (dbo.raw_mes)。
*   **注意**: 仅在需要重新导入历史数据或调试 ETL 时使用。

---

## 4. 数据库指标计算 (Database Metrics & Metrics)

*以 `_` 开头的脚本通常由主程序自动调用，或用于手动初始化指标：*

*   `_compute_mes_metrics_incremental.sql`: 增量计算 MES 指标（如产出、效率）。
*   `_compute_mes_metrics_materialized.sql`: 重新计算全量物化指标表。
*   `_init_mes_metrics_materialized.sql`: 初始化物化表结构。
*   `_refresh_mes_metrics_materialized.py`: Python 触发器，用于定期刷新 SQL 中的指标。

---

## 5. 其他辅助 (Misc)

### `.env_czxmfg` (位于项目根目录)
*   **用途**: 服务器专用环境变量。程序会根据当前用户名 (`czxmfg`) 优先加载此文件，实现生产环境自动切换。
