# MDDAP 系统编排与运行指南 (Orchestration & Workflow Guide)

本文件夹包含了驱动 MDDAP 数据平台运行的核心编排脚本，负责从数据采集、ETL 清洗到服务启动的完整全生命周期管理。

---

## 1. 核心运行脚本 (Main Entry Points)

### `refresh_parallel.bat` (首选运行方式)
*   **用途**: **全自动一键刷新**。它会自动检测并加载当前用户的虚拟环境 (`.venv` 或 `.venv_%USERNAME%`)，然后调用并行 ETL 引擎。
*   **用法**:
    *   `双击运行`：执行全套流程（采集 -> ETL -> 数据库更新 -> 刷新 PBI）。
    *   `refresh_parallel.bat --only-collection`: **[服务器测试专用]** 仅运行 Stage 0 (数据抓取)，不连接数据库，用于验证网络和账号。

### `run_etl_parallel.py`
*   **用途**: 并行任务协调引擎。将任务分为多个阶段（Stages），阶段内并行，阶段间串行。
*   **参数**:
    *   `--only-collection`: 跳过 Stage 1-5 (SQL/DB 相关)，仅执行 Stage 0。

---

## 2. 数据采集子系统 (Data Collection)

### `run_data_collection.py` (统一采集入口)
*   **用途**: 所有的自动化采集（Playwright 驱动）都通过此入口调度。
*   **参数 [target]**:
    *   `planner`: 仅采集微软 Planner 任务数据。
    *   `cmes`: 仅采集 CMES (PowerBI) 的产出与 WIP。
    *   `labor`: 仅运行 SAP 工时文件格式化。
    *   `tooling`: 仅导出 SPS 模具交易日志。
    *   `refresh`: 仅触发 PowerBI Service 在线刷新。
    *   `all`: 执行以上所有任务（默认）。
*   **参数 [Flags]**:
    *   `--headless`: 隐藏浏览器界面（默认模式，适合自动运行）。
    *   `--no-headless`: 显示浏览器界面（适合本地调试、账号验证）。

---

## 3. 服务启动与维护 (Services & Utilities)

### `launch_services.py`
*   **用途**: 启动应用层的三个核心服务：
    1.  **Dashboard** (Streamlit): 运营控制台。
    2.  **Docs** (MkDocs): 技术文档。
    3.  **Catalog** (Data Search): 数据目录。
*   **端口**: 默认占用 8501, 8000 等。

### `sqlserver_postcheck.py`
*   **用途**: **数据库健康检查**。验证 ODBC 驱动、数据库连接、表结构是否完整，以及最新数据点的时间戳。
*   **用法**: `python scripts/orchestration/sqlserver_postcheck.py`

### `export_core_to_a1.py`
*   **用途**: 深度 ETL 逻辑。将 SQL Server 中的核心物理表转换为 A1 Partitioned 逻辑，支持高效的数据对账 (`--reconcile`)。

### `trigger_pbi_refresh.py`
*   **用途**: 封装了模拟点击 PowerBI "Now Refresh" 按钮的逻辑，由采集器在 `refresh` 模式下调用。

---

## 5. 常用命令行示例 (Common CLI Examples)

以下是在 **命令提示符 (CMD)** 或 **PowerShell** 中常用的指令示例：

### 全集运行 (Full Refresh)
```batch
:: 运行全套流程 (推荐)
scripts\orchestration\refresh_parallel.bat

:: 服务器专用：仅运行采集任务进行验证
scripts\orchestration\refresh_parallel.bat --only-collection
```

### 针对性采集 (Targeted Collection)
```batch
:: 仅采集 Planner 数据并显示浏览器界面 (方便验证账号)
python scripts\orchestration\run_data_collection.py planner --no-headless

:: 仅执行 PowerBI 刷新指令
python scripts\orchestration\run_data_collection.py refresh

:: 仅采集 CMES 和 Labor 数据
python scripts\orchestration\run_data_collection.py cmes labor
```

### 服务启动 (Services)
```batch
:: 启动 Dashboard 面板
python scripts\orchestration\launch_services.py

:: 检查数据库健康状态
python scripts\orchestration\sqlserver_postcheck.py
```

---

## 6. 目录结构说明

*   `config/`: 存储编排层专用的配置文件（如 PowerBI 刷新 URL）。
*   `__pycache__/`: Python 编译产生的临时文件夹。

---

## ⚠️ 注意事项 (Important)

1.  **环境依赖**: 所有脚本运行时均依赖根目录下的 `.env` (或用户特定 `.env_czxmfg`) 中定义的凭据。
2.  **并行度**: `run_etl_parallel.py` 内部默认使用 5 个并发进程。如服务器性能受限，可修改脚本中的 `max_workers`。
3.  **日志**: 运行日志统一存放在 `shared_infrastructure/logs/` 下，文件名为 `orchestrator_YYYYMMDD_HHMMSS.log`。
