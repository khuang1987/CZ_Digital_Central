# 操作与运维指南 (Operations Guide)

本文档面向系统管理员和最终用户，说明如何日常运行数据更新、查看日志以及处理常见故障。

---

## 1. 如何运行数据更新

本系统采用 **并行调度器 (Parallel Orchestrator)** 进行数据更新，大幅提升了执行效率。

### 1.1 手动触发更新

双击运行项目根目录下的批处理脚本：

> `scripts\orchestration\refresh_parallel.bat`

**执行效果**:
1. 自动激活 Python虚拟环境。
2. 启动 `run_etl_parallel.py` 调度器。
3. 开启 5 个并发进程处理 SAP, SFC, MES 等数据源。
4. 也就是黑底白字的命令行窗口，会显示实时进度条和日志。

### 1.2 自动定时任务

生产环境通常配置 Windows Task Scheduler 每日自动运行。
- **任务名称**: `MDDAP_ETL_Daily_Refresh` (示例)
- **触发时间**: 每日凌晨 02:00
- **执行动作**: 调用上述 `.bat` 脚本

---

## 2. 如何查看日志

### 2.1 实时运行日志 (Console)

在运行窗口中，您会看到如下格式的日志，按任务分组显示：

```text
==================================================
LOGS for TASK: etl_mes_batch_output_raw
--------------------------------------------------
[INFO] Loading config from ...
[INFO] Reading source files ...
[INFO] Inserted 1500 rows into raw_mes.
--------------------------------------------------
```

### 2.2 历史日志文件

所有日志会自动归档，由调度器统一管理。

- **存储位置**: `shared_infrastructure/logs/`
- **文件命名**: `orchestrator_YYYYMMDD_HHMMSS.log`
- **内容**: 包含当次运行的所有详细信息，未报错的任务也会记录在此。

### 2.3 错误排查

如果运行窗口显示 **FAILED** 或红色报错信息：

1. 打开最新的 `orchestrator_*.log` 文件。
2. 搜索 `ERROR` 关键字。
3. 定位到具体的任务名称 (如 `etl_sfc_inspection_raw`)。
4. 查看该任务段落下的 Traceback 堆栈信息。

---

## 3. 常见故障排除 (Troubleshooting)

### 3.1 "File is open by another user" (文件被占用)

**现象**: 
日志提示 PermissionError，无法写入 Excel 或 Parquet 文件。

**原因**: 
有人（或 Power BI）打开了目标文件。

**解决**:
- 关闭所有相关 Excel 文件。
- 关闭 Power BI Desktop (如果正在连接本地文件)。
- 重新运行批处理脚本。

### 3.2 "SQL Server Connection Failed" (数据库连接失败)

**现象**:
提示 `pyodbc.Error: ... Login failed` 或 `Server not found`.

**解决**:
- 检查 VPN 是否连接（如果在家办公）。
- 检查 `config/config.yaml` 或环境变量中的数据库连接字符串是否正确。
- 确认 SQL Server 服务是否运行。

### 3.3 数据不一致 / 缺数

**现象**:
Power BI 里看到的数和产线实际数对不上。

**排查步骤**:
1. 检查源文件 (`Product Output*.xlsx`) 是否已更新到最新。
2. 检查日志中该任务的 **Filtered/Skipped** 行数（是否有数据被过滤规则剔除了）。
3. 检查 **SFC/SAP** 数据源是否缺失关键的 BatchNumber 或 CFN，导致关联失败。

---

## 4. 维护与清理

- **日志清理**: `shared_infrastructure/logs` 下的日志文件建议定期清理（如保留最近30天），可手动删除。
- **备份**: 系统会自动保留最近几次的 Parquet 备份，无需人工干预。
