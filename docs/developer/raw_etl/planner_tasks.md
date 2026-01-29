# Planner 任务数据 ETL

本文档详细说明 Microsoft Planner 任务数据的 ETL 处理逻辑。

## 1. 概览
*   **脚本**: `scripts/data_pipelines/sources/planner/etl/etl_planner_tasks_raw.py`
*   **表名**: `planner_tasks`
*   **来源**: Microsoft Graph API 导出 / Excel
*   **模式**: **MERGE (全量覆盖)** - 任务状态会随时间变化。

## 2. 核心架构：MERGE 策略
不同于交易数据（如 MES/SFC）是“追加型”，Planner 任务是“状态型”数据。同一个 TaskID 的状态可能从 `Not Started` 变为 `Completed`。
因此，不能仅做 Insert，必须使用 **SQL MERGE**。

```sql
MERGE dbo.planner_tasks AS tgt
USING StagingTable AS src
    ON tgt.TaskId = src.TaskId
WHEN MATCHED THEN
    UPDATE SET 
        tgt.Status = src.Status,
        tgt.PercentComplete = src.PercentComplete,
        tgt.CompletedDateTime = src.CompletedDateTime,
        tgt.Labels = src.Labels
WHEN NOT MATCHED THEN
    INSERT (...) VALUES (...)
```

## 3. 详细处理逻辑

### 3.1 团队名自动提取
Planner 导出通常是分多个文件的，文件名代表团队。
*   **逻辑**: 从文件名提取团队标识。
*   **示例**: `SFC-Tier1.xlsx` -> `TeamName = "SFC"` (去除 `-Tier` 后缀)。

### 3.2 标签清洗 (Label Cleaning)
Planner 的 Labels 字段通常是分号分隔的 ID 列表或杂乱文本。
*   **配置文件**: `config/label_cleaning.yaml`
*   **操作**: 
    1.  解析 JSON/CSV 格式的 Label 字段。
    2.  映射为可读的业务标签（如 `Red` -> `Urgent`）。

### 3.3 日期与空值规范化
Pandas 的 `NaT` (Not a Time) 在写入 SQL Server 时经常导致错误。
*   **处理**: 显式将 `NaT` 转换为 Python `None`，确保 SQL Server 接收到真正的 `NULL`。
*   **关键字段**: `DueDate`, `CreatedDate`, `CompletedDate`。

## 4. 输出 Schema
目标表 `dbo.planner_tasks` 的关键字段：

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `TaskId` | `NVARCHAR` | 任务唯一 ID (PK) |
| `TaskName` | `NVARCHAR` | 任务标题 |
| `BucketName` | `NVARCHAR` | 所在的桶 (如 ToDo/Doing) |
| `Status` | `NVARCHAR` | 状态 |
| `Priority` | `INT` | 优先级 |
| `TeamName` | `NVARCHAR` | **归属团队** |
| `Labels` | `NVARCHAR` | 任务标签 |
| `CompletedDate` | `DATE` | 完成日期 |
