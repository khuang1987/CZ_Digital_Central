# SFC 批次追踪数据 ETL (Raw)

本文档详细说明 SFC (Shop Floor Control) 数据的 ETL 处理逻辑。

## 1. 概览
*   **脚本**: `scripts/data_pipelines/sources/sfc/etl/etl_sfc_batch_output_raw.py`
*   **表名**: `raw_sfc`
*   **来源**: Sharepoint Excel (每日增量文件)
*   **作用**: 提供最准确的“批次 Check-in 时间”和拆批/合批记录。

## 2. 逻辑流程

```mermaid
graph TD
    SharePoint[("SFC Folder<br/>(SharePoint)")] --> Sort[按时间排序]
    Sort --> Filter{最新 N 个文件<br/>or 增量检测}
    
    Filter --> Read[读取 Excel]
    Read --> Exclude[过滤无效批次<br/>(Batch like '-0%')]
    Exclude --> Map[中文列名映射]
    Map --> TimeParse[时间格式解析]
    TimeParse --> Hash([生成 Hash])
    Hash --> DB[("SQL Server")]
```

## 3. 关键处理步骤

### 3.1 无效批次剔除
SFC 系统中，包含大量临时生成的拆批记录（以 `-0` 开头，如 `123456-01`），这些通常不是主批次，会干扰 WIP 统计。
*   **规则**: `BatchNumber` 正则匹配 `r"-0\d+$"`。
*   **操作**: `Drop` 掉匹配的行。

### 3.2 中英文列名映射
SFC 原始导出为中文列名，需映射为标准 DB 字段：

| 中文列名 | 英文 DB 字段 | 说明 |
| :--- | :--- | :--- |
| `产品号` | `CFN` | 关键 Key |
| `批次` | `BatchNumber` | 关键 Key |
| `工序号` | `Operation` | 转 Int |
| `Check In` | `TrackInOperator` | (注意原始歧义) |
| `Check In 时间` | `TrackInTime` | **核心业务时间** |
| `报工时间` | `TrackOutTime` | |

### 3.3 增量处理策略
考虑到 SFC 文件巨大且频繁更新：
1.  **文件名排序**: 始终优先处理最新的文件 (`mtime` 倒序)。
2.  **Max Files 限制**: 单次运行默认限制处理 `max_new_files=20`，防止内存溢出。
3.  **Hash 跳过**: 数据库指纹比对，已存在记录直接跳过。

## 4. 输出 Schema
目标表 `dbo.raw_sfc` 的关键字段：

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `BatchNumber` | `NVARCHAR` | 批次号 |
| `Operation` | `INT` | 工序号 |
| `TrackInTime` | `DATETIME2` | **开工时间 (用于 LT 计算)** |
| `EnterStepTime` | `DATETIME2` | 进站时间 |
| `ScrapQty` | `FLOAT` | 报废数量 |
| `TrackOutQty` | `FLOAT` | 合格数量 |
| `record_hash` | `VARCHAR(64)` | 唯一指纹 |
