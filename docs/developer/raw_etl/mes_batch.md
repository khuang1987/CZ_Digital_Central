# MES 批次产出数据 ETL (Raw)

本文档详细说明 MES (制造执行系统) 批次产出数据的 ETL 处理逻辑。

## 1. 概览
*   **脚本**: `scripts/data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py`
*   **表名**: `raw_mes`
*   **来源**: CMES / MES Web Scraper (Excel)
*   **模式**: 文件名正则过滤 + 复杂清洗

## 2. 核心挑战
MES 数据的主要挑战在于**非结构化输入**：
1.  **工序命名混乱**: 手工录入导致同一工序有几十种写法 (如 `CZM 线切割`, `(外协)线切割`).
2.  **机器编码混杂**: 机器字段可能包含名称、编号或二者混合。

## 3. 语义标准化 (Semantic Normalization)

脚本内置了一个强大的正则清洗引擎 (`standardize_operation_name`)，规则如下：

### 3.1 工序名清洗
| 原始输入 (Raw) | 清洗规则 (Logic) | 标准化输出 (Standardized) |
| :--- | :--- | :--- |
| `CZM 线切割` | 去除工厂前缀 | `线切割` |
| `CKH 数控车` | 去除工厂前缀 | `数控车` |
| `(外协) 磨削` | 去除外协标识 | `磨削` |
| `线切-01` | 正则匹配 `线切.*` | `线切割` |
| `微喷砂处理` | 包含 `微` & `喷砂` | `微喷砂` |

### 3.2 机器编码解析 (Machine Parsing)
从 `Resource` 字段中提取标准机器编码：
*   **逻辑**: 提取第一个看起来像机器码的 token。
*   **示例**:
    *   `M12345 (Brother机)` -> `M12345`
    *   `Brother机 M999` -> `M999`

## 4. 处理流程图

```mermaid
graph LR
    File[("MES Excel")] --> Regex{文件名过滤<br/>(_Year/Factory)}
    Regex --> Read[读取 Pandas]
    Read --> CleanOps[工序标准化<br/>(Regex Engine)]
    CleanOps --> ParseMach[机器编码解析]
    ParseMach --> TypeCast[类型转换<br/>(Time/Int)]
    TypeCast --> Hash([Hash生成])
    Hash --> DB[("SQL Server")]
```

## 5. 关键逻辑细节

### 5.1 跨文件去重
MES 数据按月/年拆分，可能存在重叠数据。
*   **Cross-File Deduplication**: 脚本维护一个 `seen_hashes` 集合，在单次运行中处理多个文件时，防止同一条记录被重复读取。

### 5.2 字段映射
*   `Material_Name` -> `BatchNumber` (注意: MES 导出叫 Material Name 但实际是批次号)
*   `DrawingProductNumber_Value` -> `CFN` (用于关联 SAP)
*   `Product_Name` -> `ProductNumber`

## 6. 输出 Schema
目标表 `dbo.raw_mes` 的关键字段：

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `BatchNumber` | `NVARCHAR` | 批次号 (Key) |
| `Operation` | `NVARCHAR` | **标准化后的工序名** |
| `Machine` | `NVARCHAR` | 原始机器名 |
| `Machine(#)` | `NVARCHAR` | **解析出的机器编码** |
| `TrackOutTime` | `DATETIME2` | 报工时间 (Key) |
| `StepInQuantity` | `FLOAT` | 进站数量 |
| `TrackOutQuantity` | `FLOAT` | 出站数量 |
| `record_hash` | `VARCHAR(64)` | 唯一指纹 |
