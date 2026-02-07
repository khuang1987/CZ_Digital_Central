# SAP Routing 原始数据 ETL (Raw)

本文档详细说明 SAP Routing (工艺路线) 数据的 ETL 处理逻辑。

## 1. 概览
*   **脚本**: `scripts/data_pipelines/sources/sap/etl/etl_sap_routing_raw.py`
*   **表名**: `raw_sap_routing`
*   **来源**: Sharepoint Excel (SAP 导出报表)
*   **模式**: 并行处理 (Parallel) + 增量检测 (CDC)

## 2. 处理流程

```mermaid
graph TD
    Excel[("SAP Excel<br/>(Factory Files)")] --> Check{CDC 检测<br/>(Hash/Mtime)}
    
    Check -->|No Change| Skip([跳过处理])
    Check -->|Modified| Read[读取数据]
    
    subgraph Reading ["多 Sheet 读取"]
        Read --> Sheet1[读取 'Routing' Sheet]
        Read --> Sheet2[读取 'Machining' Sheet]
        Sheet1 --> Join[关联 Join]
        Sheet2 --> Join
    end
    
    Join --> Clean[数据清洗 & 规范化]
    Clean --> Calc[工时计算]
    Calc --> Hash[生成 record_hash]
    Hash --> Deduplicate[数据库去重]
    Deduplicate --> Upsert[写入 MS SQL]
```

## 3. 详细逻辑

### 3.1 多源关联 (Multi-Source Join)
单一的 Routing 表不足以支持业务，因为 `OEE` 和 `SetupTime` 往往存储在单独的 Machining 表中。

*   **关联键**: `CFN` + `Operation` + `Group`
*   **操作**: `Left Join`
*   **目的**: 将 Machining Sheet 中的 `OEE` 和 `SetupTime` 匹配到主 Routing 表。

### 3.2 数据清洗 (Cleaning)
*   **空值处理**: 将 `#N/A`, `N/A`, `n/a`, 空字符串统一转换为 `NULL`。
*   **列名映射**:
    *   `Labor` -> `EH_labor`
    *   `Machine` -> `EH_machine`
    *   `Base Quantity` -> `Quantity`
*   **类型强转**:
    *   `Operation` -> `Int64` (去除小数位)
    *   `Plant` -> `Int64`

### 3.3 核心计算 (Core Calculation)
标准工时 (`StandardTime`) 是排产和效率计算的基石。

$$
StandardTime (min) = \frac{\text{SelectedTime (sec)}}{60}
$$

**选择逻辑**:
1.  优先使用机器工时 (`EH_machine`)。
2.  若机器工时为 0 或空，则使用人工工时 (`EH_labor`)。
3.  结果保留 1 位小数。

**OEE 默认值**:
*   若 OEE 为空或 0，则默认为 **0.77** (77%)。

### 3.4 幂等性保障 (Idempotency)
*   **Hash 生成**: `MD5(ProductNumber + CFN + Group + Operation + factory_code)`
*   **去重**:
    1.  **批内去重**: Pandas `drop_duplicates(subset=['record_hash'])`。
    2.  **库内去重**: 写入前查询数据库已存在的 Hash，仅插入新 Hash。

## 4. 输出 Schema
目标表 `dbo.raw_sap_routing` 的关键字段：

| 字段名 | 类型 | 说明 |
| :--- | :--- | :--- |
| `ProductNumber` | `NVARCHAR` | 物料号 |
| `CFN` | `NVARCHAR` | 客户成品号 (Key) |
| `Operation` | `INT` | 工序号 (Key) |
| `StandardTime` | `FLOAT` | **计算出的标准工时 (分)** |
| `SetupTime` | `FLOAT` | 换型时间 (来自 Machining) |
| `OEE` | `FLOAT` | 设备效率 |
| `EH_machine` | `FLOAT` | 原始机器秒数 |
| `EH_labor` | `FLOAT` | 原始人工秒数 |
| `record_hash` | `VARCHAR(64)` | 唯一指纹 |
