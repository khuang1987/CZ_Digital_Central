# 数据定义与来源 (Data Definitions)

本文档定义了数据平台使用的基础术语、数据来源及关键字段含义。

---

## 1. 数据来源 (Data Sources)

本平台整合了制造现场的三大核心系统数据。

### 1.1 数据源映射表

| 数据源 | 系统全称 | 文件标识 | 业务含义 | 关键用途 |
|:-------|:--------|:---------|:---------|:---------|
| **MES** | Manufacturing Execution System | `Product Output*.xlsx` | **实际产出** | 提供产量、工时、操作员、机台信息。这是数据的主骨架。 |
| **SFC** | Shop Floor Control (SharePoint) | SharePoint List | **过程追踪** | 补充 MES 缺失的精确“开始时间”(CheckIn) 和报废详情。 |
| **SAP** | SAP ERP (Routing) | `Routing_*.xlsx` | **标准参数** | 提供标准工时(ST)、OEE系数、调试时间(Setup)。用于计算效率。 |

### 1.2 数据流向

原始数据经过 ETL 处理，最终进入 Power BI。

1. **Raw (ODS)**: 原始文件原样加载。
2. **Logic (DWD)**: 关联 MES+SFC+SAP，计算 ST/LT。
3. **View (DWS)**: 聚合为 KPI 指标（如日达成率、周良率）。

---

## 2. 关键字段字典 (Data Dictionary)

### 2.1 批次与产品 (Identity)

- **BatchNumber (批次号)**
    - 定义: 产品的唯一生产流转标识。
    - 示例: `12345678`
    - 特性: 全局唯一，串联 MES 和 SFC 的主键。

- **CFN (Customer Facing Number)**
    - 定义: 客户可见的产品型号/图纸号。
    - 示例: `DS-100-AB`
    - 用途: 决定 SAP 标准工时 (不同型号工时不同)。

- **Operation (工序号)**
    - 定义: 加工步骤代码。
    - 示例: `0010` (首道), `0020` (清洗), `0030` (包装)

### 2.2 时间与状态 (Time & Status)

- **TrackInTime**
    - 定义: 进入该工序的时间点。
    - 来源: 优先取 SFC CheckIn，次选上道工序 TrackOut。

- **TrackOutTime**
    - 定义: 完成该工序的时间点。
    - 来源: MES Transaction Time。

- **IsSetup (是否换型)**
    - 定义: 标识该批次是否触发了机台换型调试。
    - 值域: `Yes` / `No`
    - 影响: `Yes` 会额外增加 Setup Time 到标准工时中。

---

## 3. 常见缩写

- **ODS**: Operational Data Store (操作数据层)
- **DWD**: Data Warehouse Detail (明细数据层)
- **KPI**: Key Performance Indicator (关键绩效指标)
- **OEE**: Overall Equipment Effectiveness (设备综合效率)
