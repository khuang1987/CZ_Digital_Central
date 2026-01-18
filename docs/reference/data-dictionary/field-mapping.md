# 字段映射表

本文档详细说明各数据源原始字段到数据库字段的映射关系。

---

## 1. MES 批次产出数据 (raw_mes)

### 数据源
- **来源**: MES 系统导出 Excel
- **文件路径**: `CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/`
- **文件格式**: `CMES_Product_Output_*.xlsx`

### 字段映射

| 原始字段名 (Excel) | 数据库字段名 | 数据类型 | 样例值 | 说明 |
|-------------------|-------------|---------|--------|------|
| Material_Name | BatchNumber | TEXT | `K25L3206`, `K25L4117` | 批次号 |
| ERPOperation | Operation | TEXT | `10`, `20`, `90` | 工序号 |
| Resource | Machine | TEXT | `1259`, `976`, `1001` | 机台号（从Resource提取第5位开始的数字） |
| DateEnteredStep | EnterStepTime | DATETIME | `2025-11-14T13:32:42` | 进入工序时间 |
| Last_TrackIn_Date | TrackInTime | DATETIME | `2025-12-03T07:11:36` | TrackIn 时间 |
| TrackOutDate | TrackOutTime | DATETIME | `2025-12-03T07:12:16` | 报工时间 |
| ERPCode | Plant | TEXT | `1303`, `9997` | 工厂代码 |
| Product_Description | Product_Desc | TEXT | `BODY, CLOSED REDUCER 5597511-01` | 产品描述 |
| Product_Name | ProductNumber | TEXT | `M463501B834`, `M424990B074` | 物料号（M开头代码） |
| DrawingProductNumber_Value | CFN | TEXT | `5597511-01`, `M424990B074` | 图纸号（Customer Facing Number） |
| Step_Name | OperationDesc | TEXT | `CZM 纵切车`, `CZM 线切割` | 工序描述 |
| LogicalFlowPath | Group | TEXT | `50258250`, `50211006` | 工艺组编号（提取数字） |
| Step_In_PrimaryQuantity | StepInQuantity | REAL | `50.0`, `150.0` | 进入工序数量 |
| TrackOut_PrimaryQuantity | TrackOutQuantity | REAL | `52.0`, `96.0` | 报工数量 |
| TrackOut_User | TrackOutOperator | TEXT | `ENT\ZONGS2` | 报工人 |
| ProductionOrder | ProductionOrder | TEXT | `232725201` | 生产订单号 |
| VSM | VSM | TEXT | - | 价值流 |

### 字段提取逻辑

#### Machine 提取
```
原始值: "CZM 1259 纵切机床 Swiss Turning"
提取后: "1259"
规则: 使用正则表达式 ^\w{3}\s*(\d+) 提取数字部分
```

#### Group 提取
```
原始值: "CZM 50258250/0010 CZM 纵切车/CZM 纵切车"
提取后: "50258250"
规则: 使用正则表达式 \d+ 匹配所有数字序列，返回最长的
```

---

## 2. SFC 批次报工数据 (raw_sfc)

### 数据源
- **来源**: SFC 系统导出 Excel
- **文件路径**: `CZ Production - 文档/General/POWER BI 数据源 V2/70-SFC导出数据/批次报工汇总报表/`
- **文件格式**: `LC-*.xlsx`

### 字段映射

| 原始字段名 (Excel) | 数据库字段名 | 数据类型 | 样例值 | 说明 |
|-------------------|-------------|---------|--------|------|
| 产品号 | CFN | TEXT | `5596106-01`, `5597250-04` | 图纸号 |
| 批次 | BatchNumber | TEXT | `K25L2010`, `K25L3100` | 批次号 |
| 工序号 | Operation | TEXT | `20`, `50`, `10` | 工序号 |
| 工序名称 | OperationDesc | TEXT | `CZM 纵切车` | 工序描述 |
| Check In 时间 | TrackInTime | DATETIME | `2025-11-14T00:49:47` | TrackIn时间 |
| 机台号 | Machine | TEXT | - | 机台号 |
| 报工时间 | TrackOutTime | DATETIME | `2025-11-26T02:17:27` | 报工时间 |
| Check In | TrackInOperator | TEXT | - | 开工人 |
| 上道工序报工时间 | EnterStepTime | DATETIME | `2025-11-13T15:53:45` | 上道工序报工时间 |
| 报工人 | TrackOutOperator | TEXT | - | 报工人 |
| 合格数量 | TrackOutQty | REAL | `130.0`, `798.0` | 合格数量 |
| 报废数量 | ScrapQty | REAL | `2.0`, `0.0` | 报废数量 |

### 英文列名兼容映射

| 英文字段名 | 映射到 |
|-----------|--------|
| CheckInTime | TrackInTime |
| CheckIn_User | TrackInOperator |
| TrackOut_User | TrackOutOperator |
| TrackOutQuantity | TrackOutQty |
| ScrapQuantity | ScrapQty |

---

## 3. SAP 工艺路线数据 (raw_sap_routing)

### 数据源
- **来源**: SAP 系统导出 Excel
- **文件路径**: `General - CZ OPS生产每日产出登记/`
- **文件格式**: `*Routing及机加工产品清单.xlsx`
- **Sheet 1**: `*Routing` - 工艺路线表
- **Sheet 2**: `*机加工清单` - 机加工产品清单

!!! note "注意"
    当前 ETL 将 Routing 和 机加工清单 两个 Sheet 合并到同一个 `raw_sap_routing` 表中。

### 字段映射

| 原始字段名 (Excel) | 数据库字段名 | 数据类型 | 样例值 | 说明 |
|-------------------|-------------|---------|--------|------|
| Material Number | ProductNumber | TEXT | `M463501B834`, `M424990B074` | 物料号（M开头代码） |
| CFN | CFN | TEXT | `36136000-02` | 图纸号（Customer Facing Number） |
| Group | Group | TEXT | `50267539` | 工艺组 |
| Plant | Plant | TEXT | `1303`, `9997` | 工厂代码 |
| Operation/activity | Operation | TEXT | `10`, `20`, `30` | 工序号 |
| Operation description | OperationDesc | TEXT | `纵切车`, `研磨` | 工序描述 |
| Work Center | WorkCenter | TEXT | `KSL08-07` | 工作中心 |
| Machine | EH_machine | REAL | `966.0`, `24.0` | 单件机器工时（秒） |
| Labor | EH_labor | REAL | `368.0`, `36.0` | 单件人工工时（秒） |
| Base Quantity | Quantity | REAL | `1.0` | 基本数量 |
| OEE | OEE | REAL | `0.77`, `0.85` | OEE系数 |
| Setup Time (h) | SetupTime | REAL | `0.5`, `1.0` | 调试时间（小时） |

### 中文列名兼容映射

| 中文字段名 | 映射到 |
|-----------|--------|
| 物料 / 物料号 | ProductNumber |
| 工厂 | Plant |
| 工序 / 工序号 | Operation |
| 工作中心 | WorkCenter |
| 工序描述 | OperationDesc |
| 基本数量 | Quantity |
| 工艺组 | Group |

---

## 4. 字段业务含义说明

### MES 关键字段

| 字段 | 业务含义 |
|------|---------|
| **BatchNumber** | 批次号，生产批次的唯一标识 |
| **Operation** | 工序号，生产流程中的步骤编号 |
| **Machine** | 机台号，从 Resource 字段提取 |
| **Plant** | 工厂代码，1303=CZM工厂，9997=CKH工厂 |
| **ProductNumber** | 物料号，SAP系统中的产品唯一标识（M开头） |
| **CFN** | 图纸号（Customer Facing Number），用于与SAP匹配 |
| **Group** | 工艺组编号，从 LogicalFlowPath 提取，用于与SAP匹配 |

### SAP 关键字段

| 字段 | 业务含义 | 计算用途 |
|------|---------|---------|
| **EH_machine** | 单件机器工时（秒） | ST计算：直接使用 |
| **EH_labor** | 单件人工工时（秒） | ST计算：EH_machine为空时使用 |
| **Quantity** | 基本数量，通常为1 | 用于计算单件时间 |
| **SetupTime** | 调试时间（小时） | ST计算：换型时加入 |
| **OEE** | OEE系数，默认0.77 | ST计算：除以OEE |
