# 跨表对照总表

本文档展示业务概念在各数据表中的字段对应关系。

---

## 核心字段对照

| 业务概念 | MES原始字段 | raw_mes | raw_sfc | raw_sap_routing | v_mes_metrics |
|---------|------------|---------|---------|-----------------|---------------|
| **批次号** | Material_Name | BatchNumber | BatchNumber | - | BatchNumber |
| **工序号** | ERPOperation | Operation | Operation | Operation | Operation |
| **机台号** | Resource | Machine | Machine | - | Machine |
| **图纸号(CFN)** | DrawingProductNumber_Value | CFN | CFN | CFN | CFN |
| **物料号** | Product_Name | ProductNumber | - | ProductNumber | ProductNumber |
| **工艺组** | LogicalFlowPath→提取 | Group | - | Group | Group |
| **进入工序时间** | DateEnteredStep | EnterStepTime | EnterStepTime | - | EnterStepTime |
| **TrackIn时间** | Last_TrackIn_Date | TrackInTime | TrackInTime | - | TrackInTime |
| **报工时间** | TrackOutDate | TrackOutTime | TrackOutTime | - | TrackOutTime |
| **报工数量** | TrackOut_PrimaryQuantity | TrackOutQuantity | TrackOutQty | - | TrackOutQuantity |
| **报废数量** | - | - | ScrapQty | - | ScrapQty |
| **工序描述** | Step_Name | OperationDesc | OperationDesc | OperationDesc | OperationDesc |
| **工厂代码** | ERPCode | Plant | - | Plant | Plant |
| **产品描述** | Product_Description | Product_Desc | - | - | Product_Desc |
| **生产订单** | ProductionOrder | ProductionOrder | - | - | ProductionOrder |
| **报工人** | TrackOut_User | TrackOutOperator | TrackOutOperator | - | TrackOutOperator |
| **开工人** | - | - | TrackInOperator | - | - |
| **单件机器工时(秒)** | - | - | - | EH_machine | EH_machine |
| **单件人工工时(秒)** | - | - | - | EH_labor | EH_labor |
| **基本数量** | - | - | - | Quantity | - |
| **是否换型** | - | - | - | - | IsSetup *(计算)* |
| **调试时间(h)** | - | - | - | SetupTime | SetupTime |
| **OEE系数** | - | - | - | OEE | OEE |
| **LT(d)** | - | - | - | - | LT(d) *(计算)* |
| **PT(d)** | - | - | - | - | PT(d) *(计算)* |
| **ST(d)** | - | - | - | - | ST(d) *(计算)* |

---

## 关联键说明

| 关联关系 | 关联键 | 说明 |
|---------|--------|------|
| raw_mes ↔ raw_sfc | `BatchNumber` + `Operation` | 获取 TrackInTime 用于 LT 计算 |
| raw_mes ↔ raw_sap_routing | `CFN` + `Operation` + `Group` | 获取 EH_machine/EH_labor 用于 ST 计算 |

---

## MES ↔ SAP 字段对照

| 业务概念 | MES字段 | SAP字段 | 样例值 |
|---------|--------|--------|--------|
| 工厂代码 | ERPCode (Plant) | Plant | `1303`, `9997` |
| 物料号 | Product_Name (ProductNumber) | Material Number (ProductNumber) | `M463501B834` |
| 图纸号 | DrawingProductNumber_Value (CFN) | CFN | `36136000-02` |
| 工艺组 | LogicalFlowPath→Group | Group | `50267539` |

---

## 数据流向

```
MES Excel                    SFC Excel                SAP Routing Excel
    │                            │                           │
    ▼                            ▼                           ▼
┌─────────┐                ┌─────────┐               ┌───────────────┐
│ raw_mes │◄───────────────│ raw_sfc │               │raw_sap_routing│
│         │ BatchNumber    │         │               │               │
│         │ + Operation    │         │               │               │
│         │◄───────────────┼─────────┼───────────────│               │
│         │ CFN+Operation  │         │               │               │
│         │ +Group         │         │               │               │
└────┬────┘                └─────────┘               └───────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        v_mes_metrics                                 │
│  - 关联 SFC 获取 TrackInTime, ScrapQty                              │
│  - 关联 SAP 获取 EH_machine, EH_labor, SetupTime, OEE               │
│  - 计算 IsSetup, PreviousBatchEndTime                               │
│  - 计算 LT(d), PT(d), ST(d)                                         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 相关文档

- [字段映射表](field-mapping.md)
- [计算逻辑说明](calculation-logic.md)
- [数据架构](../../developer/architecture/data-architecture.md)
