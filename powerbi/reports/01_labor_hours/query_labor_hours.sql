-- ============================================
-- 查询名称: Labor Hours (工时数据)
-- 报表: 01_labor_hours
-- 数据源: raw_sap_labor_hours
-- 说明: SAP 人工工时明细数据
-- ============================================

SELECT 
    Plant,
    WorkCenter,
    WorkCenterDesc,
    CostCenter,
    CostCenterDesc,
    Material,
    MaterialDesc,
    MaterialType,
    MRPController,
    MRPControllerDesc,
    ProductionScheduler,
    ProductionSchedulerDesc,
    OrderNumber,
    OrderType,
    OrderTypeDesc,
    Operation,
    OperationDesc,
    PostingDate,
    EarnedLaborUnit,
    MachineTime,
    EarnedLaborTime,
    ActualQuantity,
    ActualScrapQty,
    TargetQuantity
FROM raw_sap_labor_hours
ORDER BY PostingDate DESC, OrderNumber, Operation;
