// ============================================
// 查询名称: Labor Hours (工时数据)
// 数据源: raw_sap_labor_hours
// 说明: 复制此代码到 Power BI 高级编辑器
// ============================================

let
    // 数据库路径 - 请根据实际路径修改
    DbPath = "C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\data_pipelines\database\mddap_v2.db",
    
    // 连接 SQLite 数据库
    Source = Odbc.Query(
        "Driver={SQLite3 ODBC Driver};Database=" & DbPath,
        "SELECT 
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
        FROM raw_sap_labor_hours"
    ),
    
    // 转换数据类型
    ChangedType = Table.TransformColumnTypes(Source, {
        {"PostingDate", type date},
        {"MachineTime", type number},
        {"EarnedLaborTime", type number},
        {"ActualQuantity", type number},
        {"ActualScrapQty", type number},
        {"TargetQuantity", type number}
    })
in
    ChangedType
