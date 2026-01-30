// ============================================
// 查询名称: Operation Mapping (工序映射�?
// 数据�? dim_operation_mapping
// 说明: 复制此代码到 Power BI 高级编辑�?
// ============================================

let
    // 数据库路�?- 请根据实际路径修�?
    DbPath = "C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central\data_pipelines\database\mddap_v2.db",
    
    // 连接 SQLite 数据�?
    Source = Odbc.Query(
        "Driver={SQLite3 ODBC Driver};Database=" & DbPath,
        "SELECT 
            operation_name,
            standard_routing,
            area,
            lead_time,
            erp_code
        FROM dim_operation_mapping"
    ),
    
    // 转换数据类型
    ChangedType = Table.TransformColumnTypes(Source, {
        {"lead_time", type number}
    })
in
    ChangedType
