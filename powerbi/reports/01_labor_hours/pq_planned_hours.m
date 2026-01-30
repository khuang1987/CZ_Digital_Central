// ============================================
// 查询名称: Planned Hours (计划工时数据)
// 数据�? planned_labor_hours
// 说明: 复制此代码到 Power BI 高级编辑�?
// ============================================

let
    // 数据库路�?- 请根据实际路径修�?
    DbPath = "C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central\data_pipelines\database\mddap_v2.db",
    
    // 连接 SQLite 数据�?
    Source = Odbc.Query(
        "Driver={SQLite3 ODBC Driver};Database=" & DbPath,
        "SELECT 
            plan_date,
            cz_planned_hours,
            kh_planned_hours,
            is_cz_workday,
            is_kh_workday,
            source_file,
            created_at,
            updated_at
        FROM planned_labor_hours"
    ),
    
    // 转换数据类型
    ChangedType = Table.TransformColumnTypes(Source, {
        {"plan_date", type date},
        {"cz_planned_hours", type number},
        {"kh_planned_hours", type number},
        {"is_cz_workday", type number},
        {"is_kh_workday", type number},
        {"source_file", type text},
        {"created_at", type datetime},
        {"updated_at", type datetime}
    })
in
    ChangedType
