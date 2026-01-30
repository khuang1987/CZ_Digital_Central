// ============================================
// 查询名称: Calendar (日历�?
// 数据�? dim_calendar
// 说明: 复制此代码到 Power BI 高级编辑�?
// ============================================

let
    // 数据库路�?- 请根据实际路径修�?
    DbPath = "C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\C_code\CZ_Digital_Central\data_pipelines\database\mddap_v2.db",
    
    // 连接 SQLite 数据�?
    Source = Odbc.Query(
        "Driver={SQLite3 ODBC Driver};Database=" & DbPath,
        "SELECT 
            date,
            weekday,
            weekday_cn,
            calendar_week,
            fiscal_week,
            fiscal_week_label,
            fiscal_month,
            fiscal_quarter,
            fiscal_year,
            fiscal_month_short,
            is_workday,
            holiday_name
        FROM dim_calendar"
    ),
    
    // 转换数据类型
    ChangedType = Table.TransformColumnTypes(Source, {
        {"date", type date},
        {"calendar_week", Int64.Type},
        {"fiscal_week", Int64.Type},
        {"is_workday", Int64.Type}
    })
in
    ChangedType
