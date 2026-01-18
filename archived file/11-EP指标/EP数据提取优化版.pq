// 优化后的EP数据提取 Power Query 脚本
// 1. 从SharePoint读取Excel数据
let
    // 读取Excel文件
    Source = Excel.Workbook(Web.Contents("https://medtronicemea.sharepoint.com/sites/GlobalManufacturing/Shared%20Documents/08%20-%20Strategy%20Governance%20and%20Execution%20Team/2.0%20Strategic%20Deployment/FY25%20Strategy/FY25%20Sites%20Execution%20Plan/Asia%20China%20Region-%20EP%20&%20ADP/Global%20Manufacturing_FY25%20Site%20Execution%20Plan%20-%20CZ%20Ops.xlsx"), null, true),
    // 选择目标Sheet
    FY25Sheet = Source{[Item="FY25 Site Execution Plan",Kind="Sheet"]}[Data],
    // 转换列类型，减少后续类型转换次数
    ChangedType = Table.TransformColumnTypes(FY25Sheet,{
        {"Column1", type any}, {"Column2", type text}, {"Column3", type text}, {"Column4", type any},
        {"Column5", type text}, {"Column6", type any}, {"Column7", type any}, {"Column8", type any},
        {"Column9", type any}, {"Column10", type any}, {"Column11", type any}, {"Column12", type any},
        {"Column13", type any}, {"Column14", type any}, {"Column15", type any}, {"Column16", type any},
        {"Column17", type any}, {"Column18", type any}, {"Column19", type any}, {"Column20", type any},
        {"Column21", type any}, {"Column22", type any}, {"Column23", type any}, {"Column24", type text},
        {"Column25", type text}, {"Column26", type text}, {"Column27", type text}, {"Column28", type text},
        {"Column29", type any}
    }),
    // 跳过前9行（表头前的无用行）
    RemovedTopRows = Table.Skip(ChangedType, 9),
    // 移除底部15行（表尾无用数据）
    RemovedBottomRows = Table.RemoveLastN(RemovedTopRows, 15),
    // 过滤掉不需要的行
    FilteredRows = Table.SelectRows(RemovedBottomRows, each ([Column2] <> "(Year 1: establish baseline)" and [Column2] <> "MPS Matruity Assessment Score")),
    // 提升表头
    PromotedHeaders = Table.PromoteHeaders(FilteredRows, [PromoteAllScalars=true]),
    // 向下填充KPI和Owner列，保证数据完整
    FilledDown = Table.FillDown(PromotedHeaders,{"KPIs and PIs Owned by Function#(lf)include unit of measure","Owner"}),
    // 只保留包含#的KPI行
    FilteredRows1 = Table.SelectRows(FilledDown, each Text.Contains([#"KPIs and PIs Owned by Function#(lf)include unit of measure"], "#")),
    // 再次转换类型，保证后续处理准确
    ChangedType1 = Table.TransformColumnTypes(FilteredRows1,{
        {"Column1", type any}, {"KPIs and PIs Owned by Function#(lf)include unit of measure", type text},
        {"Owner", type text}, {"FY24 COB", type any}, {"Column5", type text}, {"FY25 YTD", type any},
        {"MAY", type any}, {"JUN", type any}, {"JUL", type any}, {"Q1", type any}, {"AUG", type any},
        {"SEP", type any}, {"OCT", type any}, {"Q2", type any}, {"NOV", type any}, {"DEC", type any},
        {"JAN", type any}, {"Q3", type any}, {"FEB", type any}, {"MAR", type any}, {"APR", type any},
        {"Q4", type any}, {"FY25 YE", type any}, {"Comments", type text},
        {"If Red, Why?#(lf)i.e. root cause", type text},
        {"If Red, What is the mitigation plan?#(lf)Include Owner", type text},
        {" If Red, When will KPI be back to objective?", type text},
        {"Link to KPI Action Plan or A3 (A3 for Mfg)", type text}, {"Column29", type any}
    }),
    // 移除无用列
    RemovedColumns = Table.RemoveColumns(ChangedType1,{"Column1"}),
    // 透视数据，将月份列展开为行
    Unpivoted = Table.UnpivotOtherColumns(RemovedColumns, {"KPIs and PIs Owned by Function#(lf)include unit of measure", "Owner", "FY24 COB", "Column5"}, "Month", "Value"),
    // 重命名列，Value改为“值”
    RenamedColumns = Table.RenameColumns(Unpivoted,{{"Month", "Month"}, {"Value", "值"}}),
    // 直接根据Month生成每月15日的日期，无需查表，起始为2024年5月
    MonthList = {"MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC", "JAN", "FEB", "MAR", "APR"},
    AddDateColumn = Table.AddColumn(RenamedColumns, "日期", each 
        let
            idx = List.PositionOf(MonthList, [Month]),
            baseYear = if idx < 8 then 2024 else 2025,
            baseMonth = if idx < 8 then idx + 5 else idx - 7
        in
            if idx <> -1 then #date(baseYear, baseMonth, 15) else null,
        type date
    ),
    // 日期类型转换（冗余，保证类型）
    ChangedType2 = Table.TransformColumnTypes(AddDateColumn,{{"日期", type date}}),
    // 进一步重命名，便于理解
    RenamedColumns1 = Table.RenameColumns(ChangedType2,{{"Column5", "Type"}, {"KPIs and PIs Owned by Function#(lf)include unit of measure", "KPIs"}}),
    // 移除无用列
    RemovedColumns1 = Table.RemoveColumns(RenamedColumns1,{"FY24 COB"}),
    // 过滤掉不需要的月份和KPI
    FilteredRows2 = Table.SelectRows(RemovedColumns1, each ([Month] <> "Comments" and [Month] <> "FY25 YE" and [Month] <> "FY25 YTD" and [Month] <> "Q1" and [Month] <> "Q2" and [Month] <> "Q3" and [Month] <> "Q4") and ([KPIs] <> "NCMR Reduction - Mfg#(lf) (% / #)")),
    // "值"列转换为小数格式，提升后续分析效率
    ChangedType3 = Table.TransformColumnTypes(FilteredRows2,{{"值", type number}}),
    // 错误值替换为null，避免报错
    ReplacedErrors = Table.ReplaceErrorValues(ChangedType3, {{"值", null}}),
    // 合并FY26数据（如有），否则只返回ReplacedErrors
    AppendedQuery = 
        try Table.Combine({ReplacedErrors, #"FY26 Site EP #"}) 
        otherwise ReplacedErrors
in
    AppendedQuery
// 说明：
// 1. 优化了类型转换和无用行/列的处理顺序，提升执行效率。
// 2. 添加了详细中文注释，便于理解和维护。
// 3. 保持与原始逻辑一致，兼容FY26数据合并。 
// 4. 日期生成逻辑已简化，无需查表，直接生成每月15日。 