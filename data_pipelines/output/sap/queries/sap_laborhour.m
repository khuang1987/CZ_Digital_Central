let
    // 1. 定义 Parquet 数据源路径 (指向 02_CURATED_PARTITIONED/sap_labor_hours)
    SourcePath = "C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output\02_CURATED_PARTITIONED\sap_labor_hours",
    
    // 2. 获取文件夹下的所有文件
    Source = Folder.Files(SourcePath),
    
    // 3. 筛选 Parquet 文件 (忽略隐藏文件或临时文件)
    FilterParquet = Table.SelectRows(Source, each ([Extension] = ".parquet")),
    
    // 4. 读取 Parquet 内容
    ReadParquet = Table.AddColumn(FilterParquet, "Data", each Parquet.Document([Content])),
    
    // 5. 展开数据列 (动态获取列名)
    // 这里的列名是 Parquet 文件中的实际列名 (通常对应 SQL Schema: WorkCenter, Material 等)
    // 我们先取第一行的列名作为展开依据
    FirstRowData = ReadParquet{0}[Data],
    ColumnNames = Table.ColumnNames(FirstRowData),
    
    ExpandData = Table.ExpandTableColumn(ReadParquet, "Data", ColumnNames, ColumnNames),
    
    // 6. 删除元数据列
    RemoveMeta = Table.RemoveColumns(ExpandData, {"Content", "Name", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}),
    
    // 7. 重命名列 (Map SQL Schema -> Power BI Expected Schema)
    // SQL Name -> Old Report Name
    RenameColumns = Table.RenameColumns(RemoveMeta, {
        {"WorkCenter", "Work Center"},
        {"Material", "Material"}, 
        {"MaterialType", "Material type"},
        {"ProductionScheduler", "Production Scheduler"},
        {"OrderNumber", "Order"},
        {"Operation", "Operation"},
        {"OperationDesc", "Operation Description"},
        {"PostingDate", "Posting date"},
        {"EarnedLaborUnit", "Earned Labor Unit"},
        {"MachineTime", "Machine Time"},
        {"EarnedLaborTime", "Earned Labor Time"},
        {"ActualQuantity", "Actual Quantity"}
    }, MissingField.Ignore),
    
    // 8. 更改数据类型 (确保与旧脚本一致)
    ChangeType = Table.TransformColumnTypes(RenameColumns, {
        {"Posting date", type date},
        {"Earned Labor Time", type number},
        {"Machine Time", type number},
        {"Actual Quantity", type number},
        {"Order", Int64.Type},      // New Int64
        {"Operation", Int64.Type}   // New Int64
    }),

    // --- 整合的额外逻辑 (来自 SAP_整合数据原始代码.m) ---

    // 9. 去重
    DistinctRows = Table.Distinct(ChangeType),

    // 10. 提取 Work Center 分隔符后的文本
    ExtractWorkCenter = Table.TransformColumns(DistinctRows, {{"Work Center", each Text.AfterDelimiter(_, "/"), type text}}),

    // 11. 过滤 Posting date 大于 2024-04-27 的数据
    FilterDate = Table.SelectRows(ExtractWorkCenter, each [Posting date] > #date(2024, 4, 27)),

    // 12. 过滤 Work Center 不以 OSP- 开头的数据
    FilterOSP = Table.SelectRows(FilterDate, each not Text.StartsWith([Work Center], "OSP-")),

    // 13. 工时单位调整 (虽然 Python ETL 已处理，但保留逻辑以防万一)
    // Earned Labor Unit为S时，Earned Labor Time除以3600，否则保持原值
    AdjustLaborTime = Table.AddColumn(FilterOSP, "Earned Labor Time_New", each if [Earned Labor Unit] = "s" then [Earned Labor Time] / 3600 else [Earned Labor Time]),
    RemoveOldLabor = Table.RemoveColumns(AdjustLaborTime, {"Earned Labor Time"}),
    RenameLabor = Table.RenameColumns(RemoveOldLabor, {{"Earned Labor Time_New", "Earned Labor Time"}}),

    // 14. 规范化 Production Scheduler
    // (Consolidates logic: 1303/#->CIN, 9997/#->INS, then strict mapping)
    NormalizeScheduler = Table.AddColumn(RenameLabor, "Production Scheduler_规范", each
        let
            ps0 = if [Production Scheduler] = null then "" else Text.Upper(Text.Trim([Production Scheduler])),
            ps  = if ps0 = "1303/#" then "1303/CIN"
                  else if ps0 = "9997/#" then "9997/INS"
                  else ps0
        in
            if ps = "1303/CIM" then "1303/植入物"
            else if ps = "1303/CIN" then "1303/器械"
            else if ps = "9997/STR" then "9997/无菌"
            else if ps = "9997/INS" then "9997/器械"
            else if ps = "9997/AP" or ps = "9997/AP1" then "9997/有源"
            else "9997/植入物",
        type text
    ),

    // 15. Final Type Check
    FinalType = Table.TransformColumnTypes(NormalizeScheduler, {{"Earned Labor Time", type number}, {"Production Scheduler_规范", type text}})
in
    FinalType