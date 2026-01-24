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
    })
in
    ChangeType