let
    // ============================================
    // SAP标准时间表 - 从Parquet文件读取
    // ============================================
    // 说明：此查询直接从ETL处理后的Parquet文件读取标准时间数据
    // 包含Routing信息、标准时间、Setup时间、OEE等
    // 文件路径：publish/SAP_Routing_latest.parquet
    // ============================================
    
    // 从SharePoint读取Parquet文件
    // SharePoint站点和路径
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    SharePointFolder = "Shared Documents/General/POWER BI 数据源 V2/30-MES导出数据",
    
    // 获取SharePoint文件夹中的所有文件（包括子文件夹）
    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    
    // 先筛选包含"publish"的文件夹路径，再在这些文件夹中查找目标文件
    PublishFolderFiles = Table.SelectRows(SharePointFiles, each
        Text.Contains([Folder Path], SharePointFolder) and
        Text.Contains(Text.Lower([Folder Path]), "publish")
    ),
    TargetFiles = Table.SelectRows(PublishFolderFiles, each
        Text.StartsWith([Name], "SAP_Routing") and
        [Extension] = ".parquet"
    ),
    
    // 按修改时间排序，取最新的文件
    SortedFiles = if Table.RowCount(TargetFiles) > 0 
        then Table.Sort(TargetFiles, {{"Date modified", Order.Descending}})
        else null,
    
    // 获取最新文件内容
    FileContent = if SortedFiles <> null and Table.RowCount(SortedFiles) > 0 
        then SortedFiles[Content]{0} 
        else null,
    
    // 读取Parquet文件
    Source = if FileContent <> null then Parquet.Document(Binary.Buffer(FileContent)) else #table({"CFN", "Operation", "Group"}, {}),
    
    // 数据类型转换（根据Parquet文件的实际字段动态转换）
    // 注意：如果某些字段不存在，Power Query会自动跳过
    TypePairsFull = {
        // 文本字段
        {"CFN", type text},
        {"Material Description", type text},
        {"Operation", type text},
        {"Operation Description", type text},
        {"Group", type text},
        {"Work Center", type text},
        {"Work Center Description", type text},
        {"Plant", type text},
        {"VSM", type text},
        {"Production Area", type text},
        // 数值字段
        {"EH_machine(s)", type number},
        {"EH_labor(s)", type number},
        {"Setup Time (h)", type number},
        {"OEE", type number},
        {"Effective Time (h)", type number},
        {"Machine(#)", Int64.Type},
        {"Standard Time (h)", type number},
        {"Unit of Measure", type text},
        {"Control Key", type text},
        {"Standard Text Key", type text},
        {"Standard Text", type text},
        // 日期字段
        {"Valid From", type date},
        {"Valid To", type date},
        {"Date created", type date}
    },
    ExistingTypePairs = List.Select(TypePairsFull, each List.Contains(Table.ColumnNames(Source), _{0})),
    #"Changed Type" = Table.TransformColumnTypes(Source, ExistingTypePairs),
    
    // 获取所有实际存在的列名（避免列重排时引用不存在的列）
    AllColumns = Table.ColumnNames(#"Changed Type"),
    
    // 定义期望的列顺序（只包含实际存在的列）
    // 标准时间表字段顺序，按业务逻辑排列
    DesiredColumns = {"CFN", "Material Description", "Operation", "Operation Description", 
        "Group", "Work Center", "Work Center Description", "Plant", "VSM", "Production Area",
        "EH_machine(s)", "EH_labor(s)", "Setup Time (h)", "OEE", "Effective Time (h)", 
        "Machine(#)", "Standard Time (h)", "Unit of Measure", "Control Key", 
        "Standard Text Key", "Standard Text", "Valid From", "Valid To", "Date created"},
    
    // 筛选出实际存在的列
    ExistingColumns = List.Select(DesiredColumns, each List.Contains(AllColumns, _)),
    
    // 添加其他未列出的列（如果有）
    OtherColumns = List.Difference(AllColumns, ExistingColumns),
    FinalColumnOrder = ExistingColumns & OtherColumns,
    
    // 列重排（只包含实际存在的字段）
    #"Reordered Columns" = Table.ReorderColumns(#"Changed Type", FinalColumnOrder),
    
    // 数据清洗：移除空行
    #"Removed Blank Rows" = Table.SelectRows(#"Reordered Columns", each not (
        Record.FieldValues(_) = List.Repeat({null}, List.Count(Record.FieldValues(_)))
    )),
    
    // 数据清洗：确保关键字段不为空
    #"Filtered Rows" = Table.SelectRows(#"Removed Blank Rows", each (
        [CFN] <> null and [CFN] <> "" and 
        [Operation] <> null and [Operation] <> ""
    )),
    
    // 最终输出
    Result = #"Filtered Rows"
in
    Result
