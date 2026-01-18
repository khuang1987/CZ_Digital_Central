let
    // ============================================
    // MES批次报工记录 - 从Parquet文件读取
    // ============================================
    // 说明：此查询直接从ETL处理后的Parquet文件读取数据
    // 所有计算（LT、PT、ST、DueTime、Weekend、CompletionStatus等）已在ETL中完成
    // 文件路径：publish/MES_处理后数据_latest.parquet
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
        Text.StartsWith([Name], "MES_处理后数据") and
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
    Source = if FileContent <> null then Parquet.Document(FileContent) else #table({"BatchNumber", "CFN", "Operation"}, {}),
    
    // 数据类型转换（根据Parquet文件的实际字段动态转换）
    // 注意：如果某些字段不存在，Power Query会自动跳过
    TypePairsFull = {
        // 时间字段
        {"TrackInTime", type datetime},
        {"TrackOutTime", type datetime},
        {"EnterStepTime", type datetime},
        {"PreviousBatchEndTime", type datetime},
        {"Checkin_SFC", type datetime},
        {"DueTime", type datetime},
        // 日期字段
        {"TrackOutDate", type date},
        {"Date created", type date},
        // 数值字段
        {"StepInQuantity", Int64.Type},
        {"TrackOutQuantity", Int64.Type},
        {"LT(d)", type number},
        {"PT(d)", type number},
        {"ST(d)", type number},
        {"Weekend(d)", type number},
        {"Tolerance(h)", type number},
        {"OEE", type number},
        {"Setup Time (h)", type number},
        {"Effective Time (h)", type number},
        {"Machine(#)", Int64.Type},
        // 文本字段
        {"BatchNumber", type text},
        {"CFN", type text},
        {"Operation", type text},
        {"Operation description", type text},
        {"Group", type text},
        {"machine", type text},
        {"CompletionStatus", type text},
        {"Setup", type text},
        {"ProductionOrder", type text},
        {"ResourceCode", type text},
        {"ResourceDescription", type text},
        {"ProductionArea", type text},
        {"VSM", type text},
        {"PlantCode", type text},
        {"Name", type text}
    },
    ExistingTypePairs = List.Select(TypePairsFull, each List.Contains(Table.ColumnNames(Source), _{0})),
    #"Changed Type" = Table.TransformColumnTypes(Source, ExistingTypePairs),
    
    // 获取所有实际存在的列名（避免列重排时引用不存在的列）
    AllColumns = Table.ColumnNames(#"Changed Type"),
    
    // 定义期望的列顺序（只包含实际存在的列）
    DesiredColumns = {"BatchNumber", "CFN", "ProductionOrder", "Operation", "Operation description", 
        "Group", "machine", "ResourceCode", "ResourceDescription", "ProductionArea",
        "StepInQuantity", "TrackOutQuantity",
        "EnterStepTime", "Checkin_SFC", "PreviousBatchEndTime", "TrackInTime", "TrackOutTime", "TrackOutDate", 
        "DueTime", "Weekend(d)", "CompletionStatus", "Tolerance(h)",
        "LT(d)", "PT(d)", "ST(d)",
        "Setup", "Setup Time (h)", "OEE", "Effective Time (h)", "Machine(#)",
        "Date created", "PlantCode", "VSM", "Name"},
    
    // 筛选出实际存在的列
    ExistingColumns = List.Select(DesiredColumns, each List.Contains(AllColumns, _)),
    
    // 添加其他未列出的列（如果有）
    OtherColumns = List.Difference(AllColumns, ExistingColumns),
    FinalColumnOrder = ExistingColumns & OtherColumns,
    
    // 列重排（只包含实际存在的字段）
    #"Reordered Columns" = Table.ReorderColumns(#"Changed Type", FinalColumnOrder),
    
    // 最终输出
    Result = #"Reordered Columns"
in
    Result
