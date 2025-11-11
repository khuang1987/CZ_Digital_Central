let
    // ============================================
    // SFC批次报工记录 - 从Parquet文件读取
    // ============================================
    // 说明：此查询直接从ETL处理后的Parquet文件读取SFC数据
    // 所有计算（LT、PT、ST、DueTime、Weekend、CompletionStatus等）已在ETL中完成
    // 文件路径：publish/SFC_处理后数据_latest.parquet
    // ============================================
    
    // 从SharePoint读取Parquet文件
    // SharePoint站点和路径
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    SharePointFolder = "Shared Documents/General/POWER BI 数据源 V2/30-MES导出数据",
    
    // 获取SharePoint文件夹中的所有文件（包括子文件夹）
    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    
    // 先筛选包含“publish”的文件夹路径，再在这些文件夹中查找目标文件
    PublishFolderFiles = Table.SelectRows(SharePointFiles, each
        Text.Contains([Folder Path], SharePointFolder) and
        Text.Contains(Text.Lower([Folder Path]), "publish")
    ),
    TargetFiles = Table.SelectRows(PublishFolderFiles, each
        Text.StartsWith([Name], "SFC_处理后数据") and
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

    // 兼容字段：某些历史文件中时间列可能为"CheckInTime"，统一重命名为"Checkin_SFC"
    SourceCols = Table.ColumnNames(Source),
    SourceRenamed = if List.Contains(SourceCols, "CheckInTime") and not List.Contains(SourceCols, "Checkin_SFC")
        then Table.RenameColumns(Source, {{"CheckInTime", "Checkin_SFC"}})
        else Source,
    
    // 数据类型转换（根据Parquet文件的实际字段动态转换）
    // 注意：SFC数据现在包含EnterStepTime和PreviousBatchEndTime字段
    // 使用Table.TransformColumnTypes会自动跳过不存在的字段
    #"Changed Type" = Table.TransformColumnTypes(SourceRenamed, {
        // 时间字段
        {"Checkin_SFC", type datetime},
        {"TrackOutTime", type datetime},
        {"EnterStepTime", type datetime},
        {"PreviousBatchEndTime", type datetime},
        {"DueTime", type datetime},
        // 数值字段
        {"TrackOutQuantity", Int64.Type},
        {"ScrapQuantity", Int64.Type},
        {"LT(d)", type number},
        {"PT(d)", type number},
        {"ST(d)", type number},
        {"Weekend(d)", type number},
        {"Tolerance(h)", type number},
        {"OEE", type number},
        {"Setup Time (h)", type number},
        {"EH_machine(s)", type number},
        {"EH_labor(s)", type number},
        {"Machine(#)", Int64.Type},
        {"machine", Int64.Type},  // machine现在是整数类型
        // 文本字段
        {"BatchNumber", type text},
        {"CFN", type text},
        {"Operation", type text},
        {"Operation description", type text},
        {"CompletionStatus", type text},
        {"ProductType", type text},
        {"TrackOut_User", type text},
        {"CheckIn_User", type text}
    }),
    
    // 获取所有实际存在的列名（避免列重排时引用不存在的列）
    AllColumns = Table.ColumnNames(#"Changed Type"),
    
    // 定义期望的列顺序（只包含实际存在的列）
    // 注意：SFC数据没有Group字段
    DesiredColumns = {"BatchNumber", "CFN", "Operation", "Operation description", 
        "machine",
        "TrackOutQuantity", "ScrapQuantity",
        "Checkin_SFC", "EnterStepTime", "PreviousBatchEndTime", "TrackOutTime", 
        "DueTime", "Weekend(d)", "CompletionStatus", "Tolerance(h)",
        "LT(d)", "PT(d)", "ST(d)",
        "Setup Time (h)", "OEE", "EH_machine(s)", "EH_labor(s)", "Machine(#)",
        "ProductType", "TrackOut_User", "CheckIn_User"},
    
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