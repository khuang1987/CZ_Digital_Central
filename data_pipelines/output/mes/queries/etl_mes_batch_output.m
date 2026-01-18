let
    // ============================================
    // MES批次报工记录 - 从Parquet文件读取
    // ============================================
    // 说明：此查询直接从ETL处理后的Parquet文件读取数据
    // 所有计算（LT、PT、ST、DueTime、Weekend、CompletionStatus等）已在ETL中完成
    // 文件路径：A1_ETL_Output/02_CURATED_PARTITIONED/mes_batch_report/
    // ============================================

    // 从SharePoint读取分区Parquet文件 (A1_ETL_Output)
    // 说明：为避免网关依赖，保持使用 SharePoint.Files（不要直接用本地 C:\ 路径）
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    PartitionedFolderKey = "a1_etl_output/02_curated_partitioned/mes_batch_report/",

    // 获取SharePoint文件夹中的所有文件（包括子文件夹）
    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    WithNormPath = Table.AddColumn(SharePointFiles, "_folder_path_norm", each Text.Lower(Text.Replace([Folder Path], "%20", " ")), type text),
    FolderFiles = Table.SelectRows(WithNormPath, each Text.Contains([_folder_path_norm], PartitionedFolderKey)),

    // 只取分区文件：.../<YYYY>/mes_metrics_<YYYYMM>.parquet 或 .../<YYYY>/mes_metrics_<YYYYQn>.parquet
    ParquetFiles = Table.SelectRows(
        FolderFiles,
        each
            [Extension] = ".parquet"
            and Text.StartsWith(Text.Lower([Name]), "mes_metrics_")
            and (
                let
                    token = try Text.BetweenDelimiters(Text.Lower([Name]), "mes_metrics_", ".parquet") otherwise null,
                    yyyy = if token <> null and Text.Length(token) >= 4 then Text.Start(token, 4) else null,
                    isYearOk = try (Number.FromText(yyyy) >= 2000 and Number.FromText(yyyy) <= 2100) otherwise false,
                    isMonthly = token <> null and Text.Length(token) = 6 and (try Number.FromText(token) otherwise null) <> null,
                    isQuarterly = token <> null and Text.Length(token) = 6 and Text.At(token, 4) = "q" and List.Contains({"1","2","3","4"}, Text.End(token, 1))
                in
                    isYearOk and (isMonthly or isQuarterly)
            )
    ),
    WithMonth = Table.AddColumn(
        ParquetFiles,
        "p_period",
        each
            let
                token = try Text.BetweenDelimiters(Text.Lower([Name]), "mes_metrics_", ".parquet") otherwise null,
                yyyy = if token <> null and Text.Length(token) >= 4 then Text.Start(token, 4) else null,
                isMonthly = token <> null and Text.Length(token) = 6 and (try Number.FromText(token) otherwise null) <> null,
                isQuarterly = token <> null and Text.Length(token) = 6 and Text.At(token, 4) = "q" and List.Contains({"1","2","3","4"}, Text.End(token, 1)),
                mm = if isMonthly then Text.End(token, 2) else null,
                q = if isQuarterly then Text.End(token, 1) else null
            in
                if yyyy <> null and mm <> null then yyyy & "-" & mm else if yyyy <> null and q <> null then yyyy & "-Q" & q else null,
        type text
    ),

    // 全量刷新：加载所有月份分区
    FilteredFiles = WithMonth,

    // 读取并合并 Parquet
    WithData = Table.AddColumn(FilteredFiles, "Data", each Parquet.Document(Binary.Buffer([Content]))),
    Combined = if Table.RowCount(FilteredFiles) > 0 then Table.Combine(WithData[Data]) else #table({"BatchNumber", "CFN", "Operation"}, {}),
    Source = Combined,

    // 数据类型转换（根据Parquet文件的实际字段动态转换）
    // 注意：如果某些字段不存在，Power Query会自动跳过
    TypePairsFull = {
        // 时间字段
        {"TrackInTime", type datetime},
        {"TrackOutTime", type datetime},
        {"EnterStepTime", type datetime},
        {"Checkin_SFC", type datetime},
        {"PreviousBatchEndTime", type datetime},
        {"DueTime", type datetime},
        // 日期字段
        {"TrackOutDate", type date},
        {"Date created", type date},
        // 数值字段
        {"StepInQuantity", Int64.Type},
        {"TrackOutQuantity", Int64.Type},
        {"ScrapQty", type number},
        {"LT(d)", type number},
        {"PT(d)", type number},
        {"ST(d)", type number},
        {"PNW(d)", type number},
        {"LNW(d)", type number},
        {"Weekend(d)", type number},
        {"Tolerance(h)", type number},
        {"OEE", type number},
        {"Setup Time (h)", type number},
        {"Effective Time (h)", type number},
        {"EH_machine", type number},
        {"EH_labor", type number},
        {"unit_time", type number},
        {"Machine(#)", Int64.Type},
        {"id", Int64.Type},
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
        {"ProductNumber", type text},
        {"Product_Desc", type text},
        {"TrackOutOperator", type text},
        {"ResourceCode", type text},
        {"ResourceDescription", type text},
        {"ProductionArea", type text},
        {"VSM", type text},
        {"Plant", type text},
        {"PlantCode", type text},
        {"factory_name", type text},
        {"Name", type text}
    },
    ExistingTypePairs = List.Select(TypePairsFull, each List.Contains(Table.ColumnNames(Source), _{0})),
    #"Changed Type" = Table.TransformColumnTypes(Source, ExistingTypePairs),
    
    // 获取所有实际存在的列名（避免列重排时引用不存在的列）
    AllColumns = Table.ColumnNames(#"Changed Type"),
    
    // 定义期望的列顺序（只包含实际存在的列）
    DesiredColumns = {
        "id", "BatchNumber", "CFN", "ProductionOrder", "Operation", "Operation description", 
        "Group", "machine", "Machine(#)", "Plant", "factory_name", "VSM",
        "ProductNumber", "Product_Desc",
        "StepInQuantity", "TrackOutQuantity", "ScrapQty",
        "EnterStepTime", "Checkin_SFC", "TrackInTime", "TrackOutTime", "TrackOutDate", "PreviousBatchEndTime",
        "LT(d)", "PT(d)", "ST(d)", "PNW(d)", "LNW(d)", "CompletionStatus",
        "Setup", "Setup Time (h)", "OEE", "unit_time", "EH_machine", "EH_labor", "TrackOutOperator"
    },
    
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