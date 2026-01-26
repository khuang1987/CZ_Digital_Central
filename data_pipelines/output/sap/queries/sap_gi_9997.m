let
    // ============================================
    // SAP 9997 发料记录 - 从分区Parquet文件读取
    // ============================================
    // 文件路径：A1_ETL_Output/02_CURATED_PARTITIONED/sap_gi_9997/<YYYY>/sap_gi_9997_<YYYYMM>.parquet
    // 说明：为避免网关依赖，保持使用 SharePoint.Files（不要直接用本地 C:\ 路径）

    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    PartitionedFolderKey = "02_curated_partitioned/sap_gi_9997",

    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    WithNormPath = Table.AddColumn(SharePointFiles, "_folder_path_norm", each Text.Lower(Text.Replace([Folder Path], "%20", " ")), type text),
    FolderFiles = Table.SelectRows(WithNormPath, each Text.Contains([_folder_path_norm], PartitionedFolderKey)),

    // 只取分区文件：.../<YYYY>/sap_gi_9997_<YYYYMM>.parquet
    ParquetFiles = Table.SelectRows(
        FolderFiles,
        each
            [Extension] = ".parquet"
            and Text.StartsWith(Text.Lower([Name]), "sap_gi_9997_")
            and (
                let
                    token = try Text.BetweenDelimiters(Text.Lower([Name]), "sap_gi_9997_", ".parquet") otherwise null,
                    yyyy = if token <> null and Text.Length(token) >= 4 then Text.Start(token, 4) else null,
                    isYearOk = try (Number.FromText(yyyy) >= 2000 and Number.FromText(yyyy) <= 2100) otherwise false,
                    isMonthly = token <> null and Text.Length(token) = 6 and (try Number.FromText(token) otherwise null) <> null
                in
                    isYearOk and isMonthly
            )
    ),

    WithMonth = Table.AddColumn(
        ParquetFiles,
        "p_period",
        each
            let
                token = try Text.BetweenDelimiters(Text.Lower([Name]), "sap_gi_9997_", ".parquet") otherwise null,
                yyyy = if token <> null and Text.Length(token) >= 4 then Text.Start(token, 4) else null,
                mm = if token <> null and Text.Length(token) = 6 then Text.End(token, 2) else null
            in
                if yyyy <> null and mm <> null then yyyy & "-" & mm else null,
        type text
    ),

    // 全量刷新：加载所有月份分区
    FilteredFiles = WithMonth,

    WithData = Table.AddColumn(FilteredFiles, "Data", each Parquet.Document(Binary.Buffer([Content]))),
    Combined = if Table.RowCount(FilteredFiles) > 0 then Table.Combine(WithData[Data]) else #table({}, {}),
    Source = Combined,

    TypePairsFull = {
        {"id", Int64.Type},
        {"PostingDate", type datetime}, // Using datetime to be safe if it has 00:00:00
        {"DocumentDate", type datetime}, // Fix: Handle "YYYY-MM-DD HH:mm:ss"
        {"Material", type text},
        {"MaterialDesc", type text},
        {"Plant", Int64.Type}, // Fix: Integer
        {"StorageLocation", type text},
        {"MovementType", type text},
        {"Quantity", type number},
        {"Unit", type text},
        {"DocumentNumber", type text},
        {"DocumentItem", type text},
        {"Batch", type text},
        {"OrderNumber", type text},
        {"CostCenter", type text},
        {"source_file", type text},
        {"record_hash", type text},
        {"created_at", type datetime},
        {"updated_at", type datetime}
    },
    ExistingTypePairs = List.Select(TypePairsFull, each List.Contains(Table.ColumnNames(Source), _{0})),
    #"Changed Type" = Table.TransformColumnTypes(Source, ExistingTypePairs),
    Result = #"Changed Type"
in
    Result
