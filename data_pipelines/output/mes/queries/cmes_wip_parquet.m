let
    // ============================================
    // CMES WIP - 从每日 Parquet 文件读取（最近7天）
    // ============================================
    // 文件路径：A1_ETL_Output/02_CURATED_PARTITIONED/cmes_wip/<CMES_WIP_前缀><YYYYMMDD>.parquet
    // 说明：为避免网关依赖，使用 SharePoint.Files（不要直接用本地 C:\ 路径）

    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    FolderKey = "02_curated_partitioned/cmes_wip",

    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    WithNormPath = Table.AddColumn(SharePointFiles, "_folder_path_norm", each Text.Lower(Text.Replace([Folder Path], "%20", " ")), type text),
    FolderFiles = Table.SelectRows(WithNormPath, each Text.Contains([_folder_path_norm], FolderKey)),

    ParquetFiles = Table.SelectRows(
        FolderFiles,
        each
            [Extension] = ".parquet"
            and (
                let
                    digits = Text.Select([Name], {"0".."9"}),
                    ymd = if Text.Length(digits) >= 8 then Text.End(digits, 8) else null,
                    y = if ymd <> null then try Number.FromText(Text.Start(ymd, 4)) otherwise null else null,
                    isOk = y <> null and y >= 2000 and y <= 2100
                in
                    isOk
            )
    ),

    WithSnapDate = Table.AddColumn(
        ParquetFiles,
        "snapshot_date",
        each
            let
                digits = Text.Select([Name], {"0".."9"}),
                ymd = if Text.Length(digits) >= 8 then Text.End(digits, 8) else null,
                y = if ymd <> null then Number.FromText(Text.Start(ymd, 4)) else null,
                m = if ymd <> null then Number.FromText(Text.Middle(ymd, 4, 2)) else null,
                d = if ymd <> null then Number.FromText(Text.End(ymd, 2)) else null
            in
                if y <> null and m <> null and d <> null then #date(y, m, d) else null,
        type date
    ),

    LatestDate = if Table.RowCount(WithSnapDate) > 0 then List.Max(WithSnapDate[snapshot_date]) else null,
    FilteredRecent = if LatestDate <> null then Table.SelectRows(WithSnapDate, each [snapshot_date] >= Date.AddDays(LatestDate, -6)) else WithSnapDate,

    WithData = Table.AddColumn(FilteredRecent, "Data", each Parquet.Document(Binary.Buffer([Content]))),
    Combined = if Table.RowCount(FilteredRecent) > 0 then Table.Combine(WithData[Data]) else #table({}, {}),
    Source = Combined,

    // 类型：尽量只对关键字段做类型转换（其余字段保持自动推断）
    TypePairsFull = {
        {"snapshot_date", type date},
        {"source_file", type text},
        {"ERPCode", Int64.Type},
        {"ProductionOrder", Int64.Type},
        {"MaterialQty", type number},
        {"OrderQty", type number},
        {"TrackInDate", type datetime},
        {"TrackOutDate", type datetime},
        {"DateEnteredStep", type datetime},
        {"LastProcessedTime", type datetime}
    },
    ExistingTypePairs = List.Select(TypePairsFull, each List.Contains(Table.ColumnNames(Source), _{0})),
    #"Changed Type" = Table.TransformColumnTypes(Source, ExistingTypePairs),
    Result = #"Changed Type"

in
    Result
