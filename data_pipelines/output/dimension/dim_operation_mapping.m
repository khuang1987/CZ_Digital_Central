let
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    StaticFolderKey = "01_curated_static",
    TargetFileName = "dim_operation_mapping.parquet",

    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    WithNormPath = Table.AddColumn(SharePointFiles, "_folder_path_norm", each Text.Lower(Text.Replace([Folder Path], "%20", " ")), type text),
    FolderFiles = Table.SelectRows(WithNormPath, each Text.Contains([_folder_path_norm], StaticFolderKey)),
    ParquetFiles = Table.SelectRows(
        FolderFiles,
        each [Extension] = ".parquet" and Text.Lower([Name]) = Text.Lower(TargetFileName)
    ),

    ContentBin = if Table.RowCount(ParquetFiles) > 0 then ParquetFiles{0}[Content] else null,
    Source = if ContentBin <> null then Parquet.Document(Binary.Buffer(ContentBin)) else #table({}, {}),

    TypePairsFull = {
        {"id", Int64.Type},
        {"operation_name", type text},
        {"standard_routing", type text},
        {"area", type text},
        {"lead_time", type number},
        {"erp_code", type text},
        {"created_at", type datetime}
    },
    ExistingTypePairs = List.Select(TypePairsFull, each List.Contains(Table.ColumnNames(Source), _{0})),
    #"Changed Type" = Table.TransformColumnTypes(Source, ExistingTypePairs),
    Result = #"Changed Type"
in
    Result
