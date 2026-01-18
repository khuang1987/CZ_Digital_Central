let
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    CoreTablesFolderKey = "a1_etl_output/03_metadata/core_tables/",
    TargetFileName = "triggercaseregistry.parquet",

    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    WithNormPath = Table.AddColumn(SharePointFiles, "_folder_path_norm", each Text.Lower(Text.Replace([Folder Path], "%20", " ")), type text),
    FolderFiles = Table.SelectRows(WithNormPath, each Text.Contains([_folder_path_norm], CoreTablesFolderKey)),
    ParquetFiles = Table.SelectRows(
        FolderFiles,
        each [Extension] = ".parquet" and Text.Lower([Name]) = TargetFileName
    ),

    ContentBin = if Table.RowCount(ParquetFiles) > 0 then ParquetFiles{0}[Content] else null,
    Source = if ContentBin <> null then Parquet.Document(Binary.Buffer(ContentBin)) else #table({}, {}),

    Result = Source
in
    Result
