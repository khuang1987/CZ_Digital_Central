let
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    PartitionedFolderKey = "a1_etl_output/02_curated_partitioned/planner_tasks/",

    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    WithNormPath = Table.AddColumn(SharePointFiles, "_folder_path_norm", each Text.Lower(Text.Replace([Folder Path], "%20", " ")), type text),
    FolderFiles = Table.SelectRows(WithNormPath, each Text.Contains([_folder_path_norm], PartitionedFolderKey)),

    ParquetFiles = Table.SelectRows(
        FolderFiles,
        each
            [Extension] = ".parquet"
            and Text.StartsWith(Text.Lower([Name]), "planner_tasks_")
            and (
                let
                    token = try Text.BetweenDelimiters(Text.Lower([Name]), "planner_tasks_", ".parquet") otherwise null,
                    yyyy = if token <> null and Text.Length(token) >= 4 then Text.Start(token, 4) else null,
                    isYearOk = try (Number.FromText(yyyy) >= 2000 and Number.FromText(yyyy) <= 2100) otherwise false,
                    isMonthly = token <> null and Text.Length(token) = 6 and (try Number.FromText(token) otherwise null) <> null
                in
                    isYearOk and isMonthly
            )
    ),

    WithData = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document(Binary.Buffer([Content]))),
    Combined =
        if Table.RowCount(ParquetFiles) > 0 then
            Table.Combine(WithData[Data])
        else
            #table(
                {
                    "id",
                    "TaskId",
                    "TaskName",
                    "BucketName",
                    "Status",
                    "Priority",
                    "Assignees",
                    "CreatedBy",
                    "CreatedDate",
                    "StartDate",
                    "DueDate",
                    "IsRecurring",
                    "IsLate",
                    "CompletedDate",
                    "CompletedBy",
                    "CompletedChecklistItemCount",
                    "ChecklistItemCount",
                    "Labels",
                    "Description",
                    "SourceFile",
                    "TeamName",
                    "ImportedAt",
                    "IsDeleted",
                    "DeletedAt",
                    "LastSeenAt",
                    "LastSeenSourceMtime",
                    "LastSeenSourceFile",
                    "updated_at"
                },
                {}
            ),

    Source = Combined,
    ToLogical = (v as any) as nullable logical =>
        if v is null then
            null
        else if Value.Is(v, type logical) then
            v
        else if Value.Is(v, type number) then
            v <> 0
        else if Value.Is(v, type text) then
            let
                t = Text.Lower(Text.Trim(v))
            in
                if t = "true" or t = "yes" or t = "y" or t = "1" then
                    true
                else if t = "false" or t = "no" or t = "n" or t = "0" or t = "" then
                    false
                else
                    null
        else
            null,
    BoolCols = {"IsRecurring", "IsLate", "IsDeleted"},
    ExistingBoolCols = List.Intersect({BoolCols, Table.ColumnNames(Source)}),
    WithNormalizedBools =
        if List.Count(ExistingBoolCols) > 0 then
            Table.TransformColumns(
                Source,
                List.Transform(ExistingBoolCols, each {_, (x) => ToLogical(x), type nullable logical})
            )
        else
            Source,
    TypePairsFull = {
        {"id", Int64.Type},
        {"TaskId", type text},
        {"TaskName", type text},
        {"BucketName", type text},
        {"Status", type text},
        {"Priority", type text},
        {"Assignees", type text},
        {"CreatedBy", type text},
        {"CreatedDate", type date},
        {"StartDate", type date},
        {"DueDate", type date},
        {"IsRecurring", type logical},
        {"IsLate", type logical},
        {"CompletedDate", type date},
        {"CompletedBy", type text},
        {"CompletedChecklistItemCount", Int64.Type},
        {"ChecklistItemCount", Int64.Type},
        {"Labels", type text},
        {"Description", type text},
        {"SourceFile", type text},
        {"TeamName", type text},
        {"ImportedAt", type datetime},
        {"IsDeleted", type logical},
        {"DeletedAt", type datetime},
        {"LastSeenAt", type datetime},
        {"LastSeenSourceMtime", type text},
        {"LastSeenSourceFile", type text},
        {"updated_at", type datetime}
    },
    ExistingTypePairs = List.Select(TypePairsFull, each List.Contains(Table.ColumnNames(WithNormalizedBools), _{0})),
    #"Changed Type" = Table.TransformColumnTypes(WithNormalizedBools, ExistingTypePairs),
    Result = #"Changed Type"
in
    Result