let
    SiteUrl = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",

    // 先精确定位到目标文件夹，避免扫描整个站点
    Root = SharePoint.Contents(SiteUrl, [ApiVersion = 15]),
    Lib  = Root{[Name = "Shared Documents"]}[Content],
    L1   = Lib{[Name = "General"]}[Content],
    L2   = L1{[Name = "POWER BI 数据源 V2"]}[Content],
    L3   = L2{[Name = "70-SFC导出数据"]}[Content],
    TargetFolder = L3{[Name = "90-SFC数据归档"]}[Content],

    // 验证目标文件夹是否存在
    TargetFolderExists = if Table.IsEmpty(TargetFolder) then error "目标文件夹 '90-SFC数据归档' 不存在" else TargetFolder,

    // 获取目标文件夹中的所有文件（包括子文件夹）
    AllFiles = let
        // 获取当前文件夹中的所有文件
        CurrentFiles = Table.SelectRows(TargetFolderExists, each [Attributes][Kind] = "File"),
        
        // 获取当前文件夹中的子文件夹
        SubFolders = Table.SelectRows(TargetFolderExists, each [Attributes][Kind] = "Folder"),
        
        // 处理每个子文件夹中的文件
        SubFolderFiles = if Table.IsEmpty(SubFolders) 
            then #table({"Name", "Content", "Extension", "Date modified", "Folder Path"}, {})
            else Table.Combine(
                Table.AddColumn(SubFolders, "SubFiles", each 
                    let
                        SubFolderContent = [Content],
                        SubFiles = Table.SelectRows(SubFolderContent, each [Attributes][Kind] = "File")
                    in
                        SubFiles
                )[SubFiles]
            ),
        
        // 合并当前文件夹和子文件夹的所有文件
        Result = Table.Combine({CurrentFiles, SubFolderFiles})
    in
        Result,

    // 筛选以"EM-"开头的parquet文件
    EMFiles = Table.SelectRows(AllFiles, each 
        Text.Lower([Extension]) = ".parquet" 
        and Text.StartsWith([Name], "EM-")),

    // 按修改时间排序，获取最近100个文件
    SortedFiles = Table.Sort(EMFiles, {{"Date modified", Order.Descending}}),
    Top100Files = Table.FirstN(SortedFiles, 100),

    // 缓存文件清单，避免多次枚举
    FilesBuffered = Table.Buffer(Top100Files),

    // 读取每个 parquet
    WithTables = Table.AddColumn(FilesBuffered, "Table", each Parquet.Document([Content])),

    // 动态展开
    ColumnsToExpand = if Table.IsEmpty(WithTables) then {} else Table.ColumnNames(WithTables{0}[Table]),
    Expanded = if List.IsEmpty(ColumnsToExpand)
        then WithTables
        else Table.ExpandTableColumn(WithTables, "Table", ColumnsToExpand),

    // 添加年份列
    WithYear = Table.AddColumn(Expanded, "Year", each Date.Year(Date.From([Date modified])), Int64.Type),

    Result = Table.RemoveColumns(WithYear, {"Content"})
in
    Result