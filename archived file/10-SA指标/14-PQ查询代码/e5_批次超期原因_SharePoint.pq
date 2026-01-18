let
    // 连接到SharePoint站点
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/CZOPS871",
    
    // 连接到SharePoint文档库
    Source = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    
    // 筛选到特定文件夹路径
    FilteredFolder = Table.SelectRows(Source, each Text.Contains([Folder Path], "General") and Text.Contains([Name], "SA不达标原因分析")),
    
    // 获取文件内容
    FileContent = Table.AddColumn(FilteredFolder, "Content", each Excel.Workbook([Content], null, true)),
    
    // 展开Excel工作簿内容
    ExpandedContent = Table.ExpandTableColumn(FileContent, "Content", {"Name", "Data", "Item", "Kind", "Hidden"}, {"SheetName", "SheetData", "Item", "Kind", "Hidden"}),
    
    // 筛选到Export工作表
    FilteredSheet = Table.SelectRows(ExpandedContent, each [Item] = "Export" and [Kind] = "Sheet"),
    
    // 获取工作表数据
    SheetData = FilteredSheet{0}[SheetData],
    
    // 提升标题行
    Headers = Table.PromoteHeaders(SheetData, [PromoteAllScalars=true]),
    
    // 筛选非空原因分类的行
    FilteredRows = Table.SelectRows(Headers, each ([原因分类] <> null)),
    
    // 选择需要的列
    RemovedOtherColumns = Table.SelectColumns(FilteredRows,{"BatchNumber", "导出日期", "CFN", "Operation", "原因分析", "原因分类"})
in
    RemovedOtherColumns
