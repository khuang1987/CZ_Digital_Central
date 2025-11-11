let
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]),
    
    // 筛选文件夹路径
    筛选的行 = Table.SelectRows(Source, each [Folder Path] = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction/Shared Documents/General/POWER BI 数据源 V2/40-SAP工时/" or Text.Contains([Folder Path], "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction/Shared Documents/General/POWER BI 数据源 KH/04-SAP工时_V2")),
    
    // 筛选EH_FY开头的文件
    筛选的行2 = Table.SelectRows(筛选的行, each Text.StartsWith([Name], "EH_FY")),
    
    // 加载Excel工作簿
    已添加自定义 = Table.AddColumn(筛选的行2, "自定义", each Excel.Workbook([Content],true)),
    展开自定义 = Table.ExpandTableColumn(已添加自定义, "自定义", {"Name", "Data", "Item", "Kind", "Hidden"}, {"Name.1", "Data", "Item", "Kind", "Hidden"}),
    筛选的行1 = Table.SelectRows(展开自定义, each ([Kind] = "Sheet") and ([Name] <> "EH_FY26.xlsx")),
    
    // 清理不需要的列
    删除的列 = Table.RemoveColumns(筛选的行1,{"Name","Name.1","Content", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path", "Item", "Kind", "Hidden"}),
    
    // 展开所有Data列
    展开Data = Table.ExpandTableColumn(删除的列, "Data", 
        {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "EA", "EA_1", "EA_2"}, 
        {"Column1", "Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27", "EA", "EA_1", "EA_2"}
    ),
    
    删除不需要列 = Table.RemoveColumns(展开Data,{"Column1", "Column3", "Column4", "Column5", "Column7", "Column9", "Column10", "Column12", "Column14", "Column15", "Column23", "Column24", "Column25", "Column26", "Column27", "EA", "EA_1", "EA_2"}),
    
    // 跳过前7行标题
    删除的顶端行 = Table.Skip(删除不需要列, 7),
    
    // 重命名所有列为标准名称
    重命名列 = Table.RenameColumns(删除的顶端行, {
        {"Column2", "Work Center"}, 
        {"Column6", "Material"}, 
        {"Column8", "Material type"}, 
        {"Column11", "Production Scheduler"}, 
        {"Column13", "Order"}, 
        {"Column16", "Operation"}, 
        {"Column17", "Operation Description"}, 
        {"Column18", "Posting date"}, 
        {"Column19", "Earned Labor Unit"}, 
        {"Column20", "Machine Time"}, 
        {"Column21", "Earned Labor Time"},
        {"Column22", "Actual Quantity"}
    }),
    
    // 统一数据类型转换
    更改类型 = Table.TransformColumnTypes(重命名列, {
        {"Posting date", type date}, 
        {"Earned Labor Time", type number}, 
        {"Machine Time", type number},
        {"Actual Quantity", type number}
    }),
    
    筛选有效订单 = Table.SelectRows(更改类型, each [Order] <> null),
    筛选调度员 = Table.SelectRows(筛选有效订单, each [Production Scheduler] <> "Production Scheduler")
in
    筛选调度员