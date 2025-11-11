let
    SharePoint源 = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    
    // ==========================================
    // 步骤1: 读取每日更新的工时数据（YPP开头）
    // ==========================================
    源1 = SharePoint.Files(SharePoint源, [ApiVersion = 15]),
    筛选1 = Table.SelectRows(源1, each ([Folder Path] = SharePoint源 & "/Shared Documents/General/POWER BI 数据源 V2/40-SAP工时/" or Text.Contains([Folder Path], SharePoint源 & "/Shared Documents/General/POWER BI 数据源 KH/04-SAP工时_V2")) and Text.StartsWith([Name], "YPP") and Text.EndsWith([Name], ".xlsx")),
    
    已添加自定义1 = Table.AddColumn(筛选1, "自定义", each Excel.Workbook([Content], true)),
    展开自定义1 = Table.ExpandTableColumn(已添加自定义1, "自定义", {"Name", "Data", "Item", "Kind", "Hidden"}, {"Name.1", "Data", "Item", "Kind", "Hidden"}),
    筛选Sheet1 = Table.SelectRows(展开自定义1, each ([Kind] = "Sheet")),
    
    删除多余列1 = Table.RemoveColumns(筛选Sheet1, {"Name", "Name.1", "Content", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path", "Item", "Kind", "Hidden"}),
    展开Data1 = Table.ExpandTableColumn(删除多余列1, "Data", 
        {"Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}, 
        {"Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24", "Column25", "Column26", "Column27"}),
    删除顶端行1 = Table.Skip(展开Data1, 6),
    提升标题1 = Table.PromoteHeaders(删除顶端行1, [PromoteAllScalars=true]),
    
    删除不需要列1 = Table.RemoveColumns(提升标题1, {"Actual start: Exec / Setup tim", "Actual finish: Exec. Time", "Actual Finish Exec.Time Operation"}),
    重命名1 = Table.RenameColumns(删除不需要列1, 
        {{"Column3", "Work Center Name"}, {"Column5", "Cost Center Name"}, {"Column7", "Material Name"}, {"Column10", "MRP Discription_1"}, {"Column12", "MRP_Discription_2"}, {"Column15", "Order type_2"}, {"Column23", "Machine Time"}, {"Column24", "Earned Labor Time"}, {"EA", "Actual Quantity"}, {"EA_1", "Actual Scrap Qty"}, {"EA_2", "Target Quantity"}}),
    筛选有效1 = Table.SelectRows(重命名1, each [Posting date] <> null and [Posting date] <> ""),
    添加工时数值1 = Table.TransformColumns(筛选有效1, {{"Earned Labor Time", each try Value.ToNumber(_) otherwise 0}}),
    更改类型1 = Table.TransformColumnTypes(添加工时数值1, {{"Posting date", type date}, {"Earned Labor Time", type number}, {"Actual Quantity", Int64.Type}, {"Actual Scrap Qty", Int64.Type}, {"Target Quantity", Int64.Type}}),
    重排序1 = Table.ReorderColumns(更改类型1, {"Plant", "Work Center", "Work Center Name", "Cost Center", "Cost Center Name", "Material", "Material Name", "Material type", "MRP Controller", "MRP Discription_1", "Production Scheduler", "MRP_Discription_2", "Order", "Order type", "Order type_2", "Operation", "Operation Description", "Posting date", "Earned Labor Time", "Earned Labor Unit", "Actual Quantity", "Actual Scrap Qty", "Target Quantity"}),
    排序1 = Table.Sort(重排序1, {{"Order", Order.Ascending}}),
    筛选Order1 = Table.SelectRows(排序1, each [Order] <> null),
    更改机器时间1 = Table.TransformColumnTypes(筛选Order1, {{"Machine Time", Int64.Type}}),
    每日更新数据 = Table.SelectRows(更改机器时间1, each [Production Scheduler] <> "Production Scheduler"),
    
    // ==========================================
    // 步骤2: 读取存档的工时数据（EH_FY开头）
    // ==========================================
    源2 = SharePoint.Files(SharePoint源, [ApiVersion = 15]),
    筛选2 = Table.SelectRows(源2, each ([Folder Path] = SharePoint源 & "/Shared Documents/General/POWER BI 数据源 V2/40-SAP工时/" or Text.Contains([Folder Path], SharePoint源 & "/Shared Documents/General/POWER BI 数据源 KH/04-SAP工时_V2")) and Text.StartsWith([Name], "EH_FY")),
    已添加自定义2 = Table.AddColumn(筛选2, "自定义", each Excel.Workbook([Content], true)),
    展开自定义2 = Table.ExpandTableColumn(已添加自定义2, "自定义", {"Name", "Data", "Item", "Kind", "Hidden"}, {"Name.1", "Data", "Item", "Kind", "Hidden"}),
    筛选Sheet2 = Table.SelectRows(展开自定义2, each [Kind] = "Sheet"),
    删除多余列2 = Table.RemoveColumns(筛选Sheet2, {"Name", "Name.1", "Content", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path", "Item", "Kind", "Hidden"}),
    展开Data2 = Table.ExpandTableColumn(删除多余列2, "Data", 
        {"Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24"}, 
        {"Column2", "Column3", "Column4", "Column5", "Column6", "Column7", "Column8", "Column9", "Column10", "Column11", "Column12", "Column13", "Column14", "Column15", "Column16", "Column17", "Column18", "Column19", "Column20", "Column21", "Column22", "Column23", "Column24"}),
    删除顶端行2 = Table.Skip(展开Data2, 6),
    提升标题2 = Table.PromoteHeaders(删除顶端行2, [PromoteAllScalars=true]),
    筛选Plant = Table.SelectRows(提升标题2, each [Plant] = "1303"),
    重命名2 = Table.RenameColumns(筛选Plant, {{"Column3", "Work Center Name"}, {"Column5", "Cost Center Name"}, {"Column7", "Material Name"}, {"Column10", "MRP Discription_1"}, {"Column12", "MRP_Discription_2"}, {"Column15", "Order type_2"}, {"Column20", "Machine Time"}, {"Column21", "Earned Labor Time"}, {"EA", "Actual Quantity"}, {"EA_1", "Actual Scrap Qty"}, {"EA_2", "Target Quantity"}}),
    添加工时数值2 = Table.TransformColumns(重命名2, {{"Earned Labor Time", each try Value.ToNumber(_) otherwise 0}}),
    更改类型2 = Table.TransformColumnTypes(添加工时数值2, {{"Posting date", type date}, {"Earned Labor Time", type number}, {"Actual Quantity", Int64.Type}, {"Actual Scrap Qty", Int64.Type}, {"Target Quantity", Int64.Type}, {"Machine Time", Int64.Type}}),
    存档数据 = Table.ReorderColumns(更改类型2, {"Plant", "Work Center", "Work Center Name", "Cost Center", "Cost Center Name", "Material", "Material Name", "Material type", "MRP Controller", "MRP Discription_1", "Production Scheduler", "MRP_Discription_2", "Order", "Order type", "Order type_2", "Operation", "Operation Description", "Posting date", "Machine Time", "Earned Labor Time", "Earned Labor Unit", "Actual Quantity", "Actual Scrap Qty", "Target Quantity"}),
    
    // ==========================================
    // 步骤3: 合并、去重、转换和过滤（优化版本）
    // ==========================================
    合并数据 = Table.Distinct(Table.Combine({存档数据, 每日更新数据})),
    提取WorkCenter = Table.TransformColumns(合并数据, {{"Work Center", each Text.AfterDelimiter(_, "/"), type text}}),
    过滤数据 = Table.SelectRows(提取WorkCenter, each [Posting date] > #date(2024, 4, 27) and not Text.StartsWith([Work Center], "OSP-")),
    替换调度员 = Table.ReplaceValue(Table.ReplaceValue(过滤数据, "1303/#", "1303/CIN", Replacer.ReplaceText, {"Production Scheduler"}), "9997/#", "9997/INS", Replacer.ReplaceText, {"Production Scheduler"}),
    临时列 = Table.AddColumn(替换调度员, "TempTime", each try Number.Round(if [Earned Labor Unit] = "s" and [Earned Labor Time] <> null then [Earned Labor Time] / 3600 else if [Earned Labor Time] <> null then [Earned Labor Time] else 0, 2) otherwise 0),
    最终结果 = Table.RenameColumns(Table.RemoveColumns(临时列, {"Earned Labor Time"}), {{"TempTime", "Earned Labor Time"}})
in
    最终结果

