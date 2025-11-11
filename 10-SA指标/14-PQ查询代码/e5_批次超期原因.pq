let
    // 数据源：SharePoint Excel文件
    Source = Excel.Workbook(Web.Contents("https://medtronicapac.sharepoint.com/sites/CZOPS871/Shared%20Documents/General/SA%E4%B8%8D%E8%BE%BE%E6%A0%87%E5%8E%9F%E5%9B%A0%E5%88%86%E6%9E%90.xlsx"), null, true),
    
    // 获取第一个工作表
    Sheet1 = Source{[Item="Export",Kind="Sheet"]}[Data],
    
    // 提升标题行
    Headers = Table.PromoteHeaders(Sheet1, [PromoteAllScalars=true]),
    #"Filtered Rows" = Table.SelectRows(Headers, each ([原因分类] <> null)),
    #"Removed Other Columns" = Table.SelectColumns(#"Filtered Rows",{"BatchNumber", "导出日期", "CFN", "Operation", "原因分析", "原因分类"})
in
    #"Removed Other Columns"