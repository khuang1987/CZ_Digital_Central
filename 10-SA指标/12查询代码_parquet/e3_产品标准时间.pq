let
    // ============================================
    // 产品标准时间表 - 从Parquet文件读取
    // ============================================
    // 说明：此查询直接从ETL合并后的标准时间表Parquet文件读取数据
    // 文件路径：publish/SAP_Routing_yyyymmdd.parquet（自动查找最新文件）
    // ============================================
    
    // 从SharePoint读取Parquet文件
    // SharePoint站点和路径
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    SharePointFolder = "Shared Documents/General/POWER BI 数据源 V2/30-MES导出数据",
    
    // 获取SharePoint文件夹中的文件
    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    
    // 筛选SAP_Routing文件（以SAP_Routing_开头，.parquet结尾）
    FilteredFiles = Table.SelectRows(SharePointFiles, each 
        Text.Contains([Folder Path], SharePointFolder) and 
        Text.StartsWith([Name], "SAP_Routing_") and 
        [Extension] = ".parquet"
    ),
    
    // 按修改时间排序，取最新的文件
    SortedFiles = if Table.RowCount(FilteredFiles) > 0 
        then Table.Sort(FilteredFiles, {{"Date modified", Order.Descending}})
        else null,
    
    // 获取最新文件内容
    LatestFileContent = if SortedFiles <> null and Table.RowCount(SortedFiles) > 0 
        then SortedFiles[Content]{0} 
        else null,
    
    // 如果找到文件，读取Parquet；否则返回空表
    Source = if LatestFileContent <> null 
        then Parquet.Document(LatestFileContent) 
        else #table({"CFN", "Operation", "Group", "Quantity", "Machine", "Labor", "OEE", "Setup Time (h)", "Effective Time (h)", "备注", "分类"}, {}),
    
    // 数据类型转换（确保字段类型正确）
    #"Changed Type" = Table.TransformColumnTypes(Source, {
        // 文本字段
        {"CFN", type text},
        {"Operation", type text},
        {"Group", type text},
        {"备注", type text},
        {"分类", type text},
        // 数值字段
        {"Quantity", type number},
        {"Machine", type number},
        {"Labor", type number},
        {"OEE", type number},
        {"Setup Time (h)", type number},
        {"Effective Time (h)", type number}
    }),
    
    // 最终输出
    Result = #"Changed Type"
in
    Result

