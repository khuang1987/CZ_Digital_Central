let
    // ============================================
    // SFC产品检验记录数据 - 原始数据加载
    // ============================================
    // 说明：此查询直接读取SFC产品检验记录原始数据
    // 包含每件产品在不同工序的合格情况
    // 文件路径：publish/SFC_Product_Inspection_latest.parquet
    // ============================================
    
    // 从SharePoint读取Parquet文件
    SharePointSite = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction",
    SharePointFolder = "Shared Documents/General/POWER BI 数据源 V2/30-MES导出数据",
    
    // 获取SharePoint文件夹中的所有文件
    SharePointFiles = SharePoint.Files(SharePointSite, [ApiVersion = 15]),
    
    // 筛选包含"publish"的文件夹路径，查找目标文件
    PublishFolderFiles = Table.SelectRows(SharePointFiles, each
        Text.Contains([Folder Path], SharePointFolder) and
        Text.Contains(Text.Lower([Folder Path]), "publish")
    ),
    TargetFiles = Table.SelectRows(PublishFolderFiles, each
        Text.StartsWith([Name], "SFC_Product_Inspection") and
        [Extension] = ".parquet"
    ),
    
    // 按修改时间排序，取最新文件
    SortedFiles = if Table.RowCount(TargetFiles) > 0 
        then Table.Sort(TargetFiles, {{"Date modified", Order.Descending}})
        else null,
    
    // 获取最新文件内容
    LatestFile = if SortedFiles <> null 
        then SortedFiles{0}[Content]
        else null,
    
    // 读取Parquet文件数据
    ParquetData = if LatestFile <> null
        then Parquet.Document(LatestFile)
        else null,
    
    // 转换为表格并应用字段类型
    SourceData = if ParquetData <> null
        then let
            // 获取表格数据
            TableData = ParquetData,
            
            // 应用字段类型转换（员工级数据结构
            TypedTable = Table.TransformColumnTypes(TableData, {
                {"Employee", type text},
                {"Date", type date},
                {"Year", type number},
                {"Month", type number},
                {"Day", type number},
                {"Week", type number},
                {"BatchNumber", type text},
                {"Operation", type text},
                {"OperationDescription", type text},
                {"Team", type text},
                {"PassQuantity", type number},
                {"FailQuantity", type number},
                {"MaxSerial", type number},
                {"CompletedQuantity", type number},
                {"OperationCount", type number},
                {"PassRate", type number},
                {"Efficiency_Score", type number},
                {"FileModTime", type datetime},
                {"SourceFile", type text},
                {"AggregatedTime", type datetime}
            })
        in
            TypedTable
        else null,
    
    // 数据质量检查和清洗
    CleanedData = if SourceData <> null
        then let
            // 移除空行
            RemoveNulls = Table.SelectRows(SourceData, each 
                [BatchNumber] <> null and [BatchNumber] <> "" and [Employee] <> null and [Employee] <> ""
            ),
            
            // 添加计算字段
            AddCalculatedFields = Table.AddColumn(RemoveNulls, "TotalInspectionCount", each 
                [PassQuantity] + [FailQuantity], type number),
            
            // 添加合格率百分比显示
            AddPassRatePercent = Table.AddColumn(AddCalculatedFields, "PassRatePercent", each 
                Number.Round([PassRate] * 100, 2), type number),
            
            // 添加时间维度字段（使用Date字段
            AddTimeDimensions = Table.AddColumn(AddPassRatePercent, "ReportYear", each 
                Date.Year([Date]), type number),
            
            AddMonth = Table.AddColumn(AddTimeDimensions, "ReportMonth", each 
                Date.Month([Date]), type number),
            
            AddQuarter = Table.AddColumn(AddMonth, "ReportQuarter", each 
                Date.QuarterOfYear([Date]), type number),
            
            // 添加批次状态标识
            AddBatchStatus = Table.AddColumn(AddQuarter, "BatchStatus", each 
                if [PassRate] >= 0.95 then "优秀"
                else if [PassRate] >= 0.90 then "良好" 
                else if [PassRate] >= 0.80 then "合格"
                else "不合格", type text)
            
        in
            AddBatchStatus
        else null,
    
    // 返回最终数据
    Result = CleanedData
    
in
    Result
