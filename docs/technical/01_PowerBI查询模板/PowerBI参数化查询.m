// PowerBI参数化查询：支持动态切换数据范围
// 在PowerBI中创建参数来控制数据加载范围

let
    // 创建参数（在PowerBI中手动创建这些参数）
    // DataMode: "Incremental" | "History" | "Custom"
    // DaysBack: 数字参数
    // UseHistoryForTrends: true/false
    
    DataMode = "Incremental",  // 可在PowerBI中切换
    DaysBack = 7,              // 可在PowerBI中调整
    UseHistoryForTrends = true, // 是否包含历史趋势数据
    
    PartitionPath = "C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/partitions",
    HistoryFile = "C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/MES_batch_report_latest.parquet",
    
    // 增量数据（快速）
    GetIncrementalData = (days as number) =>
        let
            GetPartitionMetadata = (partitionPath as text) =>
                let
                    JsonContent = Json.Document(File.Contents(partitionPath & "/partition_metadata.json")),
                    partitions = JsonContent[partitions],
                    partitionList = Record.FieldValues(partitions)
                in
                    partitionList,
            
            GetLatestPartitions = (partitionPath as text, daysBack as number) =>
                let
                    allPartitions = GetPartitionMetadata(partitionPath),
                    recentPartitions = List.Sort(
                        List.Select(allPartitions, each (DateTime.LocalNow() - DateTime.FromText(_[date]) <= #duration(daysBack, 0, 0, 0))),
                        {{"date", Order.Descending}}
                    ),
                    partitionData = List.Transform(
                        recentPartitions,
                        each Parquet.Document(File.Contents(partitionPath & "/" & _[file]))
                    ),
                    combinedData = Table.Combine(partitionData)
                in
                    combinedData,
            
            Source = GetLatestPartitions(PartitionPath, days),
            #"Changed Type" = Table.TransformColumnTypes(Source, {
                {"TrackOutDate", type date},
                {"TrackOutTime", type datetime},
                {"BatchNumber", type text},
                {"Operation", type text},
                {"CFN", type text},
                {"PT(d)", type number},
                {"ST(d)", type number},
                {"CompletionStatus", type text}
            })
        in
            #"Changed Type",
    
    // 历史数据（完整）
    GetHistoryData = () =>
        let
            Source = Parquet.Document(File.Contents(HistoryFile)),
            #"Changed Type" = Table.TransformColumnTypes(Source, {
                {"TrackOutDate", type date},
                {"TrackOutTime", type datetime},
                {"BatchNumber", type text},
                {"Operation", type text},
                {"CFN", type text},
                {"PT(d)", type number},
                {"ST(d)", type number},
                {"CompletionStatus", type text}
            })
        in
            #"Changed Type",
    
    // 智能数据源选择
    Source = switch DataMode {
        "Incremental" => GetIncrementalData(DaysBack),
        "History" => GetHistoryData(),
        "Custom" => if UseHistoryForTrends then 
            GetHistoryData() 
        else 
            GetIncrementalData(DaysBack),
        _ => GetIncrementalData(DaysBack)
    },
    
    // 添加数据模式标记
    #"Added DataMode" = Table.AddColumn(Source, "DataMode", each DataMode, type text),
    
    // 添加时间范围标记
    #"Added TimeRange" = Table.AddColumn(#"Added DataMode", "TimeRange", each 
        if DataMode = "Incremental" then "最近" & Number.ToText(DaysBack) & "天"
        else if DataMode = "History" then "全部历史"
        else "自定义", 
        type text)
    
in
    #"Added TimeRange"
