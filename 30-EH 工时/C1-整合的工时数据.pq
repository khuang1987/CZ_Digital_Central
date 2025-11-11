let
    // ==========================================
    // C1-整合的工时数据 - 融合三个数据源
    // ==========================================
    // 依赖查询:
    // - #"b2-存档的工时数据" (B2文件)
    // - #"b3-每日更新的工时数据" (B3文件)
    // - #"b1-KH存档的工时数据" (B1文件)
    // 注意: 三个数据源都会被直接合并
    // ==========================================
    
    // 直接合并所有3个数据源
    所有数据源 = Table.Combine({#"B2-存档的工时数据", #"b3-每日更新的工时数据", #"B1-KH存档的工时数据"}),
    
    // 合并、去重、提取Work Center（合并操作减少中间表）
    基础数据 = Table.TransformColumns(
        Table.Distinct(所有数据源), 
        {{"Work Center", each try Text.AfterDelimiter(_, "/") otherwise _, type text}}
    ),
    
    // 合并所有过滤条件，减少表传递次数
    过滤数据 = Table.SelectRows(
        基础数据, 
        each try ([Posting date] > #date(2024, 4, 27) and not Text.StartsWith([Work Center], "OSP-")) otherwise false
    ),
    
    // 替换Production Scheduler（合并两个替换操作）
    替换调度员 = Table.ReplaceValue(
        Table.ReplaceValue(
            过滤数据, 
            "1303/#", "1303/CIN", Replacer.ReplaceText, {"Production Scheduler"}
        ),
        "9997/#", "9997/INS", Replacer.ReplaceText, {"Production Scheduler"}
    ),
    
    // 调整工时：统一为小时，保留两位小数 + 错误处理
    临时列 = Table.AddColumn(
        替换调度员, 
        "TempTime", 
        each try Number.Round(
            if [Earned Labor Unit] = "s" and [Earned Labor Time] <> null then [Earned Labor Time] / 3600 
            else if [Earned Labor Time] <> null then [Earned Labor Time] 
            else 0, 
            2
        ) otherwise 0,
        type number
    ),
    调整工时 = Table.RenameColumns(
        Table.RemoveColumns(临时列, {"Earned Labor Time"}),
        {{"TempTime", "Earned Labor Time"}}
    )
in
    调整工时
