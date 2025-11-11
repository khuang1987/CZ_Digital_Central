let
    // 合并、去重、提取Work Center（合并操作减少中间表）
    基础数据 = Table.TransformColumns(
        Table.Distinct(
            Table.Combine({#"b2-存档的工时数据", #"b1-每日更新的工时数据"})
        ), 
        {{"Work Center", each Text.AfterDelimiter(_, "/"), type text}}
    ),
    
    // 合并所有过滤条件，减少表传递次数
    过滤数据 = Table.SelectRows(
        基础数据, 
        each [Posting date] > #date(2024, 4, 27) and not Text.StartsWith([Work Center], "OSP-")
    ),
    
    // 替换Production Scheduler（合并两个替换操作）
    替换调度员 = Table.ReplaceValue(
        Table.ReplaceValue(
            过滤数据, 
            "1303/#", "1303/CIN", Replacer.ReplaceText, {"Production Scheduler"}
        ),
        "9997/#", "9997/INS", Replacer.ReplaceText, {"Production Scheduler"}
    ),
    
    // 调整工时：使用AddColumn + 删除旧列（最清晰且性能好）+ 保留两位小数 + 错误处理
    临时列 = Table.AddColumn(
        替换调度员, 
        "TempTime", 
        each try Number.Round(
            if [Earned Labor Unit] = "s" and [Earned Labor Time] <> null then [Earned Labor Time] / 3600 else if [Earned Labor Time] <> null then [Earned Labor Time] else 0, 
            2
        ) otherwise 0
    ),
    调整工时 = Table.RenameColumns(
        Table.RemoveColumns(临时列, {"Earned Labor Time"}),
        {{"TempTime", "Earned Labor Time"}}
    )
in
    调整工时