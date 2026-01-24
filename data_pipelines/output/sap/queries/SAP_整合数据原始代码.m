let
    // 合并两张工时数据表
    合并工时数据 = Table.Combine({#"B2-存档的工时数据", #"B3-每日更新的工时数据"}),

    // 去重
    去重后工时数据 = Table.Distinct(合并工时数据),

    // 提取分隔符后的文本
    提取WorkCenter = Table.TransformColumns(
        去重后工时数据,
        {{"Work Center", each Text.AfterDelimiter(_, "/"), type text}}
    ),

    // 过滤Posting date大于2024-04-27的数据
    过滤日期 = Table.SelectRows(提取WorkCenter, each [Posting date] > #date(2024, 4, 27)),

    // 过滤Work Center不以OSP-开头的数据
    过滤OSP = Table.SelectRows(过滤日期, each not Text.StartsWith([Work Center], "OSP-")),

    // Earned Labor Unit为S时，Earned Labor Time除以3600，否则保持原值（新增列）
    添加调整后工时 = Table.AddColumn(
        过滤OSP,
        "Earned Labor Time_调整",
        each if [Earned Labor Unit] = "s" then [Earned Labor Time] / 3600 else [Earned Labor Time]
    ),

    // 删除原 Earned Labor Time 列
    删除原工时列 = Table.RemoveColumns(添加调整后工时, {"Earned Labor Time"}),

    // 重命名新列为 Earned Labor Time
    重命名工时列 = Table.RenameColumns(删除原工时列, {{"Earned Labor Time_调整", "Earned Labor Time"}}),

    // 新增：规范化 Production Scheduler（新增列；并合并原来两个“替换”逻辑）
    规范化调度员 = Table.AddColumn(
        重命名工时列,
        "Production Scheduler_规范",
        each
            let
                ps0 = Text.Upper(Text.Trim([Production Scheduler])),
                ps  = if ps0 = "1303/#" then "1303/CIN"
                      else if ps0 = "9997/#" then "9997/INS"
                      else ps0
            in
                if ps = "1303/CIM" then "1303/植入物"
                else if ps = "1303/CIN" then "1303/器械"
                else if ps = "9997/STR" then "9997/无菌"
                else if ps = "9997/INS" then "9997/器械"
                else if ps = "9997/AP" or ps = "9997/AP1" then "9997/有源"
                else "9997/植入物",
        type text
    ),

    #"Changed Type" = Table.TransformColumnTypes(
        规范化调度员,
        {{"Earned Labor Time", type number}, {"Production Scheduler_规范", type text}}
    )
in
    #"Changed Type"