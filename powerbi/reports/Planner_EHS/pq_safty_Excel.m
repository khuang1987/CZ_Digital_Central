let
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]),
    筛选的行 = Table.SelectRows(Source, each Text.Contains([Name], "TIER") or Text.Contains([Name], "Tier")),
    拓展的表 = Table.AddColumn(筛选的行, "raw_data", each Excel.Workbook([Content],true)),
    #"Filtered Rows" = Table.SelectRows(拓展的表, each ([Extension] = ".xlsx")),
    拓展的表2 = Table.ExpandTableColumn(#"Filtered Rows", "raw_data", {"Name", "Data", "Item"}, {"Name.1", "Data", "Item"}),
    筛选的行1 = Table.SelectRows(拓展的表2, each [Item] = "任务"),
    #"Sorted Rows" = Table.Sort(筛选的行1,{{"Name", Order.Ascending}}),
    #"Filtered Rows1" = Table.SelectRows(#"Sorted Rows", each not Text.Contains([Folder Path], "历史未删减")),
    #"展开的“Data”" = Table.ExpandTableColumn(#"Filtered Rows1", "Data", {"任务 ID", "任务名称", "存储桶名称", "进度", "优先级", "分配对象", "创建者", "创建日期", "开始日期", "截止日期", "是定期的", "延迟", "完成日期", "完成者", "已完成的清单项", "清单项", "标签", "说明"}, {"任务 ID", "任务名称", "存储桶名称", "进度", "优先级", "分配对象", "创建者", "创建日期", "开始日期", "截止日期", "是定期的", "延迟", "完成日期", "完成者", "已完成的清单项", "清单项", "标签", "说明"}),
    筛选的行2 = Table.SelectRows(#"展开的“Data”", each [存储桶名称] = "安全" or ([存储桶名称] = "需要升级TIER 4" and Text.Contains([任务名称], "A3-", Comparer.OrdinalIgnoreCase))),
    删除的列 = Table.RemoveColumns(筛选的行2,{"Content", "Extension", "Date accessed", "Date modified", "Date created", "Attributes", "Folder Path"}),
    更改的类型1 = Table.TransformColumnTypes(删除的列,{{"创建日期", type date}}),
    已添加索引 = Table.AddIndexColumn(更改的类型1, "索引", 1, 1, Int64.Type),
    去除的文本 = Table.TransformColumns(已添加索引,{{"说明", Text.Trim, type text}}),
    已提取分隔符之间的文本 = Table.TransformColumns(去除的文本, {{"Name", each Text.BetweenDelimiters(_, "-", "-"), type text}}),
    替换的值 = Table.ReplaceValue(已提取分隔符之间的文本,"器械加工中心","加工中心 MCT",Replacer.ReplaceText,{"Name"}),
    重命名的列 = Table.RenameColumns(替换的值,{{"Name", "区域"}}),
    替换的值1 = Table.ReplaceValue(重命名的列,"升级Tier4",null,Replacer.ReplaceValue,{"标签"})
in
    替换的值1