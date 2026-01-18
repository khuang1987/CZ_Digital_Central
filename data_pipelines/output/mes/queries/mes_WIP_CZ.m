let
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]),
    #"Filtered Rows" = Table.SelectRows(Source, each Text.Contains([Folder Path], "70-SFC导出") and Text.Contains([Folder Path], "批次流转报表")),
    #"筛选日期:近7天" = Table.SelectRows(#"Filtered Rows", let latest = List.Max(#"Filtered Rows"[Date created]) in each Date.IsInPreviousNDays([Date created], 4) or [Date created] = latest),
    拓展的表 = Table.AddColumn(#"筛选日期:近7天", "raw_data", each Excel.Workbook([Content], true)),
    拓展的表2 = Table.ExpandTableColumn(拓展的表, "raw_data", {"Name", "Data", "Item"}, {"Name.1", "Data", "Item"}),
    #"Filtered Rows2" = Table.SelectRows(拓展的表2, each ([Name.1] = "Table1")),
    删除的其他列 = Table.SelectColumns(#"Filtered Rows2",{"Name", "Date created", "Data"}),
    已提取分隔符之间的文本 = Table.TransformColumns(删除的其他列, {{"Name", each Text.BetweenDelimiters(_, "-", "."), type text}}),
    #"Extracted Text Range" = Table.TransformColumns(已提取分隔符之间的文本, {{"Name", each Text.Middle(_, 0, 8), type text}}),
    更改的类型3 = Table.TransformColumnTypes(#"Extracted Text Range", {{"Name", type date}}),
    #"Grouped Rows" = Table.Group(更改的类型3, {"Name"}, {{"DataCreated", each List.Max([Date created]), type datetime}}),
    #"Renamed Columns2" = Table.RenameColumns(#"Grouped Rows",{{"Name", "报表日期"}}),
    #"table join" = Table.Join(#"Renamed Columns2", "DataCreated", 更改的类型3, "Date created", JoinKind.Inner),
    #"Removed Other Columns" = Table.SelectColumns(#"table join",{"Name", "Date created", "Data"}),
    #"展开的“Data”" = Table.ExpandTableColumn(#"Removed Other Columns", "Data", {"产品类型", "生产流转卡号", "物料编码", "产品号", "产品名称", "批次", "开批时间", "当前工序号", "当前工序名称", "工序状态", "计划数量", "合格数量 (上道工序合格数量)", "上道结束时间", "当前开工时间", "等待时间 (小时)", "标准LT", "冻结", "机台号"}, {"产品类型", "生产流转卡号", "物料编码", "产品号", "产品名称", "批次", "开批时间", "当前工序号", "当前工序名称", "工序状态", "计划数量", "合格数量 (上道工序合格数量)", "上道结束时间", "当前开工时间", "等待时间 (小时)", "标准LT", "冻结", "机台号"}),
    重命名的列 = Table.RenameColumns(#"展开的“Data”",{{"Name", "报表时间"}}),
    
    // 添加报表日期列（用于去重分组）
    #"添加报表日期" = Table.AddColumn(重命名的列, "报表日期", each Date.From([报表时间]), type date),

    // 去重逻辑：按报表日期+批次+工序号分组，保留同一天内最新记录（解决线切割等工序重复批次问题）
    #"按批次工序去重" = Table.Group(
        #"添加报表日期", 
        {"报表日期", "批次", "当前工序号"}, 
        {
            {"最新记录", each 
                Table.First(
                    Table.Sort(_, {{"报表时间", Order.Descending}, {"上道结束时间", Order.Descending}})
                ), 
                type record
            }
        }
    ),
    #"展开去重记录" = Table.ExpandRecordColumn(
        #"按批次工序去重", 
        "最新记录", 
        {"产品类型", "生产流转卡号", "物料编码", "产品号", "产品名称", "开批时间", "当前工序名称", "工序状态", "计划数量", "合格数量 (上道工序合格数量)", "上道结束时间", "当前开工时间", "等待时间 (小时)", "标准LT", "冻结", "机台号", "报表时间"}
    ),

    工序名称清洗 = Table.AddColumn(#"展开去重记录", "工序清洗", each 
    if Text.Contains([当前工序名称], "数控铣") then "数控铣"
    else if Text.StartsWith([当前工序名称], "车") or Text.Contains([当前工序名称], "数车") or Text.Contains([当前工序名称], "数控车") or Text.Contains([当前工序名称], "车削")then "车削"
    else if Text.StartsWith([当前工序名称], "多轴") then "多轴车铣复合"
    else if Text.StartsWith([当前工序名称], "无心磨")or Text.Contains([当前工序名称], "平面磨") then "无心磨"
    else if Text.StartsWith([当前工序名称], "深孔钻")or Text.StartsWith([当前工序名称], "钻深孔") then "深孔钻"
    else if Text.Contains([当前工序名称], "五轴磨") then "五轴磨(可外协)"
    else if Text.StartsWith([当前工序名称], "线切割") then "线切割"
    else if Text.StartsWith([当前工序名称], "激光打标") then "激光打标"
    else if Text.StartsWith([当前工序名称], "纵切") then "纵切"
    else if Text.StartsWith([当前工序名称], "锯") then "锯"
    else if Text.StartsWith([当前工序名称], "数控铣") or Text.StartsWith([当前工序名称], "铣")then "数控铣"
    else if Text.StartsWith([当前工序名称], "包装") then "包装"
    else if Text.StartsWith([当前工序名称], "打标") then "打标"
    else if Text.StartsWith([当前工序名称], "点钝") then "点钝化"
    else if Text.StartsWith([当前工序名称], "电解") then "电解"
    else if Text.Contains([当前工序名称], "钝化") then "钝化"
    else if Text.Contains  ([当前工序名称], "热处理") then "热处理"
    else if Text.Contains  ([当前工序名称], "焊接") then "焊接"
    else if Text.Contains  ([当前工序名称], "激光焊") then "激光焊"
    else if Text.Contains  ([当前工序名称], "抛光") then "抛光"
    else if Text.Contains  ([当前工序名称], "喷砂") then "喷砂"
    else if Text.Contains  ([当前工序名称], "钳") then "钳"
    else if Text.Contains  ([当前工序名称], "清洗") then "清洗"
    else if Text.Contains  ([当前工序名称], "涂色") then "涂色"
    else if Text.Contains  ([当前工序名称], "微喷砂") then "微喷砂"
    else if Text.Contains  ([当前工序名称], "氩弧焊") then "氩弧焊"
    else if Text.Contains  ([当前工序名称], "研磨") then "研磨"
    else if Text.Contains  ([当前工序名称], "折弯") then "折弯"
    else if Text.Contains  ([当前工序名称], "注塑") then "注塑"
    else if Text.StartsWith  ([当前工序名称], "装") then "装配"
    else if Text.Contains  ([当前工序名称], "检验") or Text.Contains  ([当前工序名称], "终检") then "终检"
    else if Text.Contains  ([当前工序名称], "镀铬") then "镀铬(外协)"
    else if Text.Contains  ([当前工序名称], "电火花") then "电火花（外协）"
    else if Text.StartsWith([当前工序名称], "N/A") then "Preparation step"
    else "镀铬", type text),
    #"Reordered Columns" = Table.ReorderColumns(工序名称清洗,{"产品类型", "生产流转卡号", "物料编码", "产品号", "产品名称", "批次", "开批时间", "当前工序号", "当前工序名称", "工序清洗", "工序状态", "计划数量", "合格数量 (上道工序合格数量)", "上道结束时间", "当前开工时间", "等待时间 (小时)", "标准LT", "冻结", "机台号", "报表时间"}),
    合并查询工序号区域LT = Table.NestedJoin(#"Reordered Columns", {"工序清洗"}, 工序分类, {"Operation Name"}, "工序分类", JoinKind.LeftOuter),
    #"展开的“工序分类”" = Table.ExpandTableColumn(合并查询工序号区域LT, "工序分类", {"ST Routing", "LT"}, {"工序分类.ST Routing", "工序分类.LT"}),
    替换的值 = Table.ReplaceValue(#"展开的“工序分类”","N/A","1/1/2000 0:0:0 AM",Replacer.ReplaceValue,{"上道结束时间"}),
    更改的类型4 = Table.TransformColumnTypes(替换的值,{ {"上道结束时间", type datetime}}),
    已添加条件列1 = Table.AddColumn(更改的类型4, "数量", each if [#"合格数量 (上道工序合格数量)"] = "N/A" then [计划数量] else [#"合格数量 (上道工序合格数量)"]),
    更改的类型 = Table.TransformColumnTypes(已添加条件列1,{{"开批时间", type datetime}, {"当前开工时间", type datetime}}),
    #"Replaced Errors" = Table.ReplaceErrorValues(更改的类型, {{"当前开工时间", null}}),
    Custom2 = Table.AddColumn(#"Replaced Errors", "进工序天数", each if ([上道结束时间] = null or Date.From([上道结束时间]) < #date(2024, 1, 1))and [当前开工时间]=null then
    null
else if [当前开工时间]<>null and Date.From([上道结束时间]) < #date(2024, 1, 1)then Number.Round(Duration.TotalDays(DateTime.From([报表时间]) - (DateTime.From([当前开工时间]) + #duration(0, 9, 0, 0))), 1)
else
    Number.Round(
        Duration.TotalDays(
            DateTime.From([报表时间]) - (DateTime.From([上道结束时间]) + #duration(0, 9, 0, 0))
        ), 
        1
    )),
    #"Added Conditional Column" = Table.AddColumn(Custom2, "超期状态", each 
    if [上道结束时间] = null then 
        "-" 
    else if [进工序天数] <> null and [工序分类.LT] <> null and [进工序天数] > [工序分类.LT] then 
        "超期" 
    else 
        "正常"
),
    #"Removed Columns" = Table.RemoveColumns(#"Added Conditional Column",{"当前工序名称", "计划数量", "合格数量 (上道工序合格数量)"}),
    #"Renamed Columns" = Table.RenameColumns(#"Removed Columns",{{"工序分类.ST Routing", "生产单元"}, {"当前开工时间", "开工日期"}, {"当前工序号", "工序号"}, {"产品号", "产品代码"}, {"工序清洗", "工序名称"}}),
    #"Removed Columns1" = Table.RemoveColumns(#"Renamed Columns",{"标准LT"}),
    #"Renamed Columns1" = Table.RenameColumns(#"Removed Columns1",{{"工序分类.LT", "标准LT"}, {"报表时间", "报表日期"}, {"生产单元", "生产工序"}})
in
    #"Renamed Columns1"