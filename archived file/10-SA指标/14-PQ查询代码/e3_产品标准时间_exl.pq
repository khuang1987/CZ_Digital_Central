let
    // 1. 读取Excel文件
    Source = Excel.Workbook(Web.Contents("https://medtronicapac.sharepoint.com/sites/CZOPS871/Shared%20Documents/General/1303%20Routing%E5%8F%8A%E6%9C%BA%E5%8A%A0%E5%B7%A5%E4%BA%A7%E5%93%81%E6%B8%85%E5%8D%95.xlsx"), null, true),

    // 2. 读取1303 Routing表
    RoutingRaw = Source{[Item="1303 Routing",Kind="Sheet"]}[Data],
    RoutingHeaders = Table.PromoteHeaders(RoutingRaw, [PromoteAllScalars=true]),
    // RoutingHeaders 清洗
    RoutingHeadersClean = Table.ReplaceErrorValues(RoutingHeaders, List.Transform(Table.ColumnNames(RoutingHeaders), each {_, null})),
    RoutingHeadersClean2 = Table.ReplaceValue(RoutingHeadersClean, "N/A", null, Replacer.ReplaceValue, Table.ColumnNames(RoutingHeadersClean)),
    RoutingHeadersClean3 = Table.ReplaceValue(RoutingHeadersClean, "#N/A", null, Replacer.ReplaceValue, Table.ColumnNames(RoutingHeadersClean)),

    // 3. 读取1303机加工清单表
    MachiningRaw = Source{[Item="1303机加工清单",Kind="Sheet"]}[Data],
    MachiningHeaders = Table.PromoteHeaders(MachiningRaw, [PromoteAllScalars=true]),
    // MachiningHeaders 清洗
    MachiningHeadersClean = Table.ReplaceErrorValues(MachiningHeaders, List.Transform(Table.ColumnNames(MachiningHeaders), each {_, null})),
    MachiningHeadersClean4 = Table.ReplaceValue(MachiningHeadersClean, "N/A", null, Replacer.ReplaceValue, Table.ColumnNames(MachiningHeadersClean)),
    MachiningHeadersClean5 = Table.ReplaceValue(MachiningHeadersClean, "#N/A", null, Replacer.ReplaceValue, Table.ColumnNames(MachiningHeadersClean)),
    // 4. 选择用于匹配和合并的字段
    MachiningSelected = Table.SelectColumns(MachiningHeadersClean, {
        "Material Number", "CFN", "Group", "Operation/activity", "Operation description", "OEE", "调试时间", "备注"
    }),

    RoutingSelected = Table.SelectColumns(RoutingHeadersClean, {
        "Material Number", "CFN", "Group", "Operation/activity", "Operation description", /* 其他你需要的字段 */
        "Work Center", "Base Quantity", "Task list UOM", "Machine", "Unit (Machine)", "Labor", "Unit (Labor)", "Fix lot size", "Min lot size", "Rounding Value", "Max lot size"
    }),

    // 6. 合并查询（左外部连接，保留左表所有记录）
    Merged = Table.NestedJoin(
        RoutingSelected, 
        {"Material Number", "CFN", "Group", "Operation/activity"},
        MachiningSelected, 
        {"Material Number", "CFN", "Group", "Operation/activity"},
        "MachiningData", 
        JoinKind.LeftOuter
    ),

    // 7. 展开合并字段
    Expanded = Table.ExpandTableColumn(Merged, "MachiningData", {"OEE", "调试时间", "备注"}, {"OEE", "调试时间", "备注"}),
    #"Filtered Rows" = Table.SelectRows(Expanded, each ([Operation description] = "数控铣")),
    // 8. 替换所有字段中的 "N/A" 为 null
    ReplacedNA = Table.ReplaceValue(#"Filtered Rows", "N/A", null, Replacer.ReplaceValue, Table.ColumnNames(#"Filtered Rows")),

    // 9. Machine 和 Labor 字段从秒转小时，保留2位小数
    ConvertedTime = Table.TransformColumns(ReplacedNA, {
        {"Machine", each if _ = null then null else Number.Round(try Number.From(_) / 3600 otherwise null, 2)},
        {"Labor", each if _ = null then null else Number.Round(try Number.From(_) / 3600 otherwise null, 2)}
    }),

    // 10. OEE 转为百分数并保留2位小数，调试时间转为数字并保留2位小数
    ConvertedNumeric = Table.TransformColumns(ConvertedTime, {
        {"OEE", each if _ = null then null else Number.Round(try Number.From(_)  otherwise null, 2)},
        {"调试时间", each if _ = null then null else Number.Round(try Number.From(_) otherwise null, 2)}
    }),
    // 11. 类型转换
    Typed = Table.TransformColumnTypes(ConvertedNumeric, {{"OEE", Percentage.Type}, {"调试时间", type number}}),
    #"Renamed Columns" = Table.RenameColumns(Typed,{{"Operation/activity", "Operation"}})
in
    #"Renamed Columns"