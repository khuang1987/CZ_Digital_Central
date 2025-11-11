let
    // 从SharePoint获取源数据 
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]), 
    
    // 筛选70-SFC文件夹下的数据
    FilterAndCombine = Table.SelectRows(Source, each Text.Contains([Folder Path], "70-SFC") and Text.StartsWith([Name], "LC-")),
    #"Filtered Rows1" = Table.SelectRows(FilterAndCombine, let latest = List.Max(FilterAndCombine[Date created]) in each [Date created] = latest or [Date created] > #datetime(2025, 9, 26, 0, 0, 0)),
    
    // 修改Excel内容读取逻辑
    ExtractContent = Table.AddColumn(#"Filtered Rows1", "Data", each  
        let 
            WorkbookContent = Excel.Workbook([Content], true), 
            FirstSheet = WorkbookContent{0}[Data],
            RawData = FirstSheet,
            // 目标表头（包含可选列"上道工序报工时间"）
            TargetHeaders = {"产品类型", "产品号", "批次", "工序号", "工序名称", "工序状态", "机台号", "报工人", "报工时间", "上道工序报工时间", "Check In", "Check In 时间", "合格数量", "报废数量", "合格率", "NC单号", "备注"},
            // 仅取与实际列数相同的前N个表头，避免重命名数量不一致报错
            ActualColNames = Table.ColumnNames(RawData),
            HeadersToUse = List.FirstN(TargetHeaders, List.Count(ActualColNames)),
            RenamePairs = List.Zip({ActualColNames, HeadersToUse}),
            DataWithCustomHeaders = Table.RenameColumns(RawData, RenamePairs, MissingField.Ignore)
        in 
            DataWithCustomHeaders), 
    
    // 移除不需要的列
    RemoveColumns = Table.SelectColumns(ExtractContent, {"Name", "Date created", "Data"}), 
    
    // 转换日期格式 
    ConvertDate = Table.TransformColumns(RemoveColumns, {{"Date created", DateTime.Date, type date}}), 
    // 重命名日期创建列为 PascalCase
    RenameDateCreated = Table.RenameColumns(ConvertDate, {{"Date created", "DateCreated"}}),
    
    // 展开数据：优先包含可选列"上道工序报工时间"，若不存在则回退
    ExpandedData =
        let
            tryExpand = try Table.ExpandTableColumn(RenameDateCreated, "Data", {"产品类型", "产品号", "批次", "工序号", "工序名称", "报工时间", "上道工序报工时间", "Check In 时间", "机台号", "报工人", "合格数量", "报废数量"})
        in
            if tryExpand[HasError] then Table.ExpandTableColumn(RenameDateCreated, "Data", {"产品类型", "产品号", "批次", "工序号", "工序名称", "报工时间", "Check In 时间", "机台号", "报工人", "合格数量", "报废数量"}) else tryExpand[Value],

    // 规范主要字段命名（逐对尝试重命名，缺失则跳过）
    RenamedCols = 
        let
            renamePairs = {
                {"产品类型", "ProductType"},
                {"产品号", "ProductCode"},
                {"批次", "BatchNumber"},
                {"工序号", "Operation"},
                {"工序名称", "OperationDescription"},
                {"报工时间", "TrackOutTime"},
                {"上道工序报工时间", "PrevReportTime"},
                {"Check In 时间", "CheckInTime"},
                {"机台号", "ResourceCode"},
                {"报工人", "VSM"},
                {"合格数量", "TrackOutQuantity"},
                {"报废数量", "ScrapQuantity"}
            },
            renamed = List.Accumulate(
                renamePairs,
                ExpandedData,
                (state, current) => try Table.RenameColumns(state, {current}) otherwise state
            )
        in
            renamed,

    // 若缺少PrevReportTime列则补充为null
    EnsurePrevReportExists = if List.Contains(Table.ColumnNames(RenamedCols), "PrevReportTime") then RenamedCols else Table.AddColumn(RenamedCols, "PrevReportTime", each null, type nullable datetime),

    // Operation 去空格、转数字，失败为null，成功补零为4位
    CleanOperation = Table.TransformColumns(
        EnsurePrevReportExists,
        {{"Operation", each 
            let
                trimmed = Text.Trim(Text.From(_)),
                asNumber = try Number.From(trimmed) otherwise null,
                asText = if asNumber = null or asNumber = "NaN" then null else Text.PadStart(Text.From(Number.RoundDown(asNumber)), 4, "0")
            in
                asText,
            type text
        }}
    ),

    // 先把 "N/A"、空字符串等替换为 null（PrevReportTime）
    CleanPrevReportTime = Table.ReplaceValue(
        CleanOperation,
        each [PrevReportTime],
        each if [PrevReportTime] = "N/A" or [PrevReportTime] = "" then null else [PrevReportTime],
        Replacer.ReplaceValue,
        {"PrevReportTime"}
    ),

    // 再把 "N/A"、空字符串等替换为 null（CheckInTime）
    CleanCheckInTime = Table.ReplaceValue(
        CleanPrevReportTime,
        each [CheckInTime],
        each if [CheckInTime] = "N/A" or [CheckInTime] = "" then null else [CheckInTime],
        Replacer.ReplaceValue,
        {"CheckInTime"}
    ),

    // 转换 PrevReportTime 为 datetime
    FixPrevReportTime = Table.TransformColumns(
        CleanCheckInTime,
        {{"PrevReportTime", each try DateTime.From(_) otherwise null, type nullable datetime}}
    ),

    // 转换 CheckInTime 为 datetime
    FixCheckInTime = Table.TransformColumns(
        FixPrevReportTime,
        {{"CheckInTime", each try DateTime.From(_) otherwise null, type nullable datetime}}
    ),

    // 转换 TrackOutTime 为 datetime
    FixTrackOutTime = Table.TransformColumns(
        FixCheckInTime,
        {{"TrackOutTime", each try DateTime.From(_) otherwise null, type nullable datetime}}
    ),

    // 标准化工序名称（不进行分组，保留所有原始数据）
    StandardizedStepName = Table.TransformColumns(FixTrackOutTime, {
        {"OperationDescription", each
            if Text.StartsWith(_, "数控铣") then "数控铣"
            else if Text.Contains(_, "锯") or Text.Contains(_, "下料") then "锯"
            else if Text.Contains(_, "镀铬") then "镀铬"
            else if Text.StartsWith(_, "纵切") then "纵切"
            else if Text.StartsWith(_, "数控车") then "数控车"
            else if Text.StartsWith(_, "线切割") then "线切割"
            else if Text.StartsWith(_, "氩弧焊") then "氩弧焊"
            else if Text.Contains(_, "打标") then "打标"
            else if Text.StartsWith(_, "深孔") then "深孔钻"
            else if Text.StartsWith(_, "激光焊") then "激光焊"
            else if Text.StartsWith(_, "钳工") then "钳工"
            else if Text.StartsWith(_, "车削") then "车削"
            else if Text.StartsWith(_, "装配") then "装配"
            else if Text.StartsWith(_, "真空") then "真空热处理"
            else if Text.StartsWith(_, "热处理") or Text.StartsWith(_, "非真空热处理") then "热处理"
            else _
        }
    }),

    // 添加必要的字段
    AddPlantCode = Table.AddColumn(StandardizedStepName, "PlantCode", each "CZM", type text),
    AddCFN = Table.AddColumn(AddPlantCode, "CFN", each [ProductCode], type text),
    AddStartTime = Table.AddColumn(AddCFN, "StartTime", each [CheckInTime], type nullable datetime),
    AddTrackInTime = Table.AddColumn(AddStartTime, "TrackInTime", each [CheckInTime], type nullable datetime),
    AddTrackOutDate = Table.AddColumn(AddTrackInTime, "TrackOutDate", each if [TrackOutTime] <> null then Date.From([TrackOutTime]) else null, type nullable date),
    AddMachineNumber = Table.AddColumn(AddTrackOutDate, "Machine(#)", each 
        let
            resourceCode = [ResourceCode],
            extractedNumber = if resourceCode = null then null else 
                Text.Combine(List.Select(Text.ToList(Text.From(resourceCode)), each List.Contains({"0".."9"}, _)))
        in
            if extractedNumber = "" then null else Number.From(extractedNumber),
        type nullable number),

    // 合并标准时间表（左连接，保留所有SFC记录）
    MergedStandardTime = Table.NestedJoin(
        AddMachineNumber,
        {"CFN", "Operation"},
        e3_产品标准时间_exl,
        {"Material Number", "Operation"},
        "StandardData",
        JoinKind.LeftOuter
    ),

    // 展开标准时间字段，只取OEE、调试时间、Machine、Labor
    ExpandedStandardTime = Table.ExpandTableColumn(MergedStandardTime, "StandardData", {"OEE", "调试时间", "Machine", "Labor"}, {"OEE", "调试时间", "Machine", "Labor"}),
    // 将"调试时间"重命名为"Setup Time (h)"
    RenameSetupTime = Table.RenameColumns(ExpandedStandardTime, {{"调试时间", "SetupTime(h)"}}),

    // 设置OEE默认值为77%（如果OEE为空或等于0），直接写回OEE列
    FillOEE = Table.TransformColumns(RenameSetupTime, {{"OEE", each if _ = null or _ = 0 then 0.77 else _, type number}}),

    // 新增计算字段：如果Machine不为空且不等于0，则使用Machine，否则使用Labor
    AddEffectiveTime = Table.AddColumn(FillOEE, "EffectiveTime(h)", each 
        if [Machine] <> null and [Machine] <> 0 then [Machine] else [Labor], 
        type number),

    // 计算标准时间(ST) = 数量 * EffectiveTime / OEE，单位为天
    AddST = Table.AddColumn(AddEffectiveTime, "ST(d)", each 
        let
            qty = if [TrackOutQuantity] = null then 0 else [TrackOutQuantity],
            effectiveTime = if [#"EffectiveTime(h)"] = null then 0 else [#"EffectiveTime(h)"],
            oee = if [OEE] = null then 0.77 else [OEE],
            pptHours = qty * effectiveTime / oee
        in
            if pptHours = 0 then null else Number.Round(pptHours / 24, 2),
        type number),

    // 计算LT (d) = 报工时间 - CheckIn时间（单位：天，保留两位小数）
    AddLT = Table.AddColumn(AddST, "LT(d)", each 
        if [TrackOutTime] <> null and [CheckInTime] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [CheckInTime]), 2) else null,
        type number),

    // 计算PT(d) = 报工时间 - CheckIn时间（单位：天，保留两位小数）
    AddPT = Table.AddColumn(AddLT, "PT(d)", each 
        if [TrackOutTime] <> null and [CheckInTime] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [CheckInTime]), 2) else null, 
        type number),

    // 合并超期原因表，按批次号、CFN、工序号关联
    MergedOverdueReasons = Table.NestedJoin(
        AddPT,
        {"BatchNumber", "CFN", "Operation"},
        e5_批次超期原因,
        {"BatchNumber", "CFN", "OP"},
        "OverdueData",
        JoinKind.LeftOuter
    ),

    // 展开超期原因字段
    ExpandedOverdueReasons = Table.ExpandTableColumn(MergedOverdueReasons, "OverdueData", {"原因分析", "原因分类"}, {"原因分析", "原因分类"}),

    // 重命名Machine和Labor列名，添加(h)后缀
    RenamedColumns = Table.RenameColumns(ExpandedOverdueReasons, {
        {"Machine", "Machine(h)"},
        {"Labor", "Labor(h)"}
    }),

    // 重新排列列顺序，只包含有用的字段
    ReorderedColumns = Table.ReorderColumns(RenamedColumns,{
        "Name", "DateCreated", "PlantCode", "BatchNumber", "CFN", "Operation", "OperationDescription", 
        "ResourceCode", "VSM", "CheckInTime", "TrackOutTime", "StartTime", "TrackInTime", "TrackOutDate", 
        "TrackOutQuantity", "LT(d)", "PT(d)", "ST(d)", "OEE", "SetupTime(h)", "Machine(h)", "Labor(h)", 
        "EffectiveTime(h)", "Machine(#)", "原因分析", "原因分类"
    }),

    // 统一类型：Time类为datetime，Date类为date，OEE为百分比，数量为整数，Machine和Labor为2位小数
    ChangedTypeFinal = Table.TransformColumnTypes(ReorderedColumns,
        {{"DateCreated", type date},
         {"TrackOutDate", type date},
         {"TrackInTime", type datetime},
         {"TrackOutTime", type datetime},
         {"StartTime", type datetime},
         {"CheckInTime", type datetime},
         {"OEE", Percentage.Type},
         {"TrackOutQuantity", Int64.Type},
         {"Machine(h)", type number},
         {"Labor(h)", type number},
         {"SetupTime(h)", type number},
         {"Machine(#)", Int64.Type},
         {"EffectiveTime(h)", type number}
        })
in
    ChangedTypeFinal