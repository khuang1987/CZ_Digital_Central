let
    // 从SharePoint获取源数据
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]),

    // 筛选30-MES文件夹下以Product Output开头的Excel文件
    FilterAndCombine = Table.SelectRows(Source, each Text.Contains([Folder Path], "30-MES") and Text.StartsWith([Name], "Product Output") and [Extension] = ".xlsx" and not Text.Contains([Name], "精简")),

    // 一次性读取所有Excel内容并展开
    ExtractContent = Table.AddColumn(FilterAndCombine, "Data", each 
        let
            WorkbookContent = Excel.Workbook([Content], true),
            // 取第一个Sheet或按实际Sheet名调整
            FirstSheet = WorkbookContent{0}[Data]
        in
            FirstSheet
    ),

    // 移除不需要的列以减少内存占用
    RemoveColumns = Table.SelectColumns(ExtractContent, {"Name", "Date created", "Data"}),
    // 转换日期格式
    ConvertDate = Table.TransformColumns(RemoveColumns, {{"Date created", DateTime.Date, type date}}),
    // 展开数据并直接指定目标英文列名
    ExpandedData = Table.ExpandTableColumn(ConvertDate, "Data", 
        {
            "ERPCode", 
            "Material_Name", 
            "LogicalFlowPath", 
            "DateEnteredStep", 
            "Product_Name", 
            "ProductionOrder", 
            "ERPOperation", 
            "Step_Name", 
            "Resource", 
            "Resource Description", 
            "Area_Name", 
            "Step_In_PrimaryQuantity", 
            "Step_In_Date", 
            "TrackOutDate", 
            "TrackOut_PrimaryQuantity", 
            "Step_Duration_Minute",
            "First_TrackIn_Date ",
            "ERPProdSupervisor"    
        },
        {
            "PlantCode",
            "BatchNumber",
            "Group",
            "EnterStepTime",
            "CFN",
            "ProductionOrder",
            "Operation",
            "Operation description",
            "ResourceCode",
            "ResourceDescription",
            "ProductionArea",
            "StepInQuantity",
            "StepInTime",
            "TrackOutTime",
            "TrackOutQuantity",
            "StepDuration",
            "StartTime",
            "VSM"    
        }
    ),
    #"Filtered Rows" = Table.SelectRows(ExpandedData, each not Text.Contains([BatchNumber], "-")),
    #"Changed Type1" = Table.TransformColumnTypes(#"Filtered Rows",{{"TrackOutTime", type datetime}}),
    #"Replaced Value" = Table.ReplaceValue(#"Changed Type1","CZM ","",Replacer.ReplaceText,{"Operation description"}),
    #"Extracted Text Between Delimiters" = Table.TransformColumns(#"Replaced Value", {{"Group", each Text.BetweenDelimiters(_, " ", "/"), type text}}),
    // 工序名称标准化
    #"Standardize Step Name" = Table.TransformColumns(#"Extracted Text Between Delimiters", {
        {"Operation description", each
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
    #"Changed Type" = Table.TransformColumnTypes(#"Standardize Step Name",{ {"Operation description", type text}, {"Name", type text}, {"EnterStepTime", type datetime}, {"StepInTime", type datetime}, {"TrackOutTime", type datetime}, {"StartTime", type datetime}}),
    // 新增工序报工日期列，取工序报工时间的日期部分
    #"Add TrackOut Date Only" = Table.AddColumn(#"Changed Type", "TrackOutDate", each if [TrackOutTime] <> null then Date.From([TrackOutTime]) else null, type date),
    // 工序号补零为4位，批次号类型统一为文本
    MES_Normalized = Table.TransformColumns(#"Add TrackOut Date Only", {
        {"Operation", each Text.PadStart(Text.Trim(Text.From(_)), 4, "0"), type text},
        {"BatchNumber", each Text.Trim(Text.From(_)), type text}
    }),

    // 在计算CT之前：按Resource分组、按TrackOutTime排序并生成上一行时间TheoreticalStartTime，并基于CFN连续性计算SetUp
    AllColsBeforeGroup = Table.ColumnNames(MES_Normalized),
    ColsToExpandAfterGroup = List.Difference(AllColsBeforeGroup, {"ResourceCode"}),
    GroupedForPrev = Table.Group(MES_Normalized, {"ResourceCode"}, {
        {"GroupedData", each 
            let
                SortedData = Table.Sort(_, {{"TrackOutTime", Order.Ascending}}),
                // 当前表索引 0..n-1
                WithIndex = Table.AddIndexColumn(SortedData, "Index", 0, 1, Int64.Type),
                // 为了对齐上一行：复制并把索引整体+1，这样当前索引i就能匹配到上一行（i-1）
                PrevForJoin = Table.TransformColumns(WithIndex, {{"Index", each _ + 1, Int64.Type}}),
                PrevSlim = Table.SelectColumns(PrevForJoin, {"Index", "TrackOutTime", "CFN"}),
                Merged = Table.NestedJoin(WithIndex, {"Index"}, PrevSlim, {"Index"}, "Prev", JoinKind.LeftOuter),
                Expanded = Table.ExpandTableColumn(Merged, "Prev", {"TrackOutTime", "CFN"}, {"TheoreticalStartTime", "Prev_CFN"}),
                // 如果找不到上一个索引（TheoreticalStartTime为空），则使用StartTime作为理论开始时间
                WithFallbackStartTime = Table.AddColumn(Expanded, "TrackInTime", each if [TheoreticalStartTime] <> null then [TheoreticalStartTime] else [StartTime], type datetime),
                WithSetup = Table.AddColumn(WithFallbackStartTime, "Setup", each if [Prev_CFN] <> null and [CFN] = [Prev_CFN] then "No" else "Yes", type text),
                FinalResult = Table.RemoveColumns(WithSetup, {"Index", "Prev_CFN", "TheoreticalStartTime"})
            in
                FinalResult
        }
    }),
    MES_WithPrevDate = Table.ExpandTableColumn(GroupedForPrev, "GroupedData", ColsToExpandAfterGroup & {"TrackInTime", "Setup"}, ColsToExpandAfterGroup & {"TrackInTime", "Setup"}),
    // 合并SFC表，按批次号、工序号、产品号、工序名称
    MergedSFC = Table.NestedJoin(
        MES_WithPrevDate,
        {"BatchNumber", "Operation",  "Operation description"},
        e1_批次报工记录_SFC,
        {"Batch", "StepCode",  "StepName"},
        "SFCData",
        JoinKind.LeftOuter
    ),

    // 展开SFC的CheckInTime
    AddCheckinSFC = Table.ExpandTableColumn(MergedSFC, "SFCData", {"CheckInTime"}, {"Checkin_SFC"}),
    // 新增计算字段：LT (d) = 工序报工时间 - 进入工序时间（单位：天，保留两位小数），但如果ERP工序号为0010，则用工序报工时间-Checkin_SFC
    #"Add Step Duration Auto" = Table.AddColumn(AddCheckinSFC, "LT(d)", each 
        if [Operation] = "0010" then 
            (if [TrackOutTime] <> null and [Checkin_SFC] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [Checkin_SFC]), 2) else null)
        else 
            (if [TrackOutTime] <> null and [EnterStepTime] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [EnterStepTime]), 2) else null),
        type number),
    // 新增计算字段：PT(d) 优先使用Checkin_SFC，否则使用StartTime（单位：天，保留两位小数）
    #"Add Process Duration" = Table.AddColumn(#"Add Step Duration Auto", "PT(d)", each 
        if [TrackOutTime] <> null and [Checkin_SFC] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [Checkin_SFC]), 2)
        else if [TrackOutTime] <> null and [StartTime] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [StartTime]), 2)
        else null, type number),

    // 合并标准时间表（左连接，保留所有批次报工记录）
    MergedStandardTime = Table.NestedJoin(
        #"Add Process Duration",
        {"CFN", "Operation",  "Group"},
        e3_产品标准时间_exl,
        {"Material Number", "Operation", "Group"},
        "StandardData",
        JoinKind.LeftOuter
    ),

    // 展开标准时间字段，只取OEE、调试时间、Machine、Labor
    ExpandedStandardTime = Table.ExpandTableColumn(MergedStandardTime, "StandardData", {"OEE", "调试时间", "Machine", "Labor"}, {"OEE", "调试时间", "Machine", "Labor"}),
    // 将“调试时间”重命名为“Setup Time (h)”
    RenameSetupTime = Table.RenameColumns(ExpandedStandardTime, {{"调试时间", "Setup Time (h)"}}),

    // 设置OEE默认值为77%（如果OEE为空或等于0），直接写回OEE列
    FillOEE = Table.TransformColumns(RenameSetupTime, {{"OEE", each if _ = null or _ = 0 then 0.77 else _, type number}}),

    // 新增计算字段：如果Machine不为空且不等于0，则使用Machine，否则使用Labor
    AddEffectiveTime = Table.AddColumn(FillOEE, "Effective Time (h)", each 
        if [Machine] <> null and [Machine] <> 0 then [Machine] else [Labor], 
        type number),

    // 计算标准时间(ST) = 数量 * EffectiveTime / OEE，单位为天，如果SetUp为Yes则加上SetUpTime
    AddST = Table.AddColumn(AddEffectiveTime, "ST(d)", each 
        let
            qty = if [StepInQuantity] = null then 0 else [StepInQuantity],
            effectiveTime = if [#"Effective Time (h)"] = null then 0 else [#"Effective Time (h)"],
            oee = if [OEE] = null then 0.77 else [OEE],
            setupTime = if [Setup] = "Yes" and [#"Setup Time (h)"] <> null then [#"Setup Time (h)"] else 0,
            pptHours = setupTime + qty * effectiveTime / oee
        in
            if pptHours = 0 then null else Number.Round(pptHours / 24, 2),
        type number),

    // 计算应完工时间（加入30分钟换批，并顺延周末整天）
    AddDueDate = Table.AddColumn(AddST, "DueTime", each 
        let
            startTime = [TrackInTime],
            setupTime = if [Setup] = "Yes" and [#"Setup Time (h)"] <> null then [#"Setup Time (h)"] else 0,
            effectiveTime = if [#"Effective Time (h)"] = null then 0 else [#"Effective Time (h)"],
            qty = if [StepInQuantity] = null then 0 else [StepInQuantity],
            oee = if [OEE] = null then 0.77 else [OEE],
            totalHours = setupTime + effectiveTime * qty / oee,
            // 初算Due：加入0.5小时换批
            due0 = if startTime = null then null else DateTime.From(startTime) + #duration(0, Number.RoundDown(totalHours + 0.5), Number.RoundDown(((totalHours + 0.5) - Number.RoundDown(totalHours + 0.5))*60), 0),
            // 统计[start+1d, due]之间完整的周末天数，并迭代顺延直到稳定
            adjust = (s as datetime, d as datetime) as datetime =>
                let
                    // 计算两个日期之间的完整天列表（按日期比较）
                    startDate = Date.From(s),
                    endDate = Date.From(d),
                    // 从次日开始到截止日结束，构造日期列表
                    dates = if endDate <= startDate then {} else List.Dates(startDate + #duration(1,0,0,0), Duration.Days(endDate - startDate), #duration(1,0,0,0)),
                    isWeekend = (dd as date) as logical => Date.DayOfWeek(dd, Day.Sunday) = 0 or Date.DayOfWeek(dd, Day.Sunday) = 6,
                    weekendDays = List.Count(List.Select(dates, each isWeekend(_))),
                    d1 = d + #duration(weekendDays, 0, 0, 0),
                    // 二次检查，若顺延后仍跨周末，则递归继续
                    startDate1 = Date.From(s),
                    endDate1 = Date.From(d1),
                    dates1 = if endDate1 <= startDate1 then {} else List.Dates(startDate1 + #duration(1,0,0,0), Duration.Days(endDate1 - startDate1), #duration(1,0,0,0)),
                    weekendDays1 = List.Count(List.Select(dates1, each isWeekend(_))),
                    result = if weekendDays1 = weekendDays then d1 else d + #duration(weekendDays1, 0, 0, 0)
                in
                    result,
            dueFinal = if due0 = null then null else adjust(startTime, due0)
        in
            dueFinal
    ),

    // 计算完工状态
    // 新增周末扣除天数列
    AddWeekendDays = Table.AddColumn(AddDueDate, "Weekend(d)", each 
        let
            startTime = [TrackInTime],
            setupTime = if [Setup] = "Yes" and [#"Setup Time (h)"] <> null then [#"Setup Time (h)"] else 0,
            effectiveTime = if [#"Effective Time (h)"] = null then 0 else [#"Effective Time (h)"],
            qty = if [StepInQuantity] = null then 0 else [StepInQuantity],
            oee = if [OEE] = null then 0.77 else [OEE],
            totalHours = setupTime + effectiveTime * qty / oee,
            due0 = if startTime = null then null else DateTime.From(startTime) + #duration(0, Number.RoundDown(totalHours + 0.5), Number.RoundDown(((totalHours + 0.5) - Number.RoundDown(totalHours + 0.5))*60), 0),
            startDate = if startTime = null or due0 = null then null else Date.From(startTime),
            endDate = if startTime = null or due0 = null then null else Date.From(due0),
            dates = if startDate = null or endDate = null or endDate <= startDate then {} else List.Dates(startDate + #duration(1,0,0,0), Duration.Days(endDate - startDate), #duration(1,0,0,0)),
            isWeekend = (dd as date) as logical => Date.DayOfWeek(dd, Day.Sunday) = 0 or Date.DayOfWeek(dd, Day.Sunday) = 6,
            weekendDays = List.Count(List.Select(dates, each isWeekend(_))),
            d1 = if due0 = null then null else due0 + #duration(weekendDays, 0, 0, 0),
            endDate1 = if d1 = null then null else Date.From(d1),
            dates1 = if startDate = null or endDate1 = null or endDate1 <= startDate then {} else List.Dates(startDate + #duration(1,0,0,0), Duration.Days(endDate1 - startDate), #duration(1,0,0,0)),
            weekendDays1 = List.Count(List.Select(dates1, each isWeekend(_)))
        in
            if startTime = null then null else weekendDays1,
        Int64.Type),

    AddStatus = Table.AddColumn(AddWeekendDays, "CompletionStatus", each 
        let
            due = [DueTime],
            actual = [TrackOutTime]
        in
            if due = null or actual = null then null else if actual <= due+ #duration(0, 8, 0, 0) then "OnTime" else "Overdue"
    ),

    // 新增machine(#)列，从ResourceCode中提取数字
    AddMachineNumber = Table.AddColumn(AddStatus, "Machine(#)", each 
        let
            resourceCode = [ResourceCode],
            // 提取ResourceCode中的数字部分
            extractedNumber = if resourceCode = null then null else 
                Text.Combine(List.Select(Text.ToList(Text.From(resourceCode)), each List.Contains({"0".."9"}, _)))
        in
            if extractedNumber = "" then null else Number.From(extractedNumber),
        type number),

    // 重命名Machine和Labor列名，添加(h)后缀
    RenamedColumns = Table.RenameColumns(AddMachineNumber, {
        {"Machine", "Machine (h)"},
        {"Labor", "Labor (h)"}
    }),

    #"Reordered Columns1" = Table.ReorderColumns(RenamedColumns,{"Name", "Date created", "PlantCode", "BatchNumber", "Group", "CFN", "ProductionOrder", "Operation", "Operation description", "ResourceCode", "ResourceDescription", "ProductionArea", "StepInQuantity", "EnterStepTime", "StepInTime", "StartTime", "Checkin_SFC", "TrackOutTime", "TrackInTime", "DueTime", "Weekend(d)", "CompletionStatus", "ST(d)", "Setup", "TrackOutQuantity", "StepDuration", "VSM", "LT(d)", "PT(d)", "TrackOutDate", "OEE", "Setup Time (h)", "Machine (h)", "Labor (h)", "Effective Time (h)", "Machine(#)"}),
    // 统一类型：Time类为datetime，Date类为date，OEE为百分比，数量为整数，Machine和Labor为2位小数
    #"Changed Type Final" = Table.TransformColumnTypes(#"Reordered Columns1",
        {{"Date created", type date},
         {"TrackOutDate", type date},
         {"DueTime", type datetime},
         {"TrackInTime", type datetime},
         {"TrackOutTime", type datetime},
         {"StartTime", type datetime},
         {"StepInTime", type datetime},
         {"EnterStepTime", type datetime},
         {"Checkin_SFC", type datetime},
         {"OEE", Percentage.Type},
         {"StepInQuantity", Int64.Type},
         {"TrackOutQuantity", Int64.Type},
         {"Weekend(d)", Int64.Type},
         {"Machine (h)", type number},
         {"Labor (h)", type number},
         {"Setup Time (h)", type number},
         {"Machine(#)", Int64.Type},
         {"Effective Time (h)", type number}
        })
in
    #"Changed Type Final"