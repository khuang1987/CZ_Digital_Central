let
    // 以上一查询的结果作为输入（请确保在模型中该查询名称可用）
    Source = e2_批次报工记录_MES_基础前处理,

    // 合并SFC表，按批次号、工序号、工序名称
    MergedSFC = Table.NestedJoin(
        Source,
        {"BatchNumber", "Operation",  "Operation description"},
        e1_批次报工记录_SFC,
        {"Batch", "StepCode",  "StepName"},
        "SFCData",
        JoinKind.LeftOuter
    ),

    // 展开SFC的CheckInTime
    AddCheckinSFC = Table.ExpandTableColumn(MergedSFC, "SFCData", {"CheckInTime"}, {"Checkin_SFC"}),

    // 新增计算字段：LT (d)
    #"Add Step Duration Auto" = Table.AddColumn(AddCheckinSFC, "LT(d)", each 
        if [Operation] = "0010" then 
            (if [TrackOutTime] <> null and [Checkin_SFC] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [Checkin_SFC]), 2) else null)
        else 
            (if [TrackOutTime] <> null and [EnterStepTime] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [EnterStepTime]), 2) else null),
        type number),

    // 新增计算字段：PT(d)
    #"Add Process Duration" = Table.AddColumn(#"Add Step Duration Auto", "PT(d)", each 
        if [TrackOutTime] <> null and [Checkin_SFC] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [Checkin_SFC]), 2)
        else if [TrackOutTime] <> null and [TrackInTime] <> null then Number.Round(Duration.TotalDays([TrackOutTime] - [TrackInTime]), 2)
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

    // 设置OEE默认值为77%
    FillOEE = Table.TransformColumns(RenameSetupTime, {{"OEE", each if _ = null or _ = 0 then 0.77 else _, type number}}),

    // 新增Effective Time
    AddEffectiveTime = Table.AddColumn(FillOEE, "Effective Time (h)", each 
        if [Machine] <> null and [Machine] <> 0 then [Machine] else [Labor], 
        type number),

    // 计算标准时间(ST)
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
            // 第一步：计算基础完工时间（due0）= 开始时间 + 总工时 + 0.5小时换批时间
            due0 = if startTime = null then null else DateTime.From(startTime) + #duration(0, Number.RoundDown(totalHours + 0.5), Number.RoundDown(((totalHours + 0.5) - Number.RoundDown(totalHours + 0.5))*60), 0),
            // 第二步：定义周末调整函数，自动顺延周末
            adjust = (s as datetime, d as datetime) as datetime =>
                let
                    // 2.1 提取开始日期和截止日期
                    startDate = Date.From(s),
                    endDate = Date.From(d),
                    // 2.2 生成日期列表：从开始时间的次日到截止日期当日（包含）
                    // 注意：只统计完整的天数，所以从startDate+1天开始
                    dates = if endDate <= startDate then {} else List.Dates(startDate + #duration(1,0,0,0), Duration.Days(endDate - startDate), #duration(1,0,0,0)),
                    // 2.3 判断是否为周末：周日(Day.Sunday=0)或周六(Day.Sunday=6)
                    isWeekend = (dd as date) as logical => Date.DayOfWeek(dd, Day.Sunday) = 0 or Date.DayOfWeek(dd, Day.Sunday) = 6,
                    // 2.4 统计该区间内的周末天数
                    weekendDays = List.Count(List.Select(dates, each isWeekend(_))),
                    // 2.5 第一次顺延：将截止时间向后顺延weekendDays天
                    d1 = d + #duration(weekendDays, 0, 0, 0),
                    // 2.6 二次检查：重新计算顺延后的日期区间内的周末天数
                    startDate1 = Date.From(s),
                    endDate1 = Date.From(d1),
                    dates1 = if endDate1 <= startDate1 then {} else List.Dates(startDate1 + #duration(1,0,0,0), Duration.Days(endDate1 - startDate1), #duration(1,0,0,0)),
                    weekendDays1 = List.Count(List.Select(dates1, each isWeekend(_))),
                    // 2.7 稳定性检查：
                    // - 如果二次计算的周末天数等于第一次计算的天数，说明已经稳定，返回d1
                    // - 否则，继续顺延weekendDays1天（但这种情况应该很少，因为理论上最多一次迭代）
                    result = if weekendDays1 = weekendDays then d1 else d + #duration(weekendDays1, 0, 0, 0)
                in
                    result,
            // 第三步：应用周末调整，得到最终的应完工时间
            dueFinal = if due0 = null then null else adjust(startTime, due0)
        in
            dueFinal
    ),

    // 新增周末扣除天数列
    // 说明：此字段用于记录计算过程中实际扣除的周末天数，逻辑与AddDueDate中的adjust函数类似
    // 但返回的是周末天数（整数），而不是调整后的时间
    AddWeekendDays = Table.AddColumn(AddDueDate, "Weekend(d)", each 
        let
            startTime = [TrackInTime],
            setupTime = if [Setup] = "Yes" and [#"Setup Time (h)"] <> null then [#"Setup Time (h)"] else 0,
            effectiveTime = if [#"Effective Time (h)"] = null then 0 else [#"Effective Time (h)"],
            qty = if [StepInQuantity] = null then 0 else [StepInQuantity],
            oee = if [OEE] = null then 0.77 else [OEE],
            totalHours = setupTime + effectiveTime * qty / oee,
            // 第一步：计算基础完工时间（与AddDueDate保持一致）
            due0 = if startTime = null then null else DateTime.From(startTime) + #duration(0, Number.RoundDown(totalHours + 0.5), Number.RoundDown(((totalHours + 0.5) - Number.RoundDown(totalHours + 0.5))*60), 0),
            // 第二步：提取日期并生成日期列表
            startDate = if startTime = null or due0 = null then null else Date.From(startTime),
            endDate = if startTime = null or due0 = null then null else Date.From(due0),
            // 从开始时间的次日到基础完工日期当日
            dates = if startDate = null or endDate = null or endDate <= startDate then {} else List.Dates(startDate + #duration(1,0,0,0), Duration.Days(endDate - startDate), #duration(1,0,0,0)),
            // 第三步：判断周末并统计
            isWeekend = (dd as date) as logical => Date.DayOfWeek(dd, Day.Sunday) = 0 or Date.DayOfWeek(dd, Day.Sunday) = 6,
            weekendDays = List.Count(List.Select(dates, each isWeekend(_))),
            // 第四步：计算顺延后的截止时间
            d1 = if due0 = null then null else due0 + #duration(weekendDays, 0, 0, 0),
            // 第五步：重新计算顺延后区间内的周末天数（二次迭代检查）
            endDate1 = if d1 = null then null else Date.From(d1),
            dates1 = if startDate = null or endDate1 = null or endDate1 <= startDate then {} else List.Dates(startDate + #duration(1,0,0,0), Duration.Days(endDate1 - startDate), #duration(1,0,0,0)),
            // 第六步：返回二次迭代后的周末天数（确保稳定值）
            // 注意：如果顺延后区间包含了新的周末，则返回weekendDays1，否则返回weekendDays
            // 但实际上这里的逻辑返回的是weekendDays1，即最终稳定后的周末天数
            weekendDays1 = List.Count(List.Select(dates1, each isWeekend(_)))
        in
            if startTime = null then null else weekendDays1,
        Int64.Type),

    AddStatus = Table.AddColumn(AddWeekendDays, "CompletionStatus", each 
        let
            due = [DueTime],
            actual = [TrackOutTime]
        in
            if due = null or actual = null then null else if actual <= due + #duration(0, 8, 0, 0) then "OnTime" else "Overdue"
    ),

    // 新增machine(#)列，从ResourceCode中提取数字
    AddMachineNumber = Table.AddColumn(AddStatus, "Machine(#)", each 
        let
            resourceCode = [ResourceCode],
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

    #"Reordered Columns1" = Table.ReorderColumns(RenamedColumns,{"Name", "Date created", "PlantCode", "BatchNumber", "Group", "CFN", "ProductionOrder", "Operation", "Operation description", "ResourceCode", "ResourceDescription", "ProductionArea", "StepInQuantity", "EnterStepTime", "Checkin_SFC", "TrackInTime", "TrackOutTime", "TrackOutDate", "DueTime", "Weekend(d)", "CompletionStatus", "ST(d)", "Setup", "TrackOutQuantity", "VSM", "LT(d)", "PT(d)", "OEE", "Setup Time (h)", "Machine (h)", "Labor (h)", "Effective Time (h)", "Machine(#)"}),

    #"Changed Type Final" = Table.TransformColumnTypes(#"Reordered Columns1",
        {{"Date created", type date},
         {"TrackOutDate", type date},
         {"DueTime", type datetime},
         {"TrackInTime", type datetime},
         {"TrackOutTime", type datetime},
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
