let 
    // 从SharePoint获取源数据 
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]), 
    
    // 筛选70-SFC文件夹下的数据
    FilterAndCombine = Table.SelectRows(Source, each Text.Contains([Folder Path], "70-SFC") and Text.StartsWith([Name], "LC-")),
    #"Filtered Rows1" = Table.SelectRows(FilterAndCombine, let latest = List.Max(FilterAndCombine[Date created]) in each [Date created] = latest or [Date created] > #datetime(2025, 4, 26, 0, 0, 0)),
    // 修改Excel内容读取逻辑
    ExtractContent = Table.AddColumn(#"Filtered Rows1", "Data", each  
        let 
            WorkbookContent = Excel.Workbook([Content], true), 
            FirstSheet = WorkbookContent{0}[Data],
            RawData = FirstSheet,
            // 目标表头（包含可选列“上道工序报工时间”）
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
    
    // 展开数据：优先包含可选列“上道工序报工时间”，若不存在则回退
    ExpandedData =
        let
            tryExpand = try Table.ExpandTableColumn(ConvertDate, "Data", {"产品类型", "产品号", "批次", "工序号", "工序名称", "报工时间", "上道工序报工时间", "Check In 时间"})
        in
            if tryExpand[HasError] then Table.ExpandTableColumn(ConvertDate, "Data", {"产品类型", "产品号", "批次", "工序号", "工序名称", "报工时间", "Check In 时间"}) else tryExpand[Value],

    // 规范主要字段命名（逐对尝试重命名，缺失则跳过）
    RenamedCols = 
        let
            renamePairs = {
                {"产品类型", "ProductType"},
                {"产品号", "ProductCode"},
                {"批次", "Batch"},
                {"工序号", "StepCode"},
                {"工序名称", "StepName"},
                {"报工时间", "ReportTime"},
                {"上道工序报工时间", "PrevReportTime"},
                {"Check In 时间", "CheckInTime"}
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

    // StepCode 去空格、转数字，失败为null，成功补零为4位
    CleanStepCode = Table.TransformColumns(
        EnsurePrevReportExists,
        {{"StepCode", each 
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
        CleanStepCode,
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

    // 按主键分组，保留每组最早的 CheckInTime
    GroupedSFC = Table.Group(
        FixCheckInTime,
        {"ProductType", "ProductCode", "Batch", "StepCode", "StepName"},
        {{"CheckInTime", each List.Min([CheckInTime]), type nullable datetime}}
    ),

    // 后续可继续标准化工序名称等处理
    // 例如：标准化工序名称
    StandardizedStepName = Table.TransformColumns(GroupedSFC, {
        {"StepName", each
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
    })
in
    StandardizedStepName