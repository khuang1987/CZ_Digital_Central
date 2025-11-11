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
            "TrackOutDate", 
            "TrackOut_PrimaryQuantity", 
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
            "TrackOutTime",
            "TrackOutQuantity",
            "TrackInTime",
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
    #"Changed Type" = Table.TransformColumnTypes(#"Standardize Step Name",{ {"Operation description", type text}, {"Name", type text}, {"EnterStepTime", type datetime}, {"TrackOutTime", type datetime}, {"TrackInTime", type datetime}}),
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
                // 如果找不到上一个索引（TheoreticalStartTime为空），则使用TrackInTime作为理论开始时间
                WithFallbackStartTime = Table.AddColumn(Expanded, "TrackInTime_Updated", each if [TheoreticalStartTime] <> null then [TheoreticalStartTime] else [TrackInTime], type datetime),
                RemoveOldTrackInTime = Table.RemoveColumns(WithFallbackStartTime, {"TrackInTime"}),
                RenameTrackInTime = Table.RenameColumns(RemoveOldTrackInTime, {{"TrackInTime_Updated", "TrackInTime"}}),
                WithSetup = Table.AddColumn(RenameTrackInTime, "Setup", each if [Prev_CFN] <> null and [CFN] = [Prev_CFN] then "No" else "Yes", type text),
                FinalResult = Table.RemoveColumns(WithSetup, {"Index", "Prev_CFN", "TheoreticalStartTime"})
            in
                FinalResult
        }
    }),
    MES_WithPrevDate = Table.ExpandTableColumn(GroupedForPrev, "GroupedData", ColsToExpandAfterGroup & {"Setup"}, ColsToExpandAfterGroup & {"Setup"}),

    // 按功能分组重新排序字段
    #"Reordered Columns" = Table.ReorderColumns(MES_WithPrevDate, {
        // 文件信息
        "Name", 
        "Date created",
        // 批次和订单信息
        "PlantCode",
        "BatchNumber", 
        "Group",
        "ProductionOrder",
        "CFN",
        // 工序信息
        "Operation",
        "Operation description",
        // 资源信息
        "ResourceCode",
        "ResourceDescription",
        "ProductionArea",
        // 时间信息
        "TrackInTime",
        "EnterStepTime",
        "TrackOutTime",
        "TrackOutDate",
        // 数量信息
        "StepInQuantity",
        "TrackOutQuantity",
        // 其他
        "Setup",
        "VSM"
    })
in
    #"Reordered Columns"
