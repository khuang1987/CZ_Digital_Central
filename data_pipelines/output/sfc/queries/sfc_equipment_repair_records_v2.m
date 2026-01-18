let
    // ==================== 数据源1：60-设备台账维修状态 ====================
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]),
    
    // 筛选设备台账维修状态文件
    FilteredFiles = Table.SelectRows(
        Source, 
        each Text.Contains([Folder Path], "60-设备台账") and Text.Contains([Name], "维修状态")
    ),
    
    // 安全获取第一个文件（添加空值检查）
    EquipmentRepairData = if Table.RowCount(FilteredFiles) = 0 
        then #table({"订单", "描述", "订单类型", "系统状态", "技术标识号", "技术对象描述", "维护工厂", "输入者", "创建日期", "创建时间", "完成日期", "完成时间", "维修状态", "Down Status停机状态", "文件修改时间"}, {})
        else let
            FirstFile = FilteredFiles{0},
            FileModifiedTime = FirstFile[Date modified],
            
            // 读取Excel内容
            ExcelContent = Excel.Workbook(FirstFile[Content], true),
            FirstSheet = if Table.RowCount(ExcelContent) > 0 then ExcelContent{0}[Data] else #table({}, {}),
            
            // 过滤无效数据
            FilteredRawData = Table.SelectRows(
                FirstSheet,
                each [订单] <> null and [系统状态] <> "" and [创建日期] > #date(2025, 5, 1)
            ),
            
            // 列选择和重命名
            ProcessedColumns = Table.TransformColumnTypes(
                Table.RenameColumns(
                    Table.SelectColumns(
                        FilteredRawData,
                        {"订单", "描述", "订单类型", "系统状态", "技术标识号", "技术对象描述", 
                         "维护工厂", "输入者", "创建日期", "基本的开始时间", "更改订单主文件日期", "参考时间"}
                    ),
                    {{"基本的开始时间", "创建时间"}, {"更改订单主文件日期", "完成日期"}, {"参考时间", "完成时间"}}
                ),
                {{"订单", Int64.Type}, {"描述", type text}, {"订单类型", type text}, {"系统状态", type text},
                 {"技术标识号", type text}, {"技术对象描述", type text}, {"维护工厂", Int64.Type}, {"输入者", type text},
                 {"创建日期", type date}, {"创建时间", type time}, {"完成日期", type date}, {"完成时间", type time}}
            ),
            
            // 添加维修状态和停机状态列
            WithStatusColumns = Table.AddColumn(
                Table.AddColumn(
                    ProcessedColumns,
                    "维修状态",
                    each if Text.Contains([系统状态], "REL") then "维修中" else "已完成",
                    type text
                ),
                "Down Status停机状态",
                each null,
                type text
            ),
            
            // 添加文件修改时间
            WithFileTime = Table.AddColumn(WithStatusColumns, "文件修改时间", each FileModifiedTime, type datetime)
        in
            WithFileTime,
    
    // ==================== 数据源2：70-SFC维修记录 ====================
    SFCRepairData = let
        // 筛选SFC维修文件夹
        SFCRepairFiltered = Table.SelectRows(
            Source, 
            each Text.Contains([Folder Path], "70-SFC") and Text.Contains([Folder Path], "维修") and [Extension] = ".xlsx"
        ),
        
        // 检查是否有文件
        Result = if Table.RowCount(SFCRepairFiltered) = 0 
            then #table({"订单", "维护工厂", "技术标识号", "技术对象描述", "创建日期", "创建时间", "描述", "维修人员", "故障原因", "完成日期", "完成时间", "确认人", "Down Status停机状态", "维修状态", "文件修改时间"}, {})
            else let
                // 筛选最新日期文件
                LatestDate = List.Max(SFCRepairFiltered[Date created]),
                SFCRepairLatest = Table.SelectRows(SFCRepairFiltered, each [Date created] = LatestDate),
                FirstSFCFile = SFCRepairLatest{0},
                SFCFileModifiedTime = FirstSFCFile[Date modified],
                
                // 读取Excel内容
                SFCExcelContent = Excel.Workbook(FirstSFCFile[Content], true),
                
                // 查找目标Sheet（兼容两种名称）
                TargetSheet = Table.SelectRows(SFCExcelContent, each [Item] = "MaintainOrderView" or [Item] = "设备维修台账"),
                
                // 安全获取数据
                ProcessedData = if Table.RowCount(TargetSheet) = 0 
                    then #table({}, {})
                    else let
                        SheetData = TargetSheet{0}[Data],
                        
                        // 移除不需要的列（使用try处理可能不存在的列）
                        ColumnsToRemove = {"Reference date Month/day#(lf)参考日期", "Reference time#(lf)参考时间", "Repair Start Time#(lf)修理开始时间月/日"},
                        ExistingColumnsToRemove = List.Select(ColumnsToRemove, each List.Contains(Table.ColumnNames(SheetData), _)),
                        RemovedColumns = if List.Count(ExistingColumnsToRemove) > 0 
                            then Table.RemoveColumns(SheetData, ExistingColumnsToRemove) 
                            else SheetData,
                        
                        // 转换数据类型
                        Converted = Table.TransformColumnTypes(RemovedColumns, {
                            {"Actual order date Month/day#(lf)实际下达日期", type date}, 
                            {"Basic start time#(lf)基本开始时间", type time}, 
                            {"Repair finish Time#(lf)修理完成时间月/日", type date}, 
                            {"Downtime#(lf)停机时长#(lf)（H）", type number}, 
                            {"Application Department Confirmation/Date#(lf)申请部门确认/日期", type date}, 
                            {"Application Department Confirmation/Date#(lf)申请部门确认/时间", type time}
                        }),
                        
                        // 添加维修状态
                        WithStatus = Table.AddColumn(Converted, "维修状态", 
                            each if [#"Application Department Confirmation/Date#(lf)申请部门确认/时间"] <> null then "已完成" else "维修中"),
                        
                        // 重命名列
                        Renamed = Table.RenameColumns(WithStatus, {
                            {"Orde#(lf)订单", "订单"}, 
                            {"Plant#(lf)工厂", "维护工厂"}, 
                            {"Technical identification number#(lf)技术标识号", "技术标识号"}, 
                            {"Technical object description#(lf)技术对象描述", "技术对象描述"}, 
                            {"Actual order date Month/day#(lf)实际下达日期", "创建日期"}, 
                            {"Basic start time#(lf)基本开始时间", "创建时间"}, 
                            {"Failure description#(lf)故障描述", "描述"}, 
                            {"Repair personnel#(lf)维修人员", "维修人员"}, 
                            {"Fault Cause #(lf)故障原因", "故障原因"}, 
                            {"Application Department Confirmation/Date#(lf)申请部门确认/日期", "完成日期"}, 
                            {"Application Department Confirmation/Date#(lf)申请部门确认/时间", "完成时间"}, 
                            {"Application Department Confirmation/personnel#(lf)申请部门确认人", "确认人"}, 
                            {"Down Status#(lf)停机状态", "Down Status停机状态"}
                        }),
                        
                        // 添加文件修改时间
                        WithFileTime = Table.AddColumn(Renamed, "文件修改时间", each SFCFileModifiedTime, type datetime)
                    in
                        WithFileTime
            in
                ProcessedData
    in
        Result,
    
    // ==================== 合并两个数据源 ====================
    // 过滤空表后合并
    TablesToMerge = List.Select({EquipmentRepairData, SFCRepairData}, each Table.RowCount(_) > 0),
    AppendedQuery = if List.Count(TablesToMerge) = 0 
        then #table({}, {}) 
        else Table.Combine(TablesToMerge),
    
    // ==================== 计算停机时间 ====================
    CurrentDateTime = DateTime.LocalNow(),
    WithDowntimeCalculation = Table.AddColumn(
        AppendedQuery,
        "停机时间(H)",
        each let
            IsDownStatus = [Down Status停机状态] = "停机" or [Down Status停机状态] = null,
            StartDateTime = if not IsDownStatus 
                then null
                else if [创建日期] = null or [创建时间] = null 
                    then null
                    else DateTime.FromText(
                        Text.From(Date.From([创建日期])) & " " & 
                        Time.ToText([创建时间], "HH:mm:ss")
                    ),
            EndDateTime = if not IsDownStatus or StartDateTime = null
                then null
                else if [维修状态] = "已完成" 
                    then if [完成日期] = null or [完成时间] = null
                        then null
                        else DateTime.FromText(
                            Text.From(Date.From([完成日期])) & " " & 
                            Time.ToText([完成时间], "HH:mm:ss")
                        )
                    else CurrentDateTime,
            HoursDiff = if StartDateTime = null or EndDateTime = null
                then null
                else Number.Round(Duration.TotalHours(EndDateTime - StartDateTime), 2)
        in
            HoursDiff,
        type number
    ),
    
    // 添加停机天数列
    WithDowntimeDays = Table.AddColumn(
        WithDowntimeCalculation,
        "停机天数(d)",
        each if [#"停机时间(H)"] <> null then Number.Round([#"停机时间(H)"] / 24, 2) else null,
        type number
    ),

    // ==================== 关联设备台账获取单元名称 ====================
    // 创建设备台账查找表
    EquipmentLookup = Table.SelectColumns(
        d00_常州园区设备台账,
        {"Equipment ID设备编号", "单元名称"}
    ),
    
    // 使用LeftJoin进行关联
    WithUnitInfo = Table.Join(
        WithDowntimeDays,
        {"技术标识号"},
        EquipmentLookup,
        {"Equipment ID设备编号"},
        JoinKind.LeftOuter
    ),
    
    // 清理和处理空值
    FinalResult = Table.ReplaceValue(
        Table.RemoveColumns(WithUnitInfo, {"Equipment ID设备编号"}, MissingField.Ignore),
        each [单元名称],
        each if [单元名称] = null or [单元名称] = "" then "计量设备" else [单元名称],
        Replacer.ReplaceValue,
        {"单元名称"}
    )
in
    FinalResult
