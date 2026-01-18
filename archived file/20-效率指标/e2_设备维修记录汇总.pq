let
    // 优化1：使用更精确的SharePoint文件过滤，减少不必要的数据传输
    Source = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]),
    FilteredFiles = Table.SelectRows(
        Source, 
        each Text.Contains([Folder Path], "60-设备台账") and Text.Contains([Name], "维修状态")
    ),

    // 优化2：只处理第一个匹配的文件，避免处理多个文件
    FirstFile = FilteredFiles{0},
    FileModifiedTime = FirstFile[Date modified],
    
    // 优化3：直接处理Excel内容，减少中间步骤
    RawData = Excel.Workbook(FirstFile[Content], true){0}[Data],

    // 优化4：提前过滤无效数据，减少后续处理量
    FilteredRawData = Table.SelectRows(
        RawData,
        each [订单] <> null and [系统状态] <> "" and [创建日期] > #date(2025, 5, 1)
    ),

    // 优化5：一次性进行列选择和重命名，减少表操作次数
    ProcessedColumns = Table.TransformColumnTypes(
        Table.RenameColumns(
            Table.SelectColumns(
                FilteredRawData,
                {
                    "订单",
                    "描述", 
                    "订单类型",
                    "系统状态",
                    "技术标识号",
                    "技术对象描述",
                    "维护工厂",
                    "输入者",
                    "创建日期",
                    "基本的开始时间",
                    "更改订单主文件日期",
                    "参考时间"
                }
            ),
            {
                {"基本的开始时间", "创建时间"},
                {"更改订单主文件日期", "完成日期"},
                {"参考时间", "完成时间"}
            }
        ),
        {
            {"订单", Int64.Type},
            {"描述", type text},
            {"订单类型", type text},
            {"系统状态", type text},
            {"技术标识号", type text}, // 提前转换为文本类型
            {"技术对象描述", type text},
            {"维护工厂", Int64.Type},
            {"输入者", type text},
            {"创建日期", type date},
            {"创建时间", type time},
            {"完成日期", type date},
            {"完成时间", type time}
        }
    ),

    // 优化6：添加维修状态和停机状态列
    WithStatusColumns = Table.AddColumn(
        Table.AddColumn(
            ProcessedColumns,
            "维修状态",
            each if Text.Contains([系统状态], "REL") then "维修中" else "已完成",
            type text
        ),
        "Down Status停机状态",
        each null, // 60-设备台账的维修记录默认为null，表示需要计算停机时间
        type text
    ),

    // 优化7：合并维修记录和SFC数据
    // 首先获取SFC维修记录数据
    SFCRepairData = let
        // 获取SharePoint文件
        SFCRepairSource = SharePoint.Files("https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", [ApiVersion = 15]),
        
        // 筛选SFC维修文件夹
        SFCRepairFiltered = Table.SelectRows(SFCRepairSource, each Text.Contains([Folder Path], "70-SFC") and Text.Contains([Folder Path], "维修")),
        
        // 筛选最新日期文件
        SFCRepairLatest = Table.SelectRows(SFCRepairFiltered, let latest = List.Max(SFCRepairFiltered[Date created]) in each [Date created] = latest),
        
        // 添加原始文件日期
        SFCRepairWithDate = Table.AddColumn(SFCRepairLatest, "文件更新日期", each [Date created]),
        
        // 读取Excel内容
        SFCRepairExcel = Table.AddColumn(SFCRepairWithDate, "raw_data", each Excel.Workbook([Content],true)),
        SFCRepairExpanded = Table.ExpandTableColumn(SFCRepairExcel, "raw_data", {"Name", "Data", "Item"}, {"Name.1", "Data", "Item"}),
        SFCRepairFilteredRows = Table.SelectRows(SFCRepairExpanded, each ([Item] = "设备维修台账")),
        SFCRepairDataTable = SFCRepairFilteredRows{0}[Data],
        SFCRepairRemovedColumns = Table.RemoveColumns(SFCRepairDataTable,{"Reference date Month/day#(lf)参考日期", "Reference time#(lf)参考时间", "Repair Start Time#(lf)修理开始时间月/日"}),
        
        // 转换数据类型
        SFCRepairConverted = Table.TransformColumnTypes(SFCRepairRemovedColumns,{{"Actual order date Month/day#(lf)实际下达日期", type date}, {"Basic start time#(lf)基本开始时间", type time}, {"Repair finish Time#(lf)修理完成时间月/日", type date}, {"Downtime#(lf)停机时长#(lf)（H）", type number}, {"Application Department Confirmation/Date#(lf)申请部门确认/日期", type date}, {"Application Department Confirmation/Date#(lf)申请部门确认/时间", type time}}),
        
        // 添加维修状态
        SFCRepairWithStatus = Table.AddColumn(SFCRepairConverted, "维修状态", each if [#"Application Department Confirmation/Date#(lf)申请部门确认/时间"] <> null then "已完成" else "维修中"),
        
        // 重命名列
        SFCRepairRenamed = Table.RenameColumns(SFCRepairWithStatus,{{"Orde#(lf)订单", "订单"}, {"Plant#(lf)工厂", "维护工厂"}, {"Technical identification number#(lf)技术标识号", "技术标识号"}, {"Technical object description#(lf)技术对象描述", "技术对象描述"}, {"Actual order date Month/day#(lf)实际下达日期", "创建日期"}, {"Basic start time#(lf)基本开始时间", "创建时间"}, {"Failure description#(lf)故障描述", "描述"}, {"Repair personnel#(lf)维修人员", "维修人员"}, {"Fault Cause #(lf)故障原因", "故障原因"}, {"Application Department Confirmation/Date#(lf)申请部门确认/日期", "完成日期"}, {"Application Department Confirmation/Date#(lf)申请部门确认/时间", "完成时间"}, {"Application Department Confirmation/personnel#(lf)申请部门确认人", "确认人"}, {"Down Status#(lf)停机状态", "Down Status停机状态"}}),
        
        // 保持原有的停机状态列，不修改其值
        SFCRepairWithDownStatus = SFCRepairRenamed
    in
        SFCRepairWithDownStatus,
    
    // 合并维修记录和SFC维修记录数据
    AppendedQuery = Table.Combine({WithStatusColumns, SFCRepairData}),
    
    // 优化8：使用更高效的停机时间计算，并添加停机状态条件判断
    CurrentDateTime = DateTime.LocalNow(),
    WithDowntimeCalculation = Table.AddColumn(
        AppendedQuery,
        "停机时间(H)",
        each let
            // 直接判断停机状态值：停机和空值则计算，非停机则不计算
            IsDownStatus = [Down Status停机状态] = "停机" or [Down Status停机状态] = null,
            // 如果非停机状态，直接返回null
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
    
    // 优化9：添加停机天数列，重新计算以避免循环引用
    WithDowntimeDays = Table.AddColumn(
        WithDowntimeCalculation,
        "停机天数(d)",
        each let
            // 直接判断停机状态值：停机和空值则计算，非停机则不计算
            IsDownStatus = [Down Status停机状态] = "停机" or [Down Status停机状态] = null,
            // 重新计算停机时间以避免引用问题
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
                else Number.Round(Duration.TotalHours(EndDateTime - StartDateTime), 2),
            // 计算停机天数
            DaysDiff = if IsDownStatus and HoursDiff <> null
                then Number.Round(HoursDiff / 24, 2)
                else null
        in
            DaysDiff,
        type number
    ),

    // 优化10：添加文件修改时间
    WithFileModifiedTime = Table.AddColumn(
        WithDowntimeDays,
        "文件修改时间",
        each FileModifiedTime,
        type datetime
    ),

    // 优化11：使用Join操作替代逐行查找，提高性能
    // 创建设备台账查找表，只包含需要的列
    EquipmentLookup = Table.SelectColumns(
        d2_常州园区设备台账,
        {"Equipment ID设备编号", "所属单元"}
    ),
    
    // 使用LeftJoin进行关联，比逐行查找效率更高
    WithUnitInfo = Table.Join(
        WithFileModifiedTime,
        {"技术标识号"},
        EquipmentLookup,
        {"Equipment ID设备编号"},
        JoinKind.LeftOuter
    ),
    
    // 优化12：清理和重命名列
    FinalResult = Table.RenameColumns(
        Table.ReplaceValue(
            Table.RemoveColumns(WithUnitInfo, {"Equipment ID设备编号"}),
            each [所属单元],
            each if [所属单元] = null or [所属单元] = "" then "计量设备" else [所属单元],
            Replacer.ReplaceValue,
            {"所属单元"}
        ),
        {{"所属单元", "所属单元"}}
    )
in
    FinalResult