let
    // 从SharePoint获取文件并过滤所需文件
    Source = SharePoint.Files(
        "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction", 
        [ApiVersion = 15]
    ),
    SelectedSourceCols = Table.SelectColumns(
        Source,
        {"Content", "Name", "Folder Path", "Date created"}
    ),
    FilteredPath = Table.SelectRows(
        SelectedSourceCols, 
        each [Folder Path] = "https://medtronicapac.sharepoint.com/sites/ChangzhouOpsProduction/Shared Documents/General/POWER BI 数据源 V2/30-MES导出数据/CMES_WIP/"
    ),
    FilteredFiles = Table.SelectRows(
        FilteredPath, 
        each Text.StartsWith([Name], "CMES_WIP_CKH") or [Name] = ""
    ),
    BufferedFiles = Table.Buffer(FilteredFiles),

    // 筛选最近7天的数据
    LatestDate = List.Max(BufferedFiles[Date created]),
    FilteredDates = Table.SelectRows(
        BufferedFiles, 
        each Date.IsInPreviousNDays([Date created], 7) or [Date created] = LatestDate
    ),

    // 处理Excel文件内容
    WithExcelData = Table.AddColumn(FilteredDates, "raw_data", each Excel.Workbook([Content], true)),
    ExpandedData = Table.ExpandTableColumn(
        WithExcelData, 
        "raw_data", 
        {"Name", "Data", "Item"}, 
        {"SheetName", "Data", "Item"}
    ),
    ExportSheet = Table.SelectRows(ExpandedData, each [SheetName] = "Export"),
    SelectedColumns = Table.SelectColumns(ExportSheet, {"Data", "Name","Date created"}),

    // 提取日期并转换类型
    ExtractedDate = Table.TransformColumns(
        SelectedColumns, 
        {{
            "Name", 
            each let
                digits = Text.Select(_, {"0".."9"}),
                ymd = if Text.Length(digits) >= 8 then Text.End(digits, 8) else null,
                y = if ymd <> null then Number.FromText(Text.Start(ymd, 4)) else null,
                m = if ymd <> null then Number.FromText(Text.Middle(ymd, 4, 2)) else null,
                d = if ymd <> null then Number.FromText(Text.End(ymd, 2)) else null,
                dt = if y <> null and m <> null and d <> null then #datetime(y, m, d, 8, 0, 0) else null
            in
                try dt otherwise null,
            type datetime
        }}
    ),

    // 重命名Name列为报表日期
    RenamedDate = Table.RenameColumns(ExtractedDate, {{"Name", "报表日期"}}),
    #"Expanded {0}" = Table.ExpandTableColumn(RenamedDate, "Data", {"ERPCode", "ProductionOrder", "Material Name", "OrderType", "PO State", "ERPProdSupervisor", "ERPMRPController", "Product Number", "DrawingProductNumber", "Product Description", "ProductionVersion", "ParentOrder", "OnHold", "MaterialQty", "Unit", "OrderQty", "Step", "TrackInDate", "TrackOutDate", "DateEnteredStep", "LastProcessedTime", "Material State", "PO_UniversalState", "Material_UniversalState"}, {"ERPCode", "ProductionOrder", "Material Name", "OrderType", "PO State", "ERPProdSupervisor", "ERPMRPController", "Product Number", "DrawingProductNumber", "Product Description", "ProductionVersion", "ParentOrder", "OnHold", "MaterialQty", "Unit", "OrderQty", "Step", "TrackInDate", "TrackOutDate", "DateEnteredStep", "LastProcessedTime", "Material State", "PO_UniversalState", "Material_UniversalState"}),

    // 重命名列
    RenamedFields = Table.RenameColumns(
        #"Expanded {0}",
        {
            
            {"ProductionOrder", "生产流转卡号"},
            {"Material Name", "批次"},


            {"ERPProdSupervisor", "生产单元"},
            {"ERPMRPController", "MRP控制员"},
            {"Product Number", "物料编码"},
            {"DrawingProductNumber", "产品代码"},
            {"Product Description", "产品名称"},

            {"OnHold", "冻结"},
            {"MaterialQty", "数量"},
            {"Unit", "单位"},
            {"OrderQty", "订单数量"},
            {"Step", "工序名称"},
            {"TrackInDate", "开工日期时间"},
            {"TrackOutDate", "完工日期"},
            {"DateEnteredStep", "进入工序时间"},
            {"LastProcessedTime", "最后更新时间"},
            {"Material State", "工序状态"},
            {"PO_UniversalState", "工单通用状态"},
            {"Material_UniversalState", "物料通用状态"}
        }
    ),
    #"Removed Columns" = Table.RemoveColumns(RenamedFields,{"ERPCode", "OrderType", "PO State", "MRP控制员", "ProductionVersion", "ParentOrder", "单位", "工单通用状态", "物料通用状态"}),
    #"Filtered Rows" = #"Removed Columns",

    // 添加进工序天数列
    AddedDaysInProcess = Table.AddColumn(
        #"Filtered Rows",
        "进工序天数",
        each Number.RoundDown(Duration.TotalHours([报表日期] - [进入工序时间]) / 24, 1),
        type number
    ),

    TypeConverted = Table.TransformColumnTypes(
        AddedDaysInProcess,
        {
            {"进工序天数", type number},
            {"开工日期时间", type datetime}
        }
    ),

    // 处理null值和2024年之前的开工日期
    FinalResult = Table.AddColumn(
        TypeConverted,
        "开工日期",
        each if [开工日期时间] = null or [开工日期时间] < #datetime(2024, 1, 1, 0, 0, 0) 
             then null 
             else [开工日期时间],
        type datetime
    ),
    #"Added Conditional Column" = Table.AddColumn(
        FinalResult, 
        "产品类型", 
        each 
            if [生产单元] = null then "无区域"
            else if [生产单元] = "INS" then "INS类型"
            else if [生产单元] = "SPN" then "SPN类型"
            else if [生产单元] = "STR" then "STR类型"
            else if [生产单元] = "SUP" then "SUP类型"
            else if [生产单元] = "TRA" then "TRA类型"
            else "其他"
    ),
    #"Split Column by Delimiter" = Table.SplitColumn(#"Added Conditional Column", "工序名称", Splitter.SplitTextByEachDelimiter({" "}, QuoteStyle.Csv, false), {"工序名称.1", "工序名称.2"}),
    #"Renamed Columns" = Table.RenameColumns(#"Split Column by Delimiter",{{"工序名称.2", "工序名称"}}),
    #"Renamed Operation No" = Table.RenameColumns(#"Renamed Columns", {{"工序名称.1", "工序号"}}, MissingField.Ignore),
    #"Added Custom" = Table.AddColumn(#"Renamed Operation No", "plant", each 9997),
    #"Merged Queries" = Table.NestedJoin(#"Added Custom", {"工序名称", "plant"}, dim_operation_mapping, {"operation_name", "erp_code"}, "dim_operation_mapping", JoinKind.LeftOuter),
    #"Expanded {0}1" = Table.ExpandTableColumn(#"Merged Queries", "dim_operation_mapping", {"area", "lead_time"}, {"dim_operation_mapping.area", "dim_operation_mapping.lead_time"}),
    #"Replaced Value" = Table.ReplaceValue(#"Expanded {0}1",null,"5",Replacer.ReplaceValue,{"dim_operation_mapping.lead_time"}),
    #"Changed Type" = Table.TransformColumnTypes(#"Replaced Value",{{"dim_operation_mapping.lead_time", Int64.Type}}),
    #"Added Conditional Column1" = Table.AddColumn(#"Changed Type", "超期状态", each if [进工序天数] > [dim_operation_mapping.lead_time] then "超期" else "正常"),
    #"Reordered Columns" = Table.ReorderColumns(#"Added Conditional Column1",{"生产流转卡号", "批次", "生产单元", "物料编码", "产品代码", "产品名称", "冻结", "数量", "订单数量", "工序号", "工序名称", "完工日期", "进入工序时间", "开工日期时间", "最后更新时间", "工序状态", "报表日期", "Date created", "进工序天数", "开工日期", "产品类型", "超期状态"}),
    #"Duplicated Column" = Table.DuplicateColumn(#"Reordered Columns", "工序名称", "生产工序")
in
    #"Duplicated Column"