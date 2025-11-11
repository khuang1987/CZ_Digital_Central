let
    Source = Excel.Workbook(Web.Contents("https://medtronicemea.sharepoint.com/sites/GlobalManufacturing/Shared%20Documents/08%20-%20Strategy%20Governance%20and%20Execution%20Team/2.0%20Strategic%20Deployment/FY26%20Strategy%20(GM)/FY26%20Execution%20Plans-%20Sites/Asia%20China%20Region-%20FY26%20EP/Changzhou%20Campus%20-%20FY26%20Execution%20Plan.xlsx"), null, true),
    // 处理CZ表
    CZ_Sheet = Source{[Item="Execution Plan KPIs-CZ",Kind="Sheet"]}[Data],
    CZ_ChangedType5 = Table.TransformColumnTypes(CZ_Sheet,{{"Column1", type any}, {"Column2", type text}, {"Column3", type any}, {"Column4", type any}, {"Column5", type any}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type any}, {"Column10", type any}, {"Column11", type any}, {"Column12", type any}, {"Column13", type any}, {"Column14", type any}, {"Column15", type any}, {"Column16", type any}, {"Column17", type any}, {"Column18", type any}, {"Column19", type any}, {"Column20", type any}, {"Column21", type any}, {"Column22", type text}, {"Column23", type text}, {"Column24", type any}}),
    CZ_ChangedType = Table.TransformColumnTypes(CZ_ChangedType5,{{"Column1", type any}, {"Column2", type text}, {"Column3", type text}, {"Column4", type any}, {"Column5", type text}, {"Column6", type any}, {"Column7", type any}, {"Column8", type any}, {"Column9", type any}, {"Column10", type any}, {"Column11", type any}, {"Column12", type any}, {"Column13", type any}, {"Column14", type any}, {"Column15", type any}, {"Column16", type any}, {"Column17", type any}, {"Column18", type any}, {"Column19", type any}, {"Column20", type any}, {"Column21", type any}, {"Column22", type any}, {"Column23", type any}, {"Column24", type text}}),
    CZ_RemovedTopRows = Table.Skip(CZ_ChangedType,12),
    CZ_RemovedBottomRows = Table.RemoveLastN(CZ_RemovedTopRows,22),
    CZ_FilteredRows = Table.SelectRows(CZ_RemovedBottomRows, each ([Column2] <> "(Year 1: establish baseline)" and [Column2] <> "MPS Matruity Assessment Score")),
    CZ_PromotedHeaders = Table.PromoteHeaders(CZ_FilteredRows, [PromoteAllScalars=true]),
    CZ_FilledDown = Table.FillDown(CZ_PromotedHeaders,{"KPIs","Owner","KPI Tier Level to View"}),
    CZ_FilteredRows1 = Table.SelectRows(CZ_FilledDown, each Text.Contains([#"KPIs"], "#") or not Text.Contains([#"KPIs"], "%")),
    CZ_RemovedColumns = Table.RemoveColumns(CZ_FilteredRows1,{"Column1", "Column4", "FY26 YTD _1", "Column24", "If Red, Why?  Timing of KPI back to objective?  Link to A3 or KPI action Plan", "Comments #(lf)optional", "FY26 YE"}),
    CZ_RenamedColumns2 = Table.RenameColumns(CZ_RemovedColumns,{{"Column8", "Type"}}),
    CZ_UnpivotedOtherColumns = Table.UnpivotOtherColumns(CZ_RenamedColumns2, {"KPIs", "Owner","KPI Tier Level to View","FY26 YTD ", "Type"}, "属性", "值"),
    CZ_RenamedColumns = Table.RenameColumns(CZ_UnpivotedOtherColumns,{{"属性", "Month"}}),
    MonthList = {"MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC", "JAN", "FEB", "MAR", "APR"},
    CZ_AddDateColumn = Table.AddColumn(CZ_RenamedColumns, "日期", each 
        let
            idx = List.PositionOf(MonthList, [Month]),
            baseYear = if idx < 8 then 2025 else 2026,
            baseMonth = if idx < 8 then idx + 5 else idx - 7
        in
            if idx <> -1 then #date(baseYear, baseMonth, 15) else null,
        type date
    ),
    CZ_ChangedType2 = Table.TransformColumnTypes(CZ_AddDateColumn,{{"日期", type date}, {"值", type number}}),
    CZ_RoundedValues = Table.TransformColumns(CZ_ChangedType2, {{"值", each if _ <> null then Number.Round(_, 2) else null}}),
    CZ_ReplacedErrors = Table.ReplaceErrorValues(CZ_RoundedValues, {{"值", null}}),
    CZ_AddSiteColumn = Table.AddColumn(CZ_ReplacedErrors, "Site", each "CZ", type text),
    CZ_RemovedColumns1 = Table.RemoveColumns(CZ_AddSiteColumn,{"FY26 YTD ", "KPI Tier Level to View"}),
    CZ_FinalTypeConversion = Table.TransformColumnTypes(CZ_RemovedColumns1,{{"值", type number}}),
    // 处理KH表
    KH_Sheet = Source{[Item="Execution Plan KPIs-KH",Kind="Sheet"]}[Data],
    KH_ChangedType5 = Table.TransformColumnTypes(KH_Sheet,{{"Column1", type any}, {"Column2", type text}, {"Column3", type any}, {"Column4", type any}, {"Column5", type any}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}, {"Column9", type any}, {"Column10", type any}, {"Column11", type any}, {"Column12", type any}, {"Column13", type any}, {"Column14", type any}, {"Column15", type any}, {"Column16", type any}, {"Column17", type any}, {"Column18", type any}, {"Column19", type any}, {"Column20", type any}, {"Column21", type any}, {"Column22", type text}, {"Column23", type text}, {"Column24", type any}}),
    KH_ChangedType = Table.TransformColumnTypes(KH_ChangedType5,{{"Column1", type any}, {"Column2", type text}, {"Column3", type text}, {"Column4", type any}, {"Column5", type text}, {"Column6", type any}, {"Column7", type any}, {"Column8", type any}, {"Column9", type any}, {"Column10", type any}, {"Column11", type any}, {"Column12", type any}, {"Column13", type any}, {"Column14", type any}, {"Column15", type any}, {"Column16", type any}, {"Column17", type any}, {"Column18", type any}, {"Column19", type any}, {"Column20", type any}, {"Column21", type any}, {"Column22", type any}, {"Column23", type any}, {"Column24", type text}}),
    KH_RemovedTopRows = Table.Skip(KH_ChangedType,12),
    KH_RemovedBottomRows = Table.RemoveLastN(KH_RemovedTopRows,22),
    KH_FilteredRows = Table.SelectRows(KH_RemovedBottomRows, each ([Column2] <> "(Year 1: establish baseline)" and [Column2] <> "MPS Matruity Assessment Score")),
    KH_PromotedHeaders = Table.PromoteHeaders(KH_FilteredRows, [PromoteAllScalars=true]),
    KH_FilledDown = Table.FillDown(KH_PromotedHeaders,{"KPIs","Owner","KPI Tier Level to View"}),
    KH_FilteredRows1 = Table.SelectRows(KH_FilledDown, each Text.Contains([#"KPIs"], "#") or not Text.Contains([#"KPIs"], "%")),
    KH_RemovedColumns = Table.RemoveColumns(KH_FilteredRows1,{"Column1", "Column4", "FY26 YTD _1", "Column24", "If Red, Why?  Timing of KPI back to objective?  Link to A3 or KPI action Plan", "Comments #(lf)optional", "FY26 YE"}),
    KH_RenamedColumns2 = Table.RenameColumns(KH_RemovedColumns,{{"Column8", "Type"}}),
    KH_UnpivotedOtherColumns = Table.UnpivotOtherColumns(KH_RenamedColumns2, {"KPIs", "Owner","KPI Tier Level to View","FY26 YTD ", "Type"}, "属性", "值"),
    KH_RenamedColumns = Table.RenameColumns(KH_UnpivotedOtherColumns,{{"属性", "Month"}}),
    KH_AddDateColumn = Table.AddColumn(KH_RenamedColumns, "日期", each 
        let
            idx = List.PositionOf(MonthList, [Month]),
            baseYear = if idx < 8 then 2025 else 2026,
            baseMonth = if idx < 8 then idx + 5 else idx - 7
        in
            if idx <> -1 then #date(baseYear, baseMonth, 15) else null,
        type date
    ),
    KH_ChangedType2 = Table.TransformColumnTypes(KH_AddDateColumn,{{"日期", type date}, {"值", type number}}),
    KH_RoundedValues = Table.TransformColumns(KH_ChangedType2, {{"值", each if _ <> null then Number.Round(_, 2) else null}}),
    KH_ReplacedErrors = Table.ReplaceErrorValues(KH_RoundedValues, {{"值", null}}),
    KH_AddSiteColumn = Table.AddColumn(KH_ReplacedErrors, "Site", each "KH", type text),
    KH_RemovedColumns1 = Table.RemoveColumns(KH_AddSiteColumn,{"FY26 YTD ", "KPI Tier Level to View"}),
    KH_FinalTypeConversion = Table.TransformColumnTypes(KH_RemovedColumns1,{{"值", type number}}),
    // 合并两表
    CombinedTable = Table.Combine({CZ_FinalTypeConversion, KH_FinalTypeConversion}),
    // 合并后统一筛选Type为Act并重命名
    FilteredRows_Act = Table.SelectRows(CombinedTable, each ([Type] = "Act")),
    RenamedColumns_Act = Table.RenameColumns(FilteredRows_Act, {{"值", "Act"}})
in
    RenamedColumns_Act