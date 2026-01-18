# 01 Labor Hours 报表

## 报表说明
人工工时分析报表，用于分析 SAP 系统中的人工工时数据。

## 数据源

| 查询文件 | 数据表 | 说明 |
|----------|--------|------|
| `query_labor_hours.sql` | `raw_sap_labor_hours` | 工时明细数据 |
| `query_calendar.sql` | `dim_calendar` | 财历日历表 |
| `query_operation_mapping.sql` | `dim_operation_mapping` | 工序分类映射 |

## 数据模型关系

```
pq_calendar.date  ───────────┐
                             │
pq_labor_hours ──────────────┼── PostingDate
                             │
pq_operation_mapping ────────┘── OperationDesc
```

## Power BI 连接配置

1. **数据源类型**: SQLite (通过 ODBC)
2. **数据库路径**: `data_pipelines/database/mddap_v2.db`
3. **刷新方式**: 手动刷新或计划刷新

## 建议的 DAX 度量值

```dax
// 总人工工时 (小时)
Total Labor Hours = SUM(pq_labor_hours[EarnedLaborTime])

// 总机器工时 (小时)
Total Machine Hours = SUM(pq_labor_hours[MachineTime])

// 实际产量
Total Actual Qty = SUM(pq_labor_hours[ActualQuantity])

// 报废数量
Total Scrap Qty = SUM(pq_labor_hours[ActualScrapQty])

// 报废率
Scrap Rate = DIVIDE([Total Scrap Qty], [Total Actual Qty] + [Total Scrap Qty])

// 当月人工工时 (MTD)
MTD Labor Hours = 
CALCULATE(
    [Total Labor Hours],
    DATESMTD(pq_calendar[date])
)

// 当月产量
Current Month Actual Qty = 
CALCULATE(
    [Total Actual Qty],
    DATESMTD(pq_calendar[date])
)

// YTD 人工工时 (年初至今)
YTD Labor Hours = 
CALCULATE(
    [Total Labor Hours],
    DATESYTD(pq_calendar[date])
)

// YTD 机器工时
YTD Machine Hours = 
CALCULATE(
    [Total Machine Hours],
    DATESYTD(pq_calendar[date])
)

// YTD 产量
YTD Actual Qty = 
CALCULATE(
    [Total Actual Qty],
    DATESYTD(pq_calendar[date])
)

// 工作日平均工时 (基于计划安排)
Avg Hours per Workday = 
VAR PlannedWorkdays = 
    CALCULATE(
        COUNTROWS(pq_planned_hours),
        pq_planned_hours[is_cz_workday] = 1 || pq_planned_hours[is_kh_workday] = 1
    )
RETURN
    DIVIDE([Total Labor Hours], PlannedWorkdays)

// 常州工作日平均工时
Avg Hours per CZ Workday = 
VAR CZWorkdays = 
    CALCULATE(
        COUNTROWS(pq_planned_hours),
        pq_planned_hours[is_cz_workday] = 1
    )
RETURN
    DIVIDE([Total Labor Hours], CZWorkdays)

// 康辉工作日平均工时
Avg Hours per KH Workday = 
VAR KHWorkdays = 
    CALCULATE(
        COUNTROWS(pq_planned_hours),
        pq_planned_hours[is_kh_workday] = 1
    )
RETURN
    DIVIDE([Total Labor Hours], KHWorkdays)

// 同比增长
Hours YoY Growth = 
VAR CurrentYear = [Total Labor Hours]
VAR PreviousYear = CALCULATE([Total Labor Hours], SAMEPERIODLASTYEAR(pq_calendar[date]))
RETURN DIVIDE(CurrentYear - PreviousYear, PreviousYear)

// 环比增长 (与上月对比)
Hours MoM Growth = 
VAR CurrentMonth = [Total Labor Hours]
VAR PreviousMonth = CALCULATE([Total Labor Hours], DATEADD(pq_calendar[date], -1, MONTH))
RETURN DIVIDE(CurrentMonth - PreviousMonth, PreviousMonth)

// 计划工时相关度量值
Total Planned Hours = SUM(pq_planned_hours[cz_planned_hours]) + SUM(pq_planned_hours[kh_planned_hours])

CZ Planned Hours = SUM(pq_planned_hours[cz_planned_hours])

KH Planned Hours = SUM(pq_planned_hours[kh_planned_hours])

// YTD 计划工时 (响应切片器，使用财历)
YTD Planned Hours = 
VAR MaxDate = MAXX(VALUES(pq_calendar[date]), [date])
VAR CurrentFiscalYear = MAXX(VALUES(pq_calendar[fiscal_year]), [fiscal_year])
VAR FiscalYearStart = 
    CALCULATE(
        MINX(VALUES(pq_calendar[date]), [date]),
        ALL(pq_calendar),
        pq_calendar[fiscal_year] = CurrentFiscalYear
    )
RETURN
CALCULATE(
    [Total Planned Hours],
    ALL(pq_calendar),
    pq_calendar[date] >= FiscalYearStart,
    pq_calendar[date] <= MaxDate
)

// YTD 常州计划工时
YTD CZ Planned Hours = 
VAR MaxDate = MAXX(VALUES(pq_calendar[date]), [date])
VAR CurrentFiscalYear = MAXX(VALUES(pq_calendar[fiscal_year]), [fiscal_year])
VAR FiscalYearStart = 
    CALCULATE(
        MINX(VALUES(pq_calendar[date]), [date]),
        ALL(pq_calendar),
        pq_calendar[fiscal_year] = CurrentFiscalYear
    )
RETURN
CALCULATE(
    [CZ Planned Hours],
    ALL(pq_calendar),
    pq_calendar[date] >= FiscalYearStart,
    pq_calendar[date] <= MaxDate
)

// YTD 康辉计划工时
YTD KH Planned Hours = 
VAR MaxDate = MAXX(VALUES(pq_calendar[date]), [date])
VAR CurrentFiscalYear = MAXX(VALUES(pq_calendar[fiscal_year]), [fiscal_year])
VAR FiscalYearStart = 
    CALCULATE(
        MINX(VALUES(pq_calendar[date]), [date]),
        ALL(pq_calendar),
        pq_calendar[fiscal_year] = CurrentFiscalYear
    )
RETURN
CALCULATE(
    [KH Planned Hours],
    ALL(pq_calendar),
    pq_calendar[date] >= FiscalYearStart,
    pq_calendar[date] <= MaxDate
)

// 达成率
Achievement Rate = DIVIDE([Total Labor Hours], [Total Planned Hours])

// 工时差异
Variance Hours = [Total Labor Hours] - [Total Planned Hours]

// 实时工时差异 (切片器范围内，截止到昨天)
Realtime Variance Hours = 
VAR Today = TODAY()
VAR Yesterday = Today - 1
VAR MinDate = MIN(pq_calendar[date])
VAR MaxDate = MAX(pq_calendar[date])
VAR EndDate = IF(MaxDate >= Today, Yesterday, MaxDate)
VAR ActualHoursToDate = 
    CALCULATE(
        [Total Labor Hours],
        pq_labor_hours[PostingDate] >= MinDate,
        pq_labor_hours[PostingDate] <= EndDate
    )
VAR PlannedHoursToDate = 
    CALCULATE(
        [Total Planned Hours],
        pq_planned_hours[plan_date] >= MinDate,
        pq_planned_hours[plan_date] <= EndDate
    )
RETURN
ActualHoursToDate - PlannedHoursToDate

// 常州实时工时差异
CZ Realtime Variance Hours = 
VAR Today = TODAY()
VAR Yesterday = Today - 1
VAR MinDate = MIN(pq_calendar[date])
VAR MaxDate = MAX(pq_calendar[date])
VAR EndDate = IF(MaxDate >= Today, Yesterday, MaxDate)
VAR ActualHoursToDate = 
    CALCULATE(
        [Total Labor Hours],
        pq_labor_hours[PostingDate] >= MinDate,
        pq_labor_hours[PostingDate] <= EndDate
    )
VAR CZPlannedHoursToDate = 
    CALCULATE(
        [CZ Planned Hours],
        pq_planned_hours[plan_date] >= MinDate,
        pq_planned_hours[plan_date] <= EndDate
    )
RETURN
ActualHoursToDate - CZPlannedHoursToDate

// 康辉实时工时差异
KH Realtime Variance Hours = 
VAR Today = TODAY()
VAR Yesterday = Today - 1
VAR MinDate = MIN(pq_calendar[date])
VAR MaxDate = MAX(pq_calendar[date])
VAR EndDate = IF(MaxDate >= Today, Yesterday, MaxDate)
VAR ActualHoursToDate = 
    CALCULATE(
        [Total Labor Hours],
        pq_labor_hours[PostingDate] >= MinDate,
        pq_labor_hours[PostingDate] <= EndDate
    )
VAR KHPlannedHoursToDate = 
    CALCULATE(
        [KH Planned Hours],
        pq_planned_hours[plan_date] >= MinDate,
        pq_planned_hours[plan_date] <= EndDate
    )
RETURN
ActualHoursToDate - KHPlannedHoursToDate

// 工作天数相关度量值
Total Planned Workdays = 
CALCULATE(
    COUNTROWS(pq_planned_hours),
    pq_planned_hours[is_cz_workday] = 1 || pq_planned_hours[is_kh_workday] = 1
)

CZ Planned Workdays = 
CALCULATE(
    COUNTROWS(pq_planned_hours),
    pq_planned_hours[is_cz_workday] = 1
)

KH Planned Workdays = 
CALCULATE(
    COUNTROWS(pq_planned_hours),
    pq_planned_hours[is_kh_workday] = 1
)

// 工作天数 (响应切片器选择)
Selected Period Workdays = 
CALCULATE(
    COUNTROWS(pq_planned_hours),
    pq_planned_hours[is_cz_workday] = 1 || pq_planned_hours[is_kh_workday] = 1
)

// 常州工作天数 (响应切片器选择)
CZ Selected Period Workdays = 
CALCULATE(
    COUNTROWS(pq_planned_hours),
    pq_planned_hours[is_cz_workday] = 1
)

// 康辉工作天数 (响应切片器选择)
KH Selected Period Workdays = 
CALCULATE(
    COUNTROWS(pq_planned_hours),
    pq_planned_hours[is_kh_workday] = 1
)

// 已过去工作天数 (切片器选择范围内，小于今天的工作日)
Elapsed Workdays = 
VAR Today = TODAY()
VAR MinDate = MIN(pq_planned_hours[plan_date])
VAR MaxDate = MAX(pq_planned_hours[plan_date])
VAR EndDate = IF(MaxDate >= Today, Today - 1, MaxDate)
RETURN
SUMX(
    FILTER(
        pq_planned_hours,
        pq_planned_hours[plan_date] >= MinDate &&
        pq_planned_hours[plan_date] <= EndDate &&
        (pq_planned_hours[is_cz_workday] = 1 || pq_planned_hours[is_kh_workday] = 1)
    ),
    1
)

// 常州已过去工作天数
CZ Elapsed Workdays = 
VAR Today = TODAY()
VAR MinDate = MIN(pq_planned_hours[plan_date])
VAR MaxDate = MAX(pq_planned_hours[plan_date])
VAR EndDate = IF(MaxDate >= Today, Today - 1, MaxDate)
RETURN
SUMX(
    FILTER(
        pq_planned_hours,
        pq_planned_hours[plan_date] >= MinDate &&
        pq_planned_hours[plan_date] <= EndDate &&
        pq_planned_hours[is_cz_workday] = 1
    ),
    1
)

// 康辉已过去工作天数
KH Elapsed Workdays = 
VAR Today = TODAY()
VAR MinDate = MIN(pq_planned_hours[plan_date])
VAR MaxDate = MAX(pq_planned_hours[plan_date])
VAR EndDate = IF(MaxDate >= Today, Today - 1, MaxDate)
RETURN
SUMX(
    FILTER(
        pq_planned_hours,
        pq_planned_hours[plan_date] >= MinDate &&
        pq_planned_hours[plan_date] <= EndDate &&
        pq_planned_hours[is_kh_workday] = 1
    ),
    1
)

// 工作天数完成率 (已过去天数 / 计划天数)
Workdays Completion Rate = 
FORMAT(
    DIVIDE([Elapsed Workdays], [Selected Period Workdays]),
    "0.00%"
)

// 常州工作天数完成率
CZ Workdays Completion Rate = 
FORMAT(
    DIVIDE([CZ Elapsed Workdays], [CZ Selected Period Workdays]),
    "0.00%"
)

// 康辉工作天数完成率
KH Workdays Completion Rate = 
FORMAT(
    DIVIDE([KH Elapsed Workdays], [KH Selected Period Workdays]),
    "0.00%"
)

// 实际平均工时 (实际产出工时 / 已过去天数)
Actual Avg Hours per Elapsed Workday = 
VAR Workdays = [Elapsed Workdays]
RETURN
IF(
    Workdays > 0,
    DIVIDE([Total Labor Hours], Workdays)
)

// 常州实际平均工时
CZ Actual Avg Hours per Elapsed Workday = 
VAR CZWorkdays = [CZ Elapsed Workdays]
RETURN
IF(
    CZWorkdays > 0,
    DIVIDE([Total Labor Hours], CZWorkdays)
)

// 康辉实际平均工时
KH Actual Avg Hours per Elapsed Workday = 
VAR KHWorkdays = [KH Elapsed Workdays]
RETURN
IF(
    KHWorkdays > 0,
    DIVIDE([Total Labor Hours], KHWorkdays)
)

// 实际工作天数 (截止到昨天，用于特殊场景)
Actual Workdays to Date = 
VAR Today = TODAY()
VAR Yesterday = Today - 1
RETURN
CALCULATE(
    COUNTROWS(pq_planned_hours),
    pq_planned_hours[plan_date] <= Yesterday,
    pq_planned_hours[is_cz_workday] = 1 || pq_planned_hours[is_kh_workday] = 1,
    ALL(pq_calendar)  // 清除所有日期筛选
)

// 实际平均工时 (基于截止到昨天的工作日)
Actual Avg Hours per Workday = 
VAR Workdays = [Actual Workdays to Date]
RETURN
IF(
    Workdays > 0,
    DIVIDE([Total Labor Hours], Workdays)
)
```

## 文件列表

- `01_labor_hours.pbix` - Power BI 报表文件
- `pq_labor_hours.m` - 工时数据 Power Query (M 语言)
- `pq_calendar.m` - 日历表 Power Query
- `pq_operation_mapping.m` - 工序映射 Power Query
- `pq_planned_hours.m` - 计划工时 Power Query (M 语言)
- `query_*.sql` - SQL 查询参考（仅供参考）
- `README.md` - 本说明文件

## 如何使用 Power Query 文件

1. 打开 Power BI Desktop
2. **主页** → **转换数据** → **高级编辑器**
3. 复制 `.m` 文件内容粘贴到编辑器
4. 点击 **完成**

> **注意**: 需要先安装 SQLite ODBC 驱动
