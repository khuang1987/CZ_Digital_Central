-- ============================================
-- 查询名称: Calendar (日历表)
-- 报表: 01_labor_hours
-- 数据源: dim_calendar
-- 说明: 财历日历维度表，包含财年、财周、节假日信息
-- ============================================

SELECT 
    date,
    weekday,
    weekday_cn,
    calendar_week,
    fiscal_week,
    fiscal_week_label,
    fiscal_month,
    fiscal_quarter,
    fiscal_year,
    fiscal_month_short,
    is_workday,
    holiday_name
FROM dim_calendar
ORDER BY date;
