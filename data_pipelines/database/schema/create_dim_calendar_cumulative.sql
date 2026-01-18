
-- Create dim_calendar_cumulative table
IF OBJECT_ID('dbo.dim_calendar_cumulative', 'U') IS NOT NULL
    DROP TABLE dbo.dim_calendar_cumulative;

CREATE TABLE dbo.dim_calendar_cumulative (
    CalendarDate DATE PRIMARY KEY,
    IsWorkday INT,
    CumulativeNonWorkDays INT
);

-- Populate table
WITH CTE AS (
    SELECT 
        CalendarDate,
        IsWorkday,
        SUM(CASE WHEN IsWorkday = 0 THEN 1 ELSE 0 END) OVER (ORDER BY CalendarDate) as CumulativeNonWorkDays
    FROM dbo.raw_calendar
)
INSERT INTO dbo.dim_calendar_cumulative (CalendarDate, IsWorkday, CumulativeNonWorkDays)
SELECT CalendarDate, IsWorkday, CumulativeNonWorkDays
FROM CTE;

-- Create index for performance
CREATE INDEX idx_dim_cal_cum_date ON dbo.dim_calendar_cumulative(CalendarDate);
