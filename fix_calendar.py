import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

# Fix Jan 1-2 as non-working days
print("Updating Jan 1-2, 2026 to non-working days...")
cursor.execute("UPDATE dbo.dim_calendar_cumulative SET IsWorkday = 0 WHERE CalendarDate IN ('2026-01-01', '2026-01-02')")
print(f"Rows updated: {cursor.rowcount}")

# Now we need to recalculate CumulativeNonWorkDays from Jan 1 onwards
print("\nRecalculating CumulativeNonWorkDays from 2026-01-01...")
cursor.execute("""
-- Get the cumulative count as of 2025-12-31
DECLARE @baseCount INT;
SELECT @baseCount = CumulativeNonWorkDays FROM dbo.dim_calendar_cumulative WHERE CalendarDate = '2025-12-31';

-- Update all dates from 2026-01-01 onwards
;WITH cte AS (
    SELECT 
        CalendarDate,
        IsWorkday,
        @baseCount + SUM(CASE WHEN IsWorkday = 0 THEN 1 ELSE 0 END) OVER (ORDER BY CalendarDate) as NewCumNW
    FROM dbo.dim_calendar_cumulative
    WHERE CalendarDate >= '2026-01-01'
)
UPDATE c
SET c.CumulativeNonWorkDays = cte.NewCumNW
FROM dbo.dim_calendar_cumulative c
INNER JOIN cte ON c.CalendarDate = cte.CalendarDate;

SELECT @@ROWCOUNT;
""")
print("Recalculation complete.")

# Verify
print("\nVerification - Jan 1-3, 2026:")
cursor.execute("""
SELECT CalendarDate, IsWorkday, CumulativeNonWorkDays
FROM dbo.dim_calendar_cumulative
WHERE CalendarDate BETWEEN '2026-01-01' AND '2026-01-03'
ORDER BY CalendarDate
""")
for row in cursor.fetchall():
    work_status = "工作日" if row[1] == 1 else "非工作日"
    print(f"{row[0]}: {work_status}, CumNW: {row[2]}")

conn.close()
