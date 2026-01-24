import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Trace the exact calculation
print("=== 详细计算跟踪 ===")
cursor.execute("""
WITH base AS (
    SELECT 
        m.BatchNumber, m.Operation,
        m.TrackOutTime,
        LAG(m.TrackOutTime) OVER (PARTITION BY m.Machine ORDER BY m.TrackOutTime) AS PrevBatchEnd,
        COALESCE(LAG(m.TrackOutTime) OVER (PARTITION BY m.Machine ORDER BY m.TrackOutTime), m.TrackInTime) AS PT_StartTime
    FROM dbo.raw_mes m
    WHERE m.BatchNumber = 'K25M2170' AND LTRIM(RTRIM(m.Operation)) = '20'
)
SELECT 
    b.*,
    CAST(b.PT_StartTime AS DATE) AS D_PT_Start,
    CAST(b.TrackOutTime AS DATE) AS D_End,
    c_start.IsWorkday AS PT_Start_IsWork,
    c_start.CumulativeNonWorkDays AS PT_Start_CumNW,
    c_end.IsWorkday AS End_IsWork,
    c_end.CumulativeNonWorkDays AS End_CumNW,
    (c_end.CumulativeNonWorkDays - c_start.CumulativeNonWorkDays) AS RawCumDiff,
    -- Formula: (End_CumNW - (if end is non-work then 1 else 0) - Start_CumNW) * 86400 for middle days
    (c_end.CumulativeNonWorkDays - (CASE WHEN c_end.IsWorkday = 0 THEN 1 ELSE 0 END) - c_start.CumulativeNonWorkDays) AS MiddleDaysDiff
FROM base b
LEFT JOIN dbo.dim_calendar_cumulative c_start ON CAST(b.PT_StartTime AS DATE) = c_start.CalendarDate
LEFT JOIN dbo.dim_calendar_cumulative c_end ON CAST(b.TrackOutTime AS DATE) = c_end.CalendarDate
""")
for r in cursor.fetchall():
    print(f"PT_StartTime: {r[4]}")
    print(f"TrackOutTime: {r[2]}")
    print(f"D_PT_Start: {r[5]}")
    print(f"D_End: {r[6]}")
    print(f"PT_Start_IsWork: {r[7]} (1=工作日)")
    print(f"PT_Start_CumNW: {r[8]}")
    print(f"End_IsWork: {r[9]} (1=工作日)")
    print(f"End_CumNW: {r[10]}")
    print(f"RawCumDiff (End-Start): {r[11]}")
    print(f"MiddleDaysDiff (扣除结束日修正): {r[12]}")

conn.close()
