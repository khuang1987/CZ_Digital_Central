import pandas as pd
import pyodbc
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_queries():
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=mddap_v2;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    
    # Test 1: Basic Join
    q1 = """
    SELECT COUNT(*) as cnt 
    FROM dbo.planner_tasks t 
    JOIN dbo.planner_task_labels l ON t.TaskId = l.TaskId
    """
    df1 = pd.read_sql(q1, conn)
    logger.info(f"Test 1 (Task+Label Join): {df1.iloc[0]['cnt']}")

    # Test 2: Filter Excluded
    q2 = """
    SELECT COUNT(*) as cnt 
    FROM dbo.planner_tasks t 
    JOIN dbo.planner_task_labels l ON t.TaskId = l.TaskId
    WHERE l.IsExcluded = 0
    """
    df2 = pd.read_sql(q2, conn)
    logger.info(f"Test 2 (IsExcluded=0): {df2.iloc[0]['cnt']}")

    # Test 3: Filter Bucket (Unicode)
    try:
        q3 = """
        SELECT COUNT(*) as cnt 
        FROM dbo.planner_tasks t 
        JOIN dbo.planner_task_labels l ON t.TaskId = l.TaskId
        WHERE t.BucketName = ?
        """
        df3 = pd.read_sql(q3, conn, params=['安全'])
        logger.info(f"Test 3 (Bucket=Safe Param): {df3.iloc[0]['cnt']}")
    except Exception as e:
        logger.error(f"Test 3 Failed: {e}")

    # Test 4: Join Calendar
    q4 = """
    SELECT COUNT(*) as cnt
    FROM dbo.planner_tasks t 
    JOIN dbo.dim_calendar cal ON CONVERT(varchar(10), t.CreatedDate, 23) = cal.date
    WHERE t.CreatedDate > '2026-01-01'
    """
    df4 = pd.read_sql(q4, conn)
    logger.info(f"Test 4 (Join Calendar): {df4.iloc[0]['cnt']}")

    # Test 5: Full Monty (Manual WeekStarts logic)
    q5 = """
    WITH WeekStarts AS (
        SELECT fiscal_year, fiscal_week, MIN(date) as week_start
        FROM dbo.dim_calendar
        WHERE date >= DATEADD(week, -8, GETDATE())
        GROUP BY fiscal_year, fiscal_week
    )
    SELECT COUNT(*) as cnt
    FROM dbo.planner_task_labels l
    JOIN dbo.planner_tasks t ON l.TaskId = t.TaskId
    JOIN dbo.dim_calendar cal ON CONVERT(varchar(10), t.CreatedDate, 23) = cal.date
    JOIN WeekStarts ws ON cal.fiscal_year = ws.fiscal_year AND cal.fiscal_week = ws.fiscal_week
    WHERE t.BucketName = ?
      AND l.IsExcluded = 0
    """
    try:
        df5 = pd.read_sql(q5, conn, params=['安全'])
        logger.info(f"Test 5 (Full Query): {df5.iloc[0]['cnt']}")
    except Exception as e:
        logger.error(f"Test 5 Failed: {e}")

if __name__ == "__main__":
    debug_queries()
