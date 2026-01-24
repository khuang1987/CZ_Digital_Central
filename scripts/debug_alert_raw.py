import pyodbc
import pandas as pd
from datetime import datetime

def debug_query():
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=mddap_v2;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    
    # Simulate the query used in etl_alert_engine.py
    # KPI 2, CZ_Campus, < 95, StartDate 2025-12-25
    query = """
    SELECT
        d.Tag,
        CAST(d.CreatedDate AS date) AS CreatedDate,
        CONCAT(cal.fiscal_year, ' W', RIGHT('0' + CAST(cal.fiscal_week AS varchar(2)), 2)) AS FiscalWeek,
        d.Progress,
        d.Details
    FROM dbo.KPI_Data d
    JOIN dbo.dim_calendar cal ON CAST(d.CreatedDate AS date) = cal.[date]
    WHERE d.KPI_Id = 2
      AND d.Tag = 'CZ_Campus'
      AND CAST(d.CreatedDate AS date) >= '2025-12-25'
    ORDER BY d.CreatedDate
    """
    
    df = pd.read_sql(query, conn)
    print(df.to_string())

if __name__ == "__main__":
    debug_query()
