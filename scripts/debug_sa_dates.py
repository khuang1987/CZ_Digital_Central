import pyodbc
import pandas as pd
from datetime import datetime

def check_sa_dates():
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=mddap_v2;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    
    query = """
    SELECT 
        d.Tag, 
        d.CreatedDate, 
        d.Progress, 
        cal.fiscal_year, 
        cal.fiscal_week
    FROM dbo.KPI_Data d
    JOIN dbo.dim_calendar cal ON CONVERT(varchar(10), d.CreatedDate, 23) = cal.date
    WHERE d.KPI_Id = 2 
      AND d.Tag = 'CZ_Campus' 
    ORDER BY d.CreatedDate DESC
    """
    
    df = pd.read_sql(query, conn)
    print("SA Data Points:")
    print(df.to_string())

if __name__ == "__main__":
    check_sa_dates()
