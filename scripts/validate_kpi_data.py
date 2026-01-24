import pyodbc
import pandas as pd
import logging

def validate():
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=mddap_v2;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    
    # Check KPI 1 & 2 Details
    query = """
    SELECT TOP 5 KPI_Id, Tag, CreatedDate, Progress, Details
    FROM dbo.KPI_Data
    WHERE KPI_Id IN (1, 2)
    ORDER BY CreatedDate DESC
    """
    df = pd.read_sql(query, conn)
    print("KPI Data Sample:")
    print(df.to_string())

if __name__ == "__main__":
    validate()
