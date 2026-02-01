import sys
import os
from pathlib import Path
import pandas as pd

current_file = Path(__file__).resolve()
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_calendar():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    with db.get_connection() as conn:
        print("--- Current Fiscal Info ---")
        df = pd.read_sql("SELECT TOP 1 fiscal_year, fiscal_month, date FROM dim_calendar WHERE date = CAST(GETDATE() AS DATE)", conn)
        print(df.to_string())
        
        print("\n--- Sample Fiscal Months ---")
        df2 = pd.read_sql("SELECT DISTINCT TOP 10 fiscal_year, fiscal_month FROM dim_calendar ORDER BY fiscal_year DESC, fiscal_month", conn)
        print(df2.to_string())

if __name__ == "__main__":
    check_calendar()
