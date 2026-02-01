import sys
from pathlib import Path
import pandas as pd

# Add project root to path
current_file = Path(__file__).resolve()
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_fiscal_data():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    with db.get_connection() as conn:
        print("--- Fiscal Info for Today ---")
        df_today = pd.read_sql("SELECT date, fiscal_year, fiscal_month FROM dim_calendar WHERE date = CAST(GETDATE() AS DATE)", conn)
        print(df_today.to_string())
        
        print("\n--- Fiscal Info for '2026-02-01' ---")
        df_target = pd.read_sql("SELECT date, fiscal_year, fiscal_month FROM dim_calendar WHERE date = '2026-02-01'", conn)
        print(df_target.to_string())

        print("\n--- Range for 'FY26 M10 FEB' ---")
        df_range = pd.read_sql("SELECT MIN(date) as s, MAX(date) as e FROM dim_calendar WHERE fiscal_year = 'FY26' AND fiscal_month = 'FY26 M10 FEB'", conn)
        print(df_range.to_string())

        print("\n--- Any dates for FY26? ---")
        df_any = pd.read_sql("SELECT TOP 5 date, fiscal_year, fiscal_month FROM dim_calendar WHERE fiscal_year = 'FY26'", conn)
        print(df_any.to_string())

if __name__ == "__main__":
    check_fiscal_data()
