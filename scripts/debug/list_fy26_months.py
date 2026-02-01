import sys
from pathlib import Path
import pandas as pd

# Add project root to path
current_file = Path(__file__).resolve()
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def list_fy26_months():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    with db.get_connection() as conn:
        print("--- All Fiscal Months for FY26 ---")
        df = pd.read_sql("SELECT DISTINCT fiscal_month FROM dim_calendar WHERE fiscal_year = 'FY26' ORDER BY fiscal_month", conn)
        print(df.to_string())

if __name__ == "__main__":
    list_fy26_months()
