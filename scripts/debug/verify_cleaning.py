
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_cleaning():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )
    
    with db.get_connection() as conn:
        # Check if column exists and show sample
        query = "SELECT TOP 10 operation_name, display_name, area FROM dim_operation_mapping WHERE operation_name <> display_name"
        df = pd.read_sql(query, conn)
        print("\n--- Cleaning Examples (diff only) ---")
        if df.empty:
            print("No differences found? Check if rules matched anything.")
            # Check raw count
            cnt = pd.read_sql("SELECT COUNT(*) as c FROM dim_operation_mapping", conn)
            print(f"Total Rows: {cnt['c'][0]}")
        else:
            print(df)

if __name__ == "__main__":
    check_cleaning()
