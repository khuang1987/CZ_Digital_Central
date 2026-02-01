import sys
import os
from pathlib import Path
import pandas as pd

# Add project root to path
current_file = Path(__file__).resolve()
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_table():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    with db.get_connection() as conn:
        print("--- Checking planner_task_labels ---")
        try:
            df = pd.read_sql("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'planner_task_labels'", conn)
            print(df.to_string())
            if len(df) > 0:
                print("Table EXISTS.")
            else:
                print("Table DOES NOT EXIST.")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    check_table()
