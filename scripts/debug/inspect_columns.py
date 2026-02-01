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

def check_columns():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    with db.get_connection() as conn:
        print("--- Checking Columns ---")
        try:
            df = pd.read_sql("SELECT TOP 1 CreatedDate, TeamName, BucketName, Labels FROM planner_tasks", conn)
            print("Successfully read columns:")
            print(df.columns.tolist())
            print(df.to_string())
        except Exception as e:
            print(f"Error reading columns: {e}")

if __name__ == "__main__":
    check_columns()
