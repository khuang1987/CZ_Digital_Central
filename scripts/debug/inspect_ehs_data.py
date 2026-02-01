import sys
import os
from pathlib import Path
import logging
import pandas as pd

# Add project root to path
current_file = Path(__file__).resolve()
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_ehs_data():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    with db.get_connection() as conn:
        print("--- Bucket Names Distribution ---")
        df_buckets = pd.read_sql("SELECT BucketName, COUNT(*) as c FROM planner_tasks GROUP BY BucketName ORDER BY c DESC", conn)
        print(df_buckets.to_string())
        
        print("\n--- Team Names (Areas) Distribution ---")
        df_teams = pd.read_sql("SELECT TeamName, COUNT(*) as c FROM planner_tasks GROUP BY TeamName ORDER BY c DESC", conn)
        print(df_teams.to_string())

        print("\n--- Raw Labels Sample (Top 20) ---")
        # Labels are stored as string (e.g. "Red; Blue") or in a separate table? 
        # The schema showed a `Labels` column in planner_tasks AND a `planner_task_labels` table.
        # Let's check `planner_task_labels` for cleaned labels.
        try:
            df_labels = pd.read_sql("SELECT CleanedLabel, COUNT(*) as c FROM planner_task_labels GROUP BY CleanedLabel ORDER BY c DESC", conn)
            print(df_labels.to_string())
        except Exception as e:
            print(f"Could not read planner_task_labels: {e}")
            print("Checking raw Labels column in planner_tasks...")
            df_raw_labels = pd.read_sql("SELECT TOP 20 Labels FROM planner_tasks WHERE Labels IS NOT NULL", conn)
            print(df_raw_labels.to_string())

if __name__ == "__main__":
    check_ehs_data()
