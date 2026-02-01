import sys
import os
from pathlib import Path
import pandas as pd
from datetime import datetime, date

# Add project root to path
current_file = Path(__file__).resolve()
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def verify_queries():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    # Mock Parameters (from 500 error)
    fiscal_year = "FY26"
    fiscal_month = "FY26 M10 FEB"
    area = None # "147加工中心" or None
    
    # Dates (Approximate for testing)
    start_date = "2026-02-01"
    end_date = "2026-02-28"
    fiscal_ytd_start = "2025-04-26" # Approximate FY26 start

    with db.get_connection() as conn:
        print(f"--- Testing Queries with FY={fiscal_year}, FM={fiscal_month} ---")

        # Query A: Green Cross
        try:
            print("\n[A] Testing Green Cross Log...")
            sql_a = "SELECT date, status, incident_details FROM safety_green_cross_log WHERE date BETWEEN ? AND ? ORDER BY date"
            df_a = pd.read_sql(sql_a, conn, params=[start_date, end_date])
            print(f"Success. Rows: {len(df_a)}")
        except Exception as e:
            print(f"FAILED [A]: {e}")

        # Query B: YTD Incidents
        try:
            print("\n[B] Testing YTD Incidents...")
            sql_b = """
                SELECT COUNT(DISTINCT t.TaskId) as count
                FROM planner_tasks t
                JOIN planner_task_labels l ON t.TaskId = l.TaskId
                WHERE t.CreatedDate >= ?
                AND (
                    l.CleanedLabel IN (N'急救事件', N'可记录事故', N'工伤', N'Lost Time Injury')
                    OR l.CleanedLabel LIKE N'%事故%' 
                )
                AND ISNULL(t.IsDeleted, 0) = 0
            """
            # Note: Parameter binding in pandas uses ?
            df_b = pd.read_sql(sql_b, conn, params=[fiscal_ytd_start])
            print(f"Success. Rows: {len(df_b)}")
        except Exception as e:
            print(f"FAILED [B]: {e}")

        # Query C: Open Hazards
        try:
            print("\n[C] Testing Open Hazards...")
            sql_c = """
                SELECT COUNT(*) as count
                FROM planner_tasks t
                WHERE (BucketName = N'安全' OR BucketName = N'Safe' OR BucketName = N'Safety')
                AND t.CreatedDate BETWEEN ? AND ? 
                AND (Status != 'Completed' AND Status != 'Closed')
                AND ISNULL(IsDeleted, 0) = 0
            """
            df_c = pd.read_sql(sql_c, conn, params=[start_date, end_date])
            print(f"Success. Rows: {len(df_c)}")
        except Exception as e:
            print(f"FAILED [C]: {e}")
            
        # Query D: Safe Days (Global)
        try:
            print("\n[D] Testing Safe Days (Global)...")
            sql_d = """
                    SELECT 
                        DATEDIFF(day, 
                            ISNULL((SELECT MAX(date) FROM safety_green_cross_log WHERE status = 'Incident'), '2024-01-01'), 
                            GETDATE()
                        ) as safeDays
            """
            df_d = pd.read_sql(sql_d, conn)
            print(f"Success. Rows: {len(df_d)}")
        except Exception as e:
            print(f"FAILED [D]: {e}")

        # Query E: Hazards by Area
        try:
            print("\n[E] Testing Hazards by Area...")
            sql_e = """
                SELECT 
                    TeamName as area,
                    COUNT(*) as count
                FROM planner_tasks t
                WHERE (BucketName = N'安全' OR BucketName = N'Safe' OR BucketName = N'Safety')
                AND t.CreatedDate BETWEEN ? AND ?
                AND (Status != 'Completed' AND Status != 'Closed')
                AND ISNULL(IsDeleted, 0) = 0
                GROUP BY TeamName
                ORDER BY count DESC
            """
            df_e = pd.read_sql(sql_e, conn, params=[start_date, end_date])
            print(f"Success. Rows: {len(df_e)}")
        except Exception as e:
            print(f"FAILED [E]: {e}")

        # Query F: Distinct Teams
        try:
            print("\n[F] Testing Distinct Teams...")
            sql_f = """
            SELECT DISTINCT TeamName FROM planner_tasks 
            WHERE TeamName IS NOT NULL AND TeamName != ''
            ORDER BY TeamName
            """
            df_f = pd.read_sql(sql_f, conn)
            print(f"Success. Rows: {len(df_f)}")
        except Exception as e:
            print(f"FAILED [F]: {e}")
            
        print("\n--- Done ---")

if __name__ == "__main__":
    verify_queries()
