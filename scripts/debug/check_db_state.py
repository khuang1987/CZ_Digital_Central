import sys
from pathlib import Path
import pandas as pd

# Add project root to path
current_file = Path(__file__).resolve()
project_root = str(current_file.parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_db_state():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server"
    )
    
    with db.get_connection() as conn:
        print("--- Calendar Entry for Today (2026-02-01) ---")
        df_cal = pd.read_sql("SELECT * FROM dim_calendar WHERE date = '2026-02-01'", conn)
        print(df_cal.to_string())
        
        print("\n--- Safety Log Records ---")
        df_log = pd.read_sql("SELECT TOP 10 * FROM safety_green_cross_log ORDER BY date DESC", conn)
        print(df_log.to_string())
        
        print("\n--- Check Column Types ---")
        df_types = pd.read_sql("""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'safety_green_cross_log'
        """, conn)
        print(df_types.to_string())

if __name__ == "__main__":
    check_db_state()
