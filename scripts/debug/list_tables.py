
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def list_tables():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )
    
    query = """
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
    """
    
    with db.get_connection() as conn:
        df = pd.read_sql(query, conn)
        print("\n--- Database Tables ---")
        print(df)
        
        # Check dim_calendar count specifically
        try:
            cnt = pd.read_sql("SELECT COUNT(*) as c FROM dim_calendar", conn)
            print(f"\nRows in dim_calendar: {cnt['c'][0]}")
        except Exception as e:
            print(f"\nError querying dim_calendar: {e}")

if __name__ == "__main__":
    list_tables()
