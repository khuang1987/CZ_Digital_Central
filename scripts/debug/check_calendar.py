
import sys
from pathlib import Path
import pandas as pd
from datetime import date

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_calendar():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )
    
    with db.get_connection() as conn:
        print("\n--- Calendar Range ---")
        query_range = "SELECT MIN(date) as min_date, MAX(date) as max_date, COUNT(*) as cnt FROM dim_calendar"
        df_range = pd.read_sql(query_range, conn)
        print(df_range)
        
        print("\n--- Today's Entry ---")
        query_today = "SELECT * FROM dim_calendar WHERE date = CAST(GETDATE() AS DATE)"
        df_today = pd.read_sql(query_today, conn)
        if df_today.empty:
            print("WARNING: No entry for today!")
            print(f"Today is: {date.today()}")
        else:
            print("Today exists:")
            print(df_today)

if __name__ == "__main__":
    check_calendar()
