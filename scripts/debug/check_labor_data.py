
import sys
from pathlib import Path
import pandas as pd

# Add project root needed for shared modules
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_data():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )
    
    with db.get_connection() as conn:
        # Check specific date
        query_date = "SELECT COUNT(*) as Count, SUM(EarnedLaborTime) as TotalHours FROM raw_sap_labor_hours WHERE PostingDate = '2026-01-31'"
        df_date = pd.read_sql(query_date, conn)
        print(f"\n--- Data for 2026-01-31 ---")
        print(df_date)

        # Check max date
        query_max = "SELECT MAX(PostingDate) as MaxDate, COUNT(*) as TotalRows FROM raw_sap_labor_hours"
        df_max = pd.read_sql(query_max, conn)
        print(f"\n--- Global Stats ---")
        print(df_max)

        # Check sample dates to confirm format
        query_sample = "SELECT TOP 5 PostingDate, EarnedLaborTime FROM raw_sap_labor_hours ORDER BY id DESC"
        df_sample = pd.read_sql(query_sample, conn)
        print(f"\n--- Latest 5 Records ---")
        print(df_sample)

if __name__ == "__main__":
    check_data()
