
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def list_areas():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )
    
    query = "SELECT DISTINCT area FROM dim_operation_mapping ORDER BY area"
    with db.get_connection() as conn:
        df = pd.read_sql(query, conn)
        print("\n--- Distinct Areas ---")
        print(df)

if __name__ == "__main__":
    list_areas()
