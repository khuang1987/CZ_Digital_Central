
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def check_specific_cleaning():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )
    
    with db.get_connection() as conn:
        print("\n--- Current Mapping for 'Milling' types ---")
        query = """
        SELECT operation_name, display_name 
        FROM dim_operation_mapping 
        WHERE operation_name LIKE N'%数控铣%' OR operation_name LIKE N'%锯%'
        ORDER BY display_name
        """
        df = pd.read_sql(query, conn)
        print(df)

if __name__ == "__main__":
    check_specific_cleaning()
