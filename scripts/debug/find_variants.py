
import sys
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def find_variants():
    db = SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )
    
    keywords = [
        '多轴车铣', 
        '激光焊接', 
        '钳工', '装配', '钳装', 
        '清洗', 'cleaning',
        '五轴磨',
        '线切割',
        '真空热处理'
    ]
    
    print(f"Searching for variants of: {keywords}")
    
    with db.get_connection() as conn:
        conditions = " OR ".join([f"operation_name LIKE N'%{k}%'" for k in keywords])
        query = f"""
        SELECT operation_name, display_name 
        FROM dim_operation_mapping 
        WHERE {conditions}
        ORDER BY operation_name
        """
        df = pd.read_sql(query, conn)
        # Set max rows to display all
        pd.set_option('display.max_rows', None)
        print(df)

if __name__ == "__main__":
    find_variants()
