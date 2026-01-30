import os
import sys
import pyodbc
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.env_utils import ENV_FILE

def check_db():
    server = os.getenv("MDDAP_SQL_SERVER")
    database = os.getenv("MDDAP_SQL_DATABASE")
    
    print(f"--- SQL Server Post-Migration Check ---")
    print(f"Target Server: {server}")
    print(f"Target DB: {database}")
    print(f"Using .env: {ENV_FILE}")
    
    if not server or not database:
        print("Error: Missing database configuration in environment!")
        return

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"
        f"Connection Timeout=10;"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Test Query 1: Basic connection
        cursor.execute("SELECT @@VERSION")
        row = cursor.fetchone()
        print(f"\n[OK] Connection Successful!")
        print(f"SQL version: {row[0][:50]}...")
        
        # Test Query 2: Schema check
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'dbo'")
        table_count = cursor.fetchone()[0]
        print(f"[OK] Found {table_count} tables in dbo schema.")
        
        # Test Query 3: Data check
        cursor.execute("SELECT TOP 1 [Timestamp] FROM [dbo].[KPI_Data] ORDER BY [Timestamp] DESC")
        last_data = cursor.fetchone()
        if last_data:
            print(f"[OK] Latest data timestamp: {last_data[0]}")
        else:
            print("[Warning] KPI_Data table is empty.")
            
        conn.close()
        print("\nConclusion: Database environment is HEALTHY.")
        
    except Exception as e:
        print(f"\n[FAILED] Database check error: {e}")
        print("\nTroubleshooting Tips:")
        print("1. Ensure SQL Server (SQLEXPRESS) service is running.")
        print("2. Ensure ODBC Driver 17 is installed.")
        print("3. Check if current user has permissions (Trusted_Connection).")
        print(f"4. Verify .env file: {ENV_FILE}")

if __name__ == "__main__":
    check_db()
