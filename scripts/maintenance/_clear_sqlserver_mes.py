"""Maintenance: Clear MES raw tables in SQL Server.

Purpose:
  Clear MES raw data in SQL Server when you need to re-import from source files.

Effects:
  - Deletes all rows from dbo.raw_mes.
  - Deletes file state rows from dbo.etl_file_state where etl_name LIKE 'mes_raw_%'.

Safety:
  Prompts for an explicit confirmation. You must type YES to proceed.

Run:
  python scripts/maintenance/_clear_sqlserver_mes.py
"""

import sys
from pathlib import Path

try:
    import pyodbc
except ImportError:
    print("ERROR: pyodbc not installed")
    sys.exit(1)

# SQL Server connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

print("=== Clearing MES data from SQL Server ===")
print("Connecting to SQL Server...")

confirm = input("Type YES to continue: ").strip()
if confirm != "YES":
    print("Aborted. No changes were made.")
    sys.exit(0)

try:
    with pyodbc.connect(conn_str, autocommit=False) as conn:
        cur = conn.cursor()
        
        # Check counts before deletion
        raw_cnt = cur.execute("SELECT COUNT(*) FROM dbo.raw_mes").fetchone()[0]
        state_cnt = cur.execute("SELECT COUNT(*) FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%'").fetchone()[0]
        
        print(f"\nBefore deletion:")
        print(f"  dbo.raw_mes: {raw_cnt} rows")
        print(f"  dbo.etl_file_state (mes_raw_*): {state_cnt} rows")
        
        # Delete
        print("\nDeleting...")
        cur.execute("DELETE FROM dbo.raw_mes")
        cur.execute("DELETE FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%'")
        conn.commit()
        
        # Verify
        raw_cnt_after = cur.execute("SELECT COUNT(*) FROM dbo.raw_mes").fetchone()[0]
        state_cnt_after = cur.execute("SELECT COUNT(*) FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%'").fetchone()[0]
        
        print(f"\nAfter deletion:")
        print(f"  dbo.raw_mes: {raw_cnt_after} rows")
        print(f"  dbo.etl_file_state (mes_raw_*): {state_cnt_after} rows")
        print("\n✓ SQL Server MES data cleared successfully")
        
except Exception as e:
    print(f"\nERROR: {type(e).__name__}: {e}")
    sys.exit(1)
