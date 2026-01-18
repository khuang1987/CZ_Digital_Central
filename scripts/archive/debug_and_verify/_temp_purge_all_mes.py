import sqlite3
import sys
from pathlib import Path

try:
    import pyodbc
except Exception:
    pyodbc = None

project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "data_pipelines" / "database" / "mddap_v2.db"

print("=== Purge ALL MES data ===")
print("DB:", db_path)

# SQLite
with sqlite3.connect(str(db_path)) as conn:
    conn.execute("PRAGMA busy_timeout = 30000;")
    raw_cnt = conn.execute("SELECT COUNT(*) FROM raw_mes").fetchone()[0]
    state_cnt = conn.execute("SELECT COUNT(*) FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%'").fetchone()[0]
    print(f"SQLite: raw_mes={raw_cnt}, etl_file_state(mes_raw_*)={state_cnt}")
    
    conn.execute("DELETE FROM raw_mes")
    conn.execute("DELETE FROM etl_file_state WHERE etl_name LIKE 'mes_raw_%'")
    conn.commit()
    print("SQLite purge done.")

# SQL Server
if pyodbc:
    try:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            r"SERVER=localhost\SQLEXPRESS;"
            "DATABASE=mddap_v2;"
            "Trusted_Connection=yes;"
            "Encrypt=no;"
            "TrustServerCertificate=no;"
        )
        with pyodbc.connect(conn_str, autocommit=False) as sql_conn:
            cur = sql_conn.cursor()
            sql_raw = cur.execute("SELECT COUNT(*) FROM dbo.raw_mes").fetchone()[0]
            sql_state = cur.execute("SELECT COUNT(*) FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%'").fetchone()[0]
            print(f"SQL Server: dbo.raw_mes={sql_raw}, dbo.etl_file_state(mes_raw_*)={sql_state}")
            
            cur.execute("DELETE FROM dbo.raw_mes")
            cur.execute("DELETE FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%'")
            sql_conn.commit()
            print("SQL Server purge done.")
    except Exception as e:
        print(f"[WARN] SQL Server purge failed: {e}")
else:
    print("[WARN] pyodbc not available, skip SQL Server")

print("\nAll MES data purged. You can now run:")
print("  python data_pipelines\\sources\\mes\\etl\\etl_mes_batch_output_raw.py --force")
