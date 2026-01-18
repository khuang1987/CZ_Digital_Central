import pyodbc
import sys
import time

# SQL Server connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

def force_recreate():
    print("Connecting to SQL Server...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            
            # 1. Drop existing objects
            objects = [
                ("VIEW", "dbo.v_mes_metrics"),
                ("SYNONYM", "dbo.mes_metrics_current"),
                ("TABLE", "dbo.mes_metrics_snapshot_a"),
                ("TABLE", "dbo.mes_metrics_snapshot_b")
            ]
            
            print("Dropping existing objects...")
            for obj_type, obj_name in objects:
                print(f"  Dropping {obj_name}...")
                if obj_type == "SYNONYM":
                    cur.execute(f"IF OBJECT_ID('{obj_name}', 'SN') IS NOT NULL DROP SYNONYM {obj_name}")
                elif obj_type == "VIEW":
                    cur.execute(f"IF OBJECT_ID('{obj_name}', 'V') IS NOT NULL DROP VIEW {obj_name}")
                elif obj_type == "TABLE":
                    cur.execute(f"IF OBJECT_ID('{obj_name}', 'U') IS NOT NULL DROP TABLE {obj_name}")
            
            # 2. Verify they are gone
            print("\nVerifying drops...")
            for obj_type, obj_name in objects:
                suffix = 'SN' if obj_type == 'SYNONYM' else ('V' if obj_type == 'VIEW' else 'U')
                cur.execute(f"SELECT OBJECT_ID('{obj_name}', '{suffix}')")
                if cur.fetchone()[0] is not None:
                    print(f"  ERROR: {obj_name} still exists!")
                    sys.exit(1)
            print("  All objects dropped successfully.")

    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    force_recreate()
