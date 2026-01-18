import pyodbc
import sys

# SQL Server connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

def reset_snapshots():
    print("Connecting to SQL Server to reset MES materialized views...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            
            objects_to_drop = [
                ("VIEW", "dbo.v_mes_metrics"),
                ("SYNONYM", "dbo.mes_metrics_current"),
                ("TABLE", "dbo.mes_metrics_snapshot_a"),
                ("TABLE", "dbo.mes_metrics_snapshot_b")
            ]
            
            for obj_type, obj_name in objects_to_drop:
                print(f"Dropping {obj_type} {obj_name}...")
                if obj_type == "SYNONYM":
                    cur.execute(f"IF OBJECT_ID('{obj_name}', 'SN') IS NOT NULL DROP SYNONYM {obj_name}")
                elif obj_type == "VIEW":
                    cur.execute(f"IF OBJECT_ID('{obj_name}', 'V') IS NOT NULL DROP VIEW {obj_name}")
                elif obj_type == "TABLE":
                    cur.execute(f"IF OBJECT_ID('{obj_name}', 'U') IS NOT NULL DROP TABLE {obj_name}")
                
            print("\n✓ MES materialized view artifacts dropped successfully.")
            
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    reset_snapshots()
