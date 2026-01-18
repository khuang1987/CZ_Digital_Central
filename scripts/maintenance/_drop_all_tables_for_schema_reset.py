import sys
import pyodbc

# SQL Server connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

def drop_tables():
    print("Connecting to SQL Server to drop tables...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            
            # Drop View first
            print("Dropping view v_mes_metrics...")
            cur.execute("DROP VIEW IF EXISTS dbo.v_mes_metrics")
            
            # Drop Tables
            tables = [
                "dbo.raw_mes",
                "dbo.raw_sfc",
                "dbo.raw_sap_routing",
                "dbo.raw_sfc_inspection",
                "dbo.raw_calendar",
                "dbo.etl_file_state",
                "dbo.etl_run_log"
            ]
            
            for table in tables:
                print(f"Dropping table {table}...")
                # SQL Server < 2016 doesn't support DROP TABLE IF EXISTS, but assuming modern or checking object_id
                # Using simple check to be safe across versions or just try/except
                cur.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")
                
            print("\n✓ All target tables dropped successfully.")
            
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    drop_tables()
