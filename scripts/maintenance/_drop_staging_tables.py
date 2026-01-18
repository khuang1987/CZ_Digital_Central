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

def drop_staging_tables():
    print("Connecting to SQL Server to drop staging tables...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            
            tables = [
                "dbo._stg_raw_mes",
                "dbo._stg_raw_sfc",
                "dbo._stg_raw_sap_routing",
                "dbo._stg_raw_sfc_inspection"
            ]
            
            for table in tables:
                print(f"Dropping table {table}...")
                cur.execute(f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}")
                
            print("\n✓ Staging tables dropped successfully.")
            
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    drop_staging_tables()
