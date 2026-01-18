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

def clean_duplicate_file_states():
    print("Connecting to SQL Server to clean duplicate etl_file_state entries...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cur = conn.cursor()
            
            # Delete entries with forward slashes for SAP Routing, as we are now using normalized backslashes
            print("Deleting old entries with forward slashes for sap_routing_raw...")
            cur.execute("DELETE FROM dbo.etl_file_state WHERE etl_name LIKE 'sap_routing_raw_%' AND file_path LIKE '%/%'")
            print(f"Deleted {cur.rowcount} rows.")
            
            # Also checking for mixed slashes just in case
            # normalized path on windows shouldn't have forward slashes
            
            print("\n✓ Cleanup complete.")
            
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)

if __name__ == "__main__":
    clean_duplicate_file_states()
