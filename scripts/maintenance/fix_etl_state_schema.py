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

def fix_schema():
    print("Fixing schema of etl_file_state (file_mtime REAL -> FLOAT)...")
    try:
        with pyodbc.connect(conn_str, autocommit=True) as conn:
            cursor = conn.cursor()
            
            # Check current type
            cursor.execute("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'etl_file_state' AND COLUMN_NAME = 'file_mtime'")
            row = cursor.fetchone()
            if row:
                print(f"Current type: {row[0]}")
                if row[0].lower() == 'real':
                    print("Altering column to FLOAT...")
                    cursor.execute("ALTER TABLE dbo.etl_file_state ALTER COLUMN file_mtime FLOAT")
                    print("Column altered successfully.")
                else:
                    print("Column is not REAL, checking if it needs update anyway (ensure FLOAT).")
                    # Force update to FLOAT just in case
                    cursor.execute("ALTER TABLE dbo.etl_file_state ALTER COLUMN file_mtime FLOAT")
                    print("Column ensured as FLOAT.")
            else:
                print("Column file_mtime not found!")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fix_schema()
