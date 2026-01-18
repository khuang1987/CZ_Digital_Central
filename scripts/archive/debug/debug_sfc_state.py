import pyodbc
import sys
import os

# SQL Server connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

def check_sfc_state():
    print("Checking etl_file_state for sfc_%...")
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 10 etl_name, file_path, file_mtime, file_size FROM dbo.etl_file_state WHERE etl_name LIKE 'sfc_%' ORDER BY processed_time DESC")
            rows = cursor.fetchall()
            
            if not rows:
                print("No records found.")
            else:
                for row in rows:
                    etl_name, file_path, file_mtime, file_size = row
                    print(f"ETL: {etl_name}")
                    print(f"  Path: {file_path}")
                    print(f"  DB Mtime: {file_mtime}")
                    
                    if os.path.exists(file_path):
                        # Normalize path to compare
                        norm_path = os.path.normpath(os.path.abspath(file_path))
                        print(f"  FS Path: {norm_path}")
                        print(f"  Path Match: {norm_path == file_path}")
                    else:
                        print("  File not found on disk.")
                    print("-" * 40)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_sfc_state()
