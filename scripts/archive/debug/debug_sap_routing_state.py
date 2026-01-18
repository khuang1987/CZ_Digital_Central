import pyodbc
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

def check_file_state():
    print("Checking etl_file_state for sap_routing_raw_...")
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT etl_name, file_path, file_mtime, file_size, processed_time FROM dbo.etl_file_state WHERE etl_name LIKE 'sap_routing_raw_%'")
            rows = cursor.fetchall()
            
            if not rows:
                print("No records found.")
            else:
                for row in rows:
                    etl_name, file_path, file_mtime, file_size, processed_time = row
                    print(f"ETL: {etl_name}")
                    print(f"  Path: {file_path}")
                    print(f"  DB Mtime: {file_mtime}, DB Size: {file_size}")
                    print(f"  Processed: {processed_time}")
                    
                    if os.path.exists(file_path):
                        curr_mtime = os.path.getmtime(file_path)
                        curr_size = os.path.getsize(file_path)
                        print(f"  FS Mtime: {curr_mtime}, FS Size: {curr_size}")
                        print(f"  Match: Mtime={abs(curr_mtime - file_mtime) < 1}, Size={curr_size == file_size}")
                    else:
                        print("  File not found on disk.")
                    print("-" * 40)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_file_state()
