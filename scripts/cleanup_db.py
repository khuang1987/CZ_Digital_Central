import pyodbc
import os

try:
    conn_str = (
        r"Driver={ODBC Driver 17 for SQL Server};"
        r"Server=localhost;"
        r"Database=mddap_v2;"
        r"Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    print("Deleting from KPI_Data...")
    cursor.execute("DELETE FROM dbo.KPI_Data")
    print(f"Deleted {cursor.rowcount} rows from KPI_Data")
    
    print("Deleting from TriggerCaseRegistry...")
    cursor.execute("DELETE FROM dbo.TriggerCaseRegistry")
    print(f"Deleted {cursor.rowcount} rows from TriggerCaseRegistry")
    
    conn.commit()
    print("Commit successful.")
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM dbo.TriggerCaseRegistry")
    count = cursor.fetchone()[0]
    print(f"TriggerCaseRegistry count: {count}")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals(): conn.close()
