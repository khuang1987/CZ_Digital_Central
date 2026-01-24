import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    print("Checking OEE column type...")
    cursor.execute("SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'raw_sap_routing' AND COLUMN_NAME = 'OEE'")
    row = cursor.fetchone()
    if row:
        print(f"Column: {row[0]}, Type: {row[1]}, Precision: {row[2]}, Scale: {row[3]}")
    else:
        print("Column OEE not found")
        
    print("\nSample values (casted to decimal):")
    # Checking if casting makes it look right
    cursor.execute("SELECT TOP 5 OEE, CAST(OEE AS DECIMAL(10,2)) as OEE_Dec FROM dbo.raw_sap_routing ORDER BY created_at DESC")
    for r in cursor.fetchall():
        print(f"Raw: {r[0]}, Casted: {r[1]}")
        
    conn.close()
except Exception as e:
    print(f"Error: {e}")
