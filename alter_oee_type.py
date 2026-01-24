import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;'

conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

print("Converting OEE column from REAL to DECIMAL(10,2)...")
try:
    # SQL Server allows altering column type if data is compatible
    cursor.execute("ALTER TABLE dbo.raw_sap_routing ALTER COLUMN OEE DECIMAL(10,2)")
    print("Column altered successfully.")
    
    print("Verification:")
    cursor.execute("SELECT TOP 5 OEE FROM dbo.raw_sap_routing ORDER BY created_at DESC")
    for r in cursor.fetchall():
        print(f"OEE: {r[0]}")
        
except Exception as e:
    print(f"Error altering column: {e}")

conn.close()
