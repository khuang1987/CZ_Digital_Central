import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    print('--- Final Table Sizes ---')
    cursor.execute("""
    SELECT TOP 5
        t.NAME AS TableName,
        p.rows AS RowCounts,
        (SUM(a.total_pages) * 8) / 1024 AS TotalMB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    WHERE t.is_ms_shipped = 0 AND i.OBJECT_ID > 255
    GROUP BY t.Name, p.Rows
    ORDER BY TotalMB DESC
    """)
    for row in cursor.fetchall():
        print(f"Table: {row[0]:<30} | Rows: {row[1]:>10} | Total: {row[2]:>6}MB")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()
