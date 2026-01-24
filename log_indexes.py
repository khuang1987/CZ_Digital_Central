import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    with open('index_report.txt', 'w', encoding='utf-8') as f:
        f.write('--- Comprehensive Index List for raw_sfc_inspection ---\n')
        sql = """
        SELECT 
            i.name AS IndexName,
            i.type_desc AS IndexType,
            ISNULL(STUFF((SELECT ', ' + c.name
                          FROM sys.index_columns ic
                          JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                          WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                          ORDER BY ic.index_column_id
                          FOR XML PATH('')), 1, 2, ''), '') as Columns,
            (SUM(s.used_page_count) * 8) / 1024 AS SizeMB
        FROM sys.indexes i
        JOIN sys.dm_db_partition_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id
        WHERE i.object_id = OBJECT_ID('dbo.raw_sfc_inspection')
        GROUP BY i.name, i.type_desc, i.object_id, i.index_id
        ORDER BY SizeMB DESC
        """
        cursor.execute(sql)
        for row in cursor.fetchall():
            f.write(f"Index: {str(row[0]):35} | Type: {row[1]:15} | Size: {row[3]:>6} MB | Columns: {row[2]}\n")

except Exception as e:
    with open('index_report.txt', 'a', encoding='utf-8') as f:
        f.write(f"\nError: {e}\n")
finally:
    if 'conn' in locals():
        conn.close()
