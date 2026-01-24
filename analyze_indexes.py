import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    print('--- Comprehensive Index Analysis ---')
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
        print(f"Index: {str(row[0]):35} | Type: {row[1]:15} | Size: {row[3]:>6} MB | Columns: {row[2]}")

    print('\n--- Average String Lengths (Top 1000) ---')
    cursor.execute("SELECT TOP 1000 * FROM dbo.raw_sfc_inspection")
    rows = cursor.fetchall()
    cols = [column[0] for column in cursor.description]
    for i, cname in enumerate(cols):
        lens = [len(str(r[i])) for r in rows if r[i] is not None]
        if lens:
            print(f"{cname:20} | Avg: {sum(lens)/len(lens):3.1f}")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()
