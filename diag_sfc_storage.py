import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    print('--- Table Column Definitions ---')
    cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'raw_sfc_inspection'
    ORDER BY ORDINAL_POSITION
    """)
    for row in cursor.fetchall():
        print(row)

    print('\n--- Average & Max String Lengths (Sample 10,000) ---')
    cursor.execute("""
    SELECT TOP 10000 * FROM dbo.raw_sfc_inspection
    """)
    rows = cursor.fetchall()
    cols = [column[0] for column in cursor.description if column[0] not in ('id', 'created_at', 'ReportDate')]
    
    for ci, cname in enumerate(cols):
        idx = -1
        # find index of column in description
        for i, col_desc in enumerate(cursor.description):
            if col_desc[0] == cname:
                idx = i
                break
        
        lengths = [len(str(r[idx])) for r in rows if r[idx] is not None]
        if lengths:
            print(f"{cname:20} | Avg: {sum(lengths)/len(lengths):3.1f} | Max: {max(lengths):3}")
        else:
            print(f"{cname:20} | (No data)")

    print('\n--- Index & Space Usage ---')
    cursor.execute("""
    EXEC sp_spaceused 'raw_sfc_inspection'
    """)
    res = cursor.fetchone()
    print(f"Name: {res[0]}, Rows: {res[1]}, Reserved: {res[2]}, Data: {res[3]}, IndexSize: {res[4]}, Unused: {res[5]}")

    print('\n--- All Indexes for Table ---')
    cursor.execute("""
    SELECT 
        i.name as IndexName,
        i.type_desc,
        ISNULL(STUFF((SELECT ', ' + c.name
                      FROM sys.index_columns ic
                      JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                      WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id
                      ORDER BY ic.index_column_id
                      FOR XML PATH('')), 1, 2, ''), '') as Columns
    FROM sys.indexes i
    WHERE i.object_id = OBJECT_ID('dbo.raw_sfc_inspection')
    """)
    for row in cursor.fetchall():
        print(f"Index: {row[0]} ({row[1]}) | Columns: {row[2]}")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()
