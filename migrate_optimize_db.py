import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

try:
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    
    print("--- Starting Database Optimization ---")
    
    # 1. Drop redundant index
    print("Step 1: Dropping redundant index idx_raw_sfc_insp_hash...")
    cursor.execute("IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'idx_raw_sfc_insp_hash' AND object_id = OBJECT_ID('dbo.raw_sfc_inspection')) DROP INDEX idx_raw_sfc_insp_hash ON dbo.raw_sfc_inspection")
    
    # 2. Drop source_file column
    print("Step 2: Dropping source_file column from raw_sfc_inspection...")
    # First drop any dependent indexes (if any exist on this column)
    # Check if any index uses source_file
    cursor.execute("""
        SELECT i.name
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID('dbo.raw_sfc_inspection') AND c.name = 'source_file'
    """)
    dep_indexes = cursor.fetchall()
    for idx in dep_indexes:
        print(f"Dropping dependent index: {idx[0]}")
        cursor.execute(f"DROP INDEX [{idx[0]}] ON dbo.raw_sfc_inspection")
    
    # Now drop the column
    cursor.execute("IF EXISTS (SELECT 1 FROM sys.columns WHERE name = 'source_file' AND object_id = OBJECT_ID('dbo.raw_sfc_inspection')) ALTER TABLE dbo.raw_sfc_inspection DROP COLUMN source_file")
    
    # 3. Shrink Database
    print("Step 3: Shrinking database to reclaim space (this may take a few minutes)...")
    cursor.execute(f"DBCC SHRINKDATABASE(N'{db}')")
    
    print("\n--- Optimization Complete ---")
    
    # Verify final sizes
    cursor.execute("SELECT name, size*8/1024 AS SizeMB FROM sys.master_files WHERE database_id = DB_ID(?)", (db,))
    for row in cursor.fetchall():
        print(f"File: {row[0]} - Current Size: {row[1]}MB")

except Exception as e:
    print(f"Error during migration: {e}")
finally:
    if 'conn' in locals():
        conn.close()
