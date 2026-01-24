import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # Check total rows vs distinct rows (excluding technical ID)
    print("Checking for duplicate records in raw_sfc_inspection...")
    
    # We'll pick a few key columns to identify a unique record if possible, or just check the whole row
    # In raw tables, usually we check if the exact same data from the same file was imported twice
    
    cursor.execute("""
    SELECT TOP 10 * FROM dbo.raw_sfc_inspection
    """)
    cols = [column[0] for column in cursor.description if column[0] not in ('id', 'created_at')]
    col_str = ", ".join([f"[{c}]" for c in cols])
    
    sql = f"""
    SELECT COUNT(*) as TotalRows, 
           COUNT(DISTINCT CHECKSUM({col_str})) as ApproxDistinctRows
    FROM dbo.raw_sfc_inspection
    """
    # Note: CHECKSUM isn't perfect but good for a quick check. 
    # Better: Group by all columns and count > 1
    
    print("Top Duplicate Records (grouped by all business columns):")
    sql_dupes = f"""
    SELECT TOP 10 COUNT(*) as Occurrence, source_file, {col_str}
    FROM dbo.raw_sfc_inspection
    GROUP BY source_file, {col_str}
    HAVING COUNT(*) > 1
    ORDER BY Occurrence DESC
    """
    cursor.execute(sql_dupes)
    rows = cursor.fetchall()
    if not rows:
        print("No exact duplicates found per (source_file + business columns).")
    for row in rows:
        print(f"Occurrence: {row[0]} | File: {row[1]}")

    print("\nChecking if multiple files contain the same data (Cross-file duplication):")
    # For inspection data, each file might be a snapshot. If the ETL appends snapshots, it grows fast.
    sql_cross = f"""
    SELECT TOP 10 COUNT(DISTINCT source_file) as FileCount, {col_str}
    FROM dbo.raw_sfc_inspection
    GROUP BY {col_str}
    HAVING COUNT(DISTINCT source_file) > 1
    ORDER BY FileCount DESC
    """
    cursor.execute(sql_cross)
    rows = cursor.fetchall()
    if not rows:
        print("No cross-file duplicates found.")
    for row in rows:
        print(f"Appear in {row[0]} different source files.")

except Exception as e:
    print(f"Error: {e}")
finally:
    if 'conn' in locals():
        conn.close()
