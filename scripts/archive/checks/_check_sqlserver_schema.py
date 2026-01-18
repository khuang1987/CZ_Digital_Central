import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

print("=== Checking SQL Server Schema ===")

with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    # Check etl_file_state table structure
    print("\netl_file_state table columns:")
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'etl_file_state' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} (nullable={row[2]})")
    
    # Check raw_mes table structure
    print("\nraw_mes table columns:")
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'raw_mes' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]} (nullable={row[2]})")
