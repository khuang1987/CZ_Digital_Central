import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    print("=== raw_mes table column order ===")
    cur.execute("""
        SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'raw_mes' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    
    for row in cur.fetchall():
        pos, name, dtype = row
        print(f"{pos:2d}. {name:30s} {dtype}")
        
        if pos == 14:
            print(f"    ^^^ This is parameter 14 (0-indexed: 13)")
