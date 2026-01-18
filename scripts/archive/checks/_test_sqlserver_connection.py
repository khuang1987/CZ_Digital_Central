import sys

try:
    import pyodbc
    print("✓ pyodbc installed")
except ImportError:
    print("✗ pyodbc not installed")
    sys.exit(1)

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
    "TrustServerCertificate=no;"
)

print("\nTesting SQL Server connection...")
print(f"Connection string: {conn_str}")

try:
    conn = pyodbc.connect(conn_str, timeout=10)
    print("✓ Connected to SQL Server")
    
    cur = conn.cursor()
    
    # Check if tables exist
    tables = cur.execute("""
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' 
        AND TABLE_NAME IN ('raw_mes', 'etl_file_state')
    """).fetchall()
    
    print(f"\nFound tables: {[t[0] for t in tables]}")
    
    if ('raw_mes',) in tables:
        cnt = cur.execute("SELECT COUNT(*) FROM dbo.raw_mes").fetchone()[0]
        print(f"  dbo.raw_mes: {cnt} rows")
    
    if ('etl_file_state',) in tables:
        cnt = cur.execute("SELECT COUNT(*) FROM dbo.etl_file_state WHERE etl_name LIKE 'mes_raw_%'").fetchone()[0]
        print(f"  dbo.etl_file_state (mes_raw_*): {cnt} rows")
    
    conn.close()
    print("\n✓ Connection test successful")
    
except Exception as e:
    print(f"\n✗ Connection failed: {type(e).__name__}: {e}")
    sys.exit(1)
