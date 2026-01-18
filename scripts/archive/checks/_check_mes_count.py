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
    
    # Total count
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes")
    total = cur.fetchone()[0]
    print(f"Total records in raw_mes: {total:,}")
    
    # Count by factory
    cur.execute("""
        SELECT factory_name, COUNT(*) as cnt
        FROM dbo.raw_mes
        GROUP BY factory_name
        ORDER BY factory_name
    """)
    print("\nRecords by factory:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")
    
    # Count by year (from TrackOutTime)
    cur.execute("""
        SELECT YEAR(TrackOutTime) as year, COUNT(*) as cnt
        FROM dbo.raw_mes
        WHERE TrackOutTime IS NOT NULL
        GROUP BY YEAR(TrackOutTime)
        ORDER BY year
    """)
    print("\nRecords by year:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")
