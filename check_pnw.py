import pyodbc
import os

server = os.getenv('MDDAP_SQL_SERVER', r'localhost\SQLEXPRESS')
db = os.getenv('MDDAP_SQL_DATABASE', 'mddap_v2')
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;'

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("=== K25M2170 Op20 视图结果 ===")
cursor.execute("""
SELECT [PT(d)], [PNW(d)]
FROM dbo.v_mes_metrics
WHERE BatchNumber='K25M2170' AND LTRIM(RTRIM(Operation))='20'
""")
for r in cursor.fetchall():
    print(f"PT(d): {r[0]} 天")
    print(f"PNW(d): {r[1]} 天")

conn.close()
