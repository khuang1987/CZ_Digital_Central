"""Maintenance: Clear routing ETL file-state records (SQL Server).

Purpose:
  Remove routing-related rows from dbo.etl_file_state so routing ETLs can re-import.

Effects:
  - Deletes rows from dbo.etl_file_state where etl_name LIKE '%routing%'.

Run:
  python scripts/maintenance/_clear_routing_file_state.py
"""

import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

print("=" * 80)
print("清除 routing 文件状态记录")
print("=" * 80)

with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    # 检查当前记录
    cur.execute("""
        SELECT etl_name, file_path
        FROM dbo.etl_file_state
        WHERE etl_name LIKE '%routing%'
    """)
    
    records = cur.fetchall()
    print(f"\n找到 {len(records)} 条 routing 文件状态记录:")
    for row in records:
        print(f"  {row[0]}: {row[1]}")
    
    if records:
        # 删除记录
        print("\n删除文件状态记录...")
        cur.execute("""
            DELETE FROM dbo.etl_file_state
            WHERE etl_name LIKE '%routing%'
        """)
        deleted = cur.rowcount
        conn.commit()
        print(f"✓ 删除了 {deleted} 条记录")
    else:
        print("\n没有找到 routing 文件状态记录")

print("\n" + "=" * 80)
print("✓ 可以重新运行 ETL 导入")
print("=" * 80)
