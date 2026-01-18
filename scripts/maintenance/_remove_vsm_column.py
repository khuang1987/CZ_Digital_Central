"""Maintenance: Drop the VSM column from dbo.raw_mes (SQL Server).

Purpose:
  One-off schema cleanup to remove dbo.raw_mes.VSM if it exists.

Effects:
  - Alters dbo.raw_mes and drops column VSM when present.

Run:
  python scripts/maintenance/_remove_vsm_column.py
"""

import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

print("连接到 SQL Server...")
with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    # 检查 VSM 列是否存在
    cur.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'raw_mes' 
        AND COLUMN_NAME = 'VSM'
        AND TABLE_SCHEMA = 'dbo'
    """)
    
    vsm_exists = cur.fetchone()[0] > 0
    
    if vsm_exists:
        print("删除 VSM 列...")
        cur.execute("ALTER TABLE dbo.raw_mes DROP COLUMN VSM")
        conn.commit()
        print("✓ VSM 列已删除")
    else:
        print("VSM 列不存在，无需删除")
    
    # 验证表结构
    print("\n当前 raw_mes 表结构:")
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'raw_mes' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    
    for row in cur.fetchall():
        print(f"  {row[0]:30s} {row[1]:15s} nullable={row[2]}")
