"""Maintenance: Populate StandardTime in dbo.raw_sap_routing (SQL Server).

Purpose:
  Backfill/update the StandardTime column from EH_machine and EH_labor.

Effects:
  - Updates dbo.raw_sap_routing.StandardTime for rows with non-null/positive EH values.
  - Formula: StandardTime (minutes) = (EH_machine + EH_labor) / 60.0

Run:
  python scripts/maintenance/_update_standardtime.py
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
print("更新 raw_sap_routing 表的 StandardTime 字段")
print("=" * 80)

with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    # 1. 检查当前状态
    print("\n检查当前状态...")
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN StandardTime IS NOT NULL THEN 1 END) as has_st,
            COUNT(CASE WHEN EH_machine IS NOT NULL OR EH_labor IS NOT NULL THEN 1 END) as has_eh
        FROM dbo.raw_sap_routing
    """)
    
    row = cur.fetchone()
    print(f"总记录数: {row[0]:,}")
    print(f"StandardTime 非空: {row[1]:,}")
    print(f"EH_machine/EH_labor 非空: {row[2]:,}")
    
    # 2. 计算 StandardTime
    # StandardTime (分钟) = (EH_machine + EH_labor) / 60
    print("\n计算 StandardTime...")
    print("公式: StandardTime (分钟) = (EH_machine + EH_labor) / 60")
    
    cur.execute("""
        UPDATE dbo.raw_sap_routing
        SET StandardTime = (ISNULL(EH_machine, 0) + ISNULL(EH_labor, 0)) / 60.0
        WHERE (EH_machine IS NOT NULL OR EH_labor IS NOT NULL)
            AND (EH_machine > 0 OR EH_labor > 0)
    """)
    
    affected_rows = cur.rowcount
    conn.commit()
    
    print(f"✓ 更新了 {affected_rows:,} 条记录")
    
    # 3. 验证更新结果
    print("\n验证更新结果...")
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN StandardTime IS NOT NULL AND StandardTime > 0 THEN 1 END) as has_valid_st,
            AVG(CASE WHEN StandardTime IS NOT NULL AND StandardTime > 0 THEN StandardTime END) as avg_st,
            MIN(CASE WHEN StandardTime IS NOT NULL AND StandardTime > 0 THEN StandardTime END) as min_st,
            MAX(CASE WHEN StandardTime IS NOT NULL AND StandardTime > 0 THEN StandardTime END) as max_st
        FROM dbo.raw_sap_routing
    """)
    
    row = cur.fetchone()
    print(f"StandardTime > 0 的记录数: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    if row[2]:
        print(f"StandardTime 平均值: {row[2]:.2f} 分钟")
        print(f"StandardTime 最小值: {row[3]:.2f} 分钟")
        print(f"StandardTime 最大值: {row[4]:.2f} 分钟")
    
    # 4. 查看样本数据
    print("\n样本数据（前5条）:")
    cur.execute("""
        SELECT TOP 5 
            CFN, Operation, [Group], 
            EH_machine, EH_labor, StandardTime
        FROM dbo.raw_sap_routing
        WHERE StandardTime IS NOT NULL AND StandardTime > 0
        ORDER BY id
    """)
    
    print("\nCFN              Op    Group      EH_machine  EH_labor  StandardTime(min)")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:5s} {row[2]:10s} {row[3]:10.1f}  {row[4]:8.1f}  {row[5]:8.2f}")

print("\n" + "=" * 80)
print("✓ StandardTime 更新完成")
print("=" * 80)
