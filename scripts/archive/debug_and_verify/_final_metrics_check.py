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
    
    print("=" * 80)
    print("v_mes_metrics 视图最终数据质量报告")
    print("=" * 80)
    
    # 1. 视图总记录数
    cur.execute("SELECT COUNT(*) FROM dbo.v_mes_metrics")
    total = cur.fetchone()[0]
    print(f"\n✓ 视图总记录数: {total:,}")
    
    # 2. Routing 匹配完成度
    print("\n" + "=" * 80)
    print("Routing 匹配完成度（CFN + Operation + Group）")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as has_unit_time,
            COUNT(CASE WHEN ST_d IS NOT NULL THEN 1 END) as has_st_d
        FROM dbo.v_mes_metrics
    """)
    row = cur.fetchone()
    
    matched_count = row[1]
    matched_rate = matched_count / total * 100 if total > 0 else 0
    
    print(f"\n✓ 有 unit_time（单件时间）: {matched_count:,} / {total:,}")
    print(f"✓ Routing 匹配率: {matched_rate:.2f}%")
    print(f"✓ 有 ST_d（标准时间天数）: {row[2]:,} / {total:,} ({row[2]/total*100:.2f}%)")
    
    # 3. CompletionStatus 分布
    print("\n" + "=" * 80)
    print("CompletionStatus 分布")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            CompletionStatus,
            COUNT(*) as cnt,
            CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS DECIMAL(5,2)) as pct
        FROM dbo.v_mes_metrics
        GROUP BY CompletionStatus
        ORDER BY cnt DESC
    """)
    
    print("\nStatus                  记录数          占比")
    print("-" * 60)
    for row in cur.fetchall():
        status = row[0] if row[0] else 'NULL'
        print(f"{status:20s} {row[1]:10,}      {row[2]:6.2f}%")
    
    # 4. 关键指标字段完整性（不计算平均值以避免溢出）
    print("\n" + "=" * 80)
    print("关键指标字段完整性")
    print("=" * 80)
    
    key_metrics = ['LT_d', 'ST_d', 'unit_time']
    
    for metric in key_metrics:
        cur.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN [{metric}] IS NOT NULL THEN 1 END) as has_value
            FROM dbo.v_mes_metrics
        """)
        row = cur.fetchone()
        has_value = row[1]
        pct = has_value / row[0] * 100 if row[0] > 0 else 0
        
        print(f"{metric:15s}: {has_value:8,} / {row[0]:8,} ({pct:5.2f}%)")
    
    # 5. 按年份统计
    print("\n" + "=" * 80)
    print("按年份统计 Routing 匹配率")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            YEAR(TrackOutTime) as year,
            COUNT(*) as total,
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as matched,
            CAST(COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as match_rate
        FROM dbo.v_mes_metrics
        WHERE TrackOutTime IS NOT NULL
        GROUP BY YEAR(TrackOutTime)
        ORDER BY year
    """)
    
    print("\n年份    总记录数      匹配数      匹配率")
    print("-" * 60)
    for row in cur.fetchall():
        print(f"{row[0]:4d}  {row[1]:10,}  {row[2]:10,}    {row[3]:6.2f}%")
    
    # 6. 按工厂统计
    print("\n" + "=" * 80)
    print("按工厂统计 Routing 匹配率")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            factory_name,
            COUNT(*) as total,
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as matched,
            CAST(COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2)) as match_rate
        FROM dbo.v_mes_metrics
        GROUP BY factory_name
        ORDER BY factory_name
    """)
    
    print("\n工厂            总记录数      匹配数      匹配率")
    print("-" * 60)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:10,}  {row[2]:10,}    {row[3]:6.2f}%")
    
    # 7. 样本数据
    print("\n" + "=" * 80)
    print("样本数据（OnTime 状态的前5条记录）")
    print("=" * 80)
    
    cur.execute("""
        SELECT TOP 5
            BatchNumber, Operation, CFN, TrackOutQuantity,
            CAST(LT_d AS DECIMAL(10,2)) as LT_d, 
            CAST(ST_d AS DECIMAL(10,2)) as ST_d, 
            CAST(unit_time AS DECIMAL(10,2)) as unit_time, 
            CompletionStatus
        FROM dbo.v_mes_metrics
        WHERE CompletionStatus = 'OnTime'
        ORDER BY id
    """)
    
    print("\n")
    for row in cur.fetchall():
        print(f"批次: {row[0]}, Op: {row[1]}, CFN: {row[2]}, Qty: {row[3]}")
        print(f"  LT(d)={row[4]:.2f}, ST(d)={row[5]:.2f}, unit_time={row[6]:.2f}min")
        print(f"  Status: {row[7]}")
        print()
    
    print("=" * 80)
    print("数据质量评估")
    print("=" * 80)
    
    if matched_rate >= 70:
        print(f"\n✓ Routing 匹配率 {matched_rate:.2f}% >= 70%")
        print("✓ 数据质量良好，可以导出到 Parquet 文件")
    elif matched_rate >= 60:
        print(f"\n⚠️ Routing 匹配率 {matched_rate:.2f}% 在 60-70% 之间")
        print("⚠️ 数据可用，但建议优化匹配逻辑")
    else:
        print(f"\n❌ Routing 匹配率 {matched_rate:.2f}% < 60%")
        print("❌ 建议检查匹配逻辑后再导出")
    
    print("=" * 80)
