"""验证过滤子批次后的数据质量"""
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
    print("过滤子批次后的数据质量验证")
    print("=" * 80)
    
    # 1. 数据量对比
    print("\n" + "=" * 80)
    print("数据量对比")
    print("=" * 80)
    
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes")
    raw_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes WHERE BatchNumber LIKE '%-0%'")
    sub_batch_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM dbo.v_mes_metrics")
    view_total = cur.fetchone()[0]
    
    print(f"\nraw_mes 总记录数: {raw_total:,}")
    print(f"子批次记录数（含 -0）: {sub_batch_count:,} ({sub_batch_count/raw_total*100:.2f}%)")
    print(f"主批次记录数（不含 -0）: {raw_total - sub_batch_count:,} ({(raw_total - sub_batch_count)/raw_total*100:.2f}%)")
    print(f"v_mes_metrics 记录数: {view_total:,}")
    
    # 2. 验证过滤效果
    print("\n" + "=" * 80)
    print("验证过滤效果")
    print("=" * 80)
    
    cur.execute("""
        SELECT COUNT(*) 
        FROM dbo.v_mes_metrics 
        WHERE BatchNumber LIKE '%-0%'
    """)
    
    filtered_sub = cur.fetchone()[0]
    print(f"\n✓ 视图中包含 -0 的记录: {filtered_sub:,}")
    
    if filtered_sub == 0:
        print("✓ 子批次过滤成功，视图中无 -0 记录")
    else:
        print("⚠️ 仍有子批次记录未被过滤")
    
    # 3. 样本数据检查
    print("\n" + "=" * 80)
    print("样本批次号检查（前20条）")
    print("=" * 80)
    
    cur.execute("""
        SELECT TOP 20 BatchNumber
        FROM dbo.v_mes_metrics
        ORDER BY id
    """)
    
    print("\n批次号:")
    for row in cur.fetchall():
        print(f"  {row[0]}")
    
    # 4. Routing 匹配率
    print("\n" + "=" * 80)
    print("Routing 匹配完成度")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as has_unit_time,
            COUNT(CASE WHEN ST_d IS NOT NULL THEN 1 END) as has_st
        FROM dbo.v_mes_metrics
    """)
    
    row = cur.fetchone()
    print(f"\n✓ 总记录数: {row[0]:,}")
    print(f"✓ 有 unit_time: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
    print(f"✓ 有 ST_d: {row[2]:,} ({row[2]/row[0]*100:.2f}%)")
    
    # 5. CompletionStatus 分布
    print("\n" + "=" * 80)
    print("CompletionStatus 分布")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            CompletionStatus,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
        FROM dbo.v_mes_metrics
        GROUP BY CompletionStatus
        ORDER BY count DESC
    """)
    
    print("\nStatus           记录数          占比")
    print("-" * 60)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:10,}  {row[2]:8.2f}%")
    
    # 6. 按年份统计
    print("\n" + "=" * 80)
    print("按年份统计")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            YEAR(TrackOutTime) as year,
            COUNT(*) as total,
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as has_routing
        FROM dbo.v_mes_metrics
        WHERE TrackOutTime IS NOT NULL
        GROUP BY YEAR(TrackOutTime)
        ORDER BY year
    """)
    
    print("\n年份    总记录数      Routing匹配    匹配率")
    print("-" * 60)
    for row in cur.fetchall():
        print(f"{row[0]:4d}  {row[1]:10,}  {row[2]:10,}  {row[2]/row[1]*100:8.2f}%")
    
    # 7. 按工厂统计
    print("\n" + "=" * 80)
    print("按工厂统计")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            factory_name,
            COUNT(*) as total,
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as has_routing
        FROM dbo.v_mes_metrics
        GROUP BY factory_name
        ORDER BY factory_name
    """)
    
    print("\n工厂            总记录数      Routing匹配    匹配率")
    print("-" * 70)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:10,}  {row[2]:10,}  {row[2]/row[1]*100:8.2f}%")
    
    print("\n" + "=" * 80)
    print("✓ 数据过滤验证完成")
    print("=" * 80)
