"""验证 2025 年数据质量"""
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
    print("2025 年 MES 数据质量验证")
    print("=" * 80)
    
    # 1. 数据量统计
    print("\n" + "=" * 80)
    print("数据量统计")
    print("=" * 80)
    
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes")
    raw_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM dbo.v_mes_metrics")
    view_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes WHERE BatchNumber LIKE '%-0%'")
    sub_batch = cur.fetchone()[0]
    
    print(f"\nraw_mes 总记录数: {raw_count:,}")
    print(f"  其中子批次（含 -0）: {sub_batch:,} ({sub_batch/raw_count*100:.2f}%)")
    print(f"  主批次（不含 -0）: {raw_count - sub_batch:,} ({(raw_count - sub_batch)/raw_count*100:.2f}%)")
    print(f"v_mes_metrics 记录数: {view_count:,}")
    
    # 2. 年份验证
    print("\n" + "=" * 80)
    print("年份验证")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            YEAR(TrackOutTime) as year,
            COUNT(*) as count
        FROM dbo.v_mes_metrics
        WHERE TrackOutTime IS NOT NULL
        GROUP BY YEAR(TrackOutTime)
        ORDER BY year
    """)
    
    print("\n年份    记录数")
    print("-" * 40)
    for row in cur.fetchall():
        print(f"{row[0]:4d}  {row[1]:10,}")
    
    # 3. 月份分布
    print("\n" + "=" * 80)
    print("2025 年月份分布")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            MONTH(TrackOutTime) as month,
            COUNT(*) as count
        FROM dbo.v_mes_metrics
        WHERE YEAR(TrackOutTime) = 2025
        GROUP BY MONTH(TrackOutTime)
        ORDER BY month
    """)
    
    print("\n月份    记录数")
    print("-" * 40)
    for row in cur.fetchall():
        print(f"{row[0]:4d}  {row[1]:10,}")
    
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
    total_records = 0
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:10,}  {row[2]:8.2f}%")
        total_records += row[1]
    
    # 6. 按工厂统计
    print("\n" + "=" * 80)
    print("按工厂统计")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            factory_name,
            COUNT(*) as total,
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) as has_routing,
            COUNT(CASE WHEN CompletionStatus = 'OnTime' THEN 1 END) as ontime_count
        FROM dbo.v_mes_metrics
        GROUP BY factory_name
        ORDER BY factory_name
    """)
    
    print("\n工厂            总记录数      Routing匹配    OnTime记录")
    print("-" * 70)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:10,}  {row[2]:10,} ({row[2]/row[1]*100:5.1f}%)  "
              f"{row[3]:10,} ({row[3]/row[1]*100:5.1f}%)")
    
    # 7. 数据质量评分
    print("\n" + "=" * 80)
    print("数据质量评分")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(CASE WHEN unit_time IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as routing_rate,
            COUNT(CASE WHEN ST_d IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as st_rate,
            COUNT(CASE WHEN CompletionStatus IN ('OnTime', 'Delayed') THEN 1 END) * 100.0 / COUNT(*) as valid_status_rate
        FROM dbo.v_mes_metrics
    """)
    
    row = cur.fetchone()
    print(f"\n✓ Routing 匹配率: {row[0]:.2f}%")
    print(f"✓ ST_d 完成率: {row[1]:.2f}%")
    print(f"✓ 有效状态率（OnTime/Delayed）: {row[2]:.2f}%")
    
    print("\n" + "=" * 80)
    print(f"✓ 2025 年数据验证完成，共 {view_count:,} 条记录")
    print("=" * 80)
