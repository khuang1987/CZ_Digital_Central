"""检查 raw_mes 数据按年份分布"""
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
    print("raw_mes 数据按年份分布")
    print("=" * 80)
    
    # 按年份统计
    cur.execute("""
        SELECT 
            YEAR(TrackOutTime) as year,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage,
            MIN(TrackOutTime) as min_date,
            MAX(TrackOutTime) as max_date
        FROM dbo.raw_mes
        WHERE TrackOutTime IS NOT NULL
        GROUP BY YEAR(TrackOutTime)
        ORDER BY year
    """)
    
    print("\n年份    记录数          占比        最早日期            最晚日期")
    print("-" * 80)
    
    total = 0
    year_2025_count = 0
    
    for row in cur.fetchall():
        print(f"{row[0]:4d}  {row[1]:10,}  {row[2]:8.2f}%  {row[3]}  {row[4]}")
        total += row[1]
        if row[0] == 2025:
            year_2025_count = row[1]
    
    # 检查 NULL 日期
    cur.execute("""
        SELECT COUNT(*) 
        FROM dbo.raw_mes 
        WHERE TrackOutTime IS NULL
    """)
    null_count = cur.fetchone()[0]
    
    if null_count > 0:
        print(f"NULL  {null_count:10,}  {null_count/(total+null_count)*100:8.2f}%")
    
    print("-" * 80)
    print(f"总计  {total + null_count:10,}  100.00%")
    
    print("\n" + "=" * 80)
    print("删除预览")
    print("=" * 80)
    
    print(f"\n✓ 保留 2025 年数据: {year_2025_count:,} 条")
    print(f"✗ 将删除其他年份数据: {total - year_2025_count + null_count:,} 条")
    print(f"  删除比例: {(total - year_2025_count + null_count)/(total + null_count)*100:.2f}%")
    
    # 按工厂统计 2025 年数据
    print("\n" + "=" * 80)
    print("2025 年数据按工厂分布")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            factory_name,
            COUNT(*) as count
        FROM dbo.raw_mes
        WHERE YEAR(TrackOutTime) = 2025
        GROUP BY factory_name
        ORDER BY factory_name
    """)
    
    print("\n工厂            记录数")
    print("-" * 40)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:10,}")
    
    print("\n" + "=" * 80)
