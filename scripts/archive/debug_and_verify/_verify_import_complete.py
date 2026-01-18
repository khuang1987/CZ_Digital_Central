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
    print("MES 数据导入完成验证")
    print("=" * 80)
    
    # 1. 总记录数
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes")
    total = cur.fetchone()[0]
    print(f"\n✓ 总记录数: {total:,}")
    
    # 2. 按年份统计
    cur.execute("""
        SELECT YEAR(TrackOutTime) as year, COUNT(*) as cnt
        FROM dbo.raw_mes
        WHERE TrackOutTime IS NOT NULL
        GROUP BY YEAR(TrackOutTime)
        ORDER BY year
    """)
    print("\n按年份统计:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")
    
    # 3. 按工厂统计
    cur.execute("""
        SELECT factory_name, COUNT(*) as cnt
        FROM dbo.raw_mes
        GROUP BY factory_name
        ORDER BY factory_name
    """)
    print("\n按工厂统计:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]:,}")
    
    # 4. 检查时间戳字段
    print("\n" + "=" * 80)
    print("时间戳字段检查")
    print("=" * 80)
    
    cur.execute("""
        SELECT COUNT(*) as missing_created
        FROM dbo.raw_mes
        WHERE created_at IS NULL
    """)
    missing_created = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(*) as missing_updated
        FROM dbo.raw_mes
        WHERE updated_at IS NULL
    """)
    missing_updated = cur.fetchone()[0]
    
    if missing_created == 0 and missing_updated == 0:
        print("\n✓ created_at 和 updated_at 字段已正确填充")
        
        # 显示时间戳样本
        cur.execute("""
            SELECT TOP 3 BatchNumber, created_at, updated_at
            FROM dbo.raw_mes
            WHERE created_at IS NOT NULL
            ORDER BY id
        """)
        print("\n时间戳样本:")
        for row in cur.fetchall():
            print(f"  {row[0]}: created={row[1]}, updated={row[2]}")
    else:
        print(f"\n⚠️ created_at 缺失: {missing_created:,} ({missing_created/total*100:.2f}%)")
        print(f"⚠️ updated_at 缺失: {missing_updated:,} ({missing_updated/total*100:.2f}%)")
    
    # 5. 检查关键字段完整性
    print("\n" + "=" * 80)
    print("关键字段完整性")
    print("=" * 80)
    
    key_fields = [
        ('BatchNumber', 'nvarchar'),
        ('Operation', 'nvarchar'),
        ('CFN', 'nvarchar'),
        ('ProductNumber', 'nvarchar'),
        ('Group', 'nvarchar'),
        ('TrackOutTime', 'datetime2')
    ]
    
    for field, dtype in key_fields:
        # Use string formatting to avoid f-string bracket issues
        sql = """
            SELECT COUNT(*) as missing_count
            FROM dbo.raw_mes
            WHERE [{}] IS NULL OR [{}] = ''
        """.format(field, field)
        cur.execute(sql)
        missing = cur.fetchone()[0]
        pct = missing/total*100
        status = "✓" if pct < 10 else "⚠️"
        print(f"{status} {field:20s}: {missing:8,} 缺失 ({pct:5.2f}%)")
    
    print("\n" + "=" * 80)
    print("数据导入验证完成")
    print("=" * 80)
