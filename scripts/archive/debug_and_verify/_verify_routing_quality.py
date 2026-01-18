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
    print("Routing 数据质量验证（重新导入后）")
    print("=" * 80)
    
    # 1. 总记录数
    cur.execute("SELECT COUNT(*) FROM dbo.raw_sap_routing")
    total = cur.fetchone()[0]
    print(f"\n✓ 总记录数: {total:,}")
    
    # 2. StandardTime 完整性
    print("\n" + "=" * 80)
    print("StandardTime 字段验证")
    print("=" * 80)
    
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
    print(f"\n✓ StandardTime > 0: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    if row[2]:
        print(f"  平均值: {row[2]:.1f} 分钟")
        print(f"  范围: {row[3]:.1f} - {row[4]:.1f} 分钟")
    
    # 检查小数位数
    cur.execute("""
        SELECT TOP 5 CFN, Operation, StandardTime
        FROM dbo.raw_sap_routing
        WHERE StandardTime IS NOT NULL AND StandardTime > 0
        ORDER BY id
    """)
    print("\n样本数据（检查小数位）:")
    for row in cur.fetchall():
        print(f"  CFN={row[0]}, Op={row[1]}, ST={row[2]:.1f} min")
    
    # 3. SetupTime 完整性
    print("\n" + "=" * 80)
    print("SetupTime 字段验证")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN SetupTime IS NOT NULL AND SetupTime > 0 THEN 1 END) as has_valid_setup
        FROM dbo.raw_sap_routing
    """)
    
    row = cur.fetchone()
    print(f"\n✓ SetupTime > 0: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    
    if row[1] > 0:
        cur.execute("""
            SELECT TOP 5 CFN, Operation, SetupTime
            FROM dbo.raw_sap_routing
            WHERE SetupTime IS NOT NULL AND SetupTime > 0
            ORDER BY id
        """)
        print("\n样本数据:")
        for row in cur.fetchall():
            print(f"  CFN={row[0]}, Op={row[1]}, SetupTime={row[2]:.1f} min")
    
    # 4. OEE 完整性
    print("\n" + "=" * 80)
    print("OEE 字段验证")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN OEE IS NOT NULL THEN 1 END) as has_oee,
            COUNT(CASE WHEN OEE IS NOT NULL AND OEE > 0 THEN 1 END) as has_valid_oee
        FROM dbo.raw_sap_routing
    """)
    
    row = cur.fetchone()
    print(f"\n✓ OEE 非空: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    print(f"✓ OEE > 0: {row[2]:,} / {row[0]:,} ({row[2]/row[0]*100:.2f}%)")
    
    if row[1] > 0:
        cur.execute("""
            SELECT TOP 5 CFN, Operation, OEE
            FROM dbo.raw_sap_routing
            WHERE OEE IS NOT NULL
            ORDER BY id
        """)
        print("\n样本数据:")
        for row in cur.fetchall():
            print(f"  CFN={row[0]}, Op={row[1]}, OEE={row[2]:.2f}")
    
    # 5. 按工厂统计
    print("\n" + "=" * 80)
    print("按工厂统计数据完整性")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            factory_code,
            COUNT(*) as total,
            COUNT(CASE WHEN StandardTime IS NOT NULL AND StandardTime > 0 THEN 1 END) as has_st,
            COUNT(CASE WHEN SetupTime IS NOT NULL AND SetupTime > 0 THEN 1 END) as has_setup,
            COUNT(CASE WHEN OEE IS NOT NULL THEN 1 END) as has_oee
        FROM dbo.raw_sap_routing
        GROUP BY factory_code
        ORDER BY factory_code
    """)
    
    print("\n工厂    总数      StandardTime  SetupTime    OEE")
    print("-" * 70)
    for row in cur.fetchall():
        print(f"{row[0]:6s} {row[1]:8,}  {row[2]:8,} ({row[2]/row[1]*100:5.1f}%)  "
              f"{row[3]:8,} ({row[3]/row[1]*100:5.1f}%)  {row[4]:8,} ({row[4]/row[1]*100:5.1f}%)")
    
    # 6. 综合评估
    print("\n" + "=" * 80)
    print("数据质量评估")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(CASE WHEN StandardTime IS NOT NULL AND StandardTime > 0 THEN 1 END) * 100.0 / COUNT(*) as st_rate
        FROM dbo.raw_sap_routing
    """)
    st_rate = cur.fetchone()[0]
    
    print(f"\n✓ StandardTime 完成率: {st_rate:.2f}%")
    
    if st_rate >= 95:
        print("✓ 数据质量优秀，可以用于 MES 指标计算")
    elif st_rate >= 90:
        print("⚠️ 数据质量良好，但建议检查缺失原因")
    else:
        print("❌ 数据质量需要改进")
    
    print("=" * 80)
