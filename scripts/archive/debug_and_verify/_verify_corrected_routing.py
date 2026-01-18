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
    print("Routing 数据质量验证（修正计算逻辑后）")
    print("=" * 80)
    
    # 1. StandardTime 验证
    print("\n" + "=" * 80)
    print("StandardTime 计算验证（优先 machine，否则 labor）")
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
    
    # 检查计算逻辑是否正确
    cur.execute("""
        SELECT TOP 10
            CFN, Operation,
            EH_machine, EH_labor,
            StandardTime,
            CASE 
                WHEN EH_machine > 0 THEN EH_machine / 60.0
                ELSE EH_labor / 60.0
            END as expected_st
        FROM dbo.raw_sap_routing
        WHERE StandardTime IS NOT NULL AND StandardTime > 0
        ORDER BY id
    """)
    
    print("\n样本数据（验证计算逻辑）:")
    print("CFN              Op    Machine  Labor    ST(实际) ST(预期)")
    print("-" * 75)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:5s} {row[2]:8.1f} {row[3]:8.1f} {row[4]:8.1f} {row[5]:8.1f}")
    
    # 2. OEE 验证
    print("\n" + "=" * 80)
    print("OEE 字段验证（0 值应替换为 77%）")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN OEE IS NOT NULL THEN 1 END) as has_oee,
            COUNT(CASE WHEN OEE = 0.77 THEN 1 END) as default_oee,
            COUNT(CASE WHEN OEE > 0 AND OEE != 0.77 THEN 1 END) as custom_oee,
            AVG(OEE) as avg_oee,
            MIN(OEE) as min_oee,
            MAX(OEE) as max_oee
        FROM dbo.raw_sap_routing
    """)
    
    row = cur.fetchone()
    print(f"\n✓ OEE 非空: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    print(f"✓ OEE = 0.77 (默认值): {row[2]:,} ({row[2]/row[0]*100:.2f}%)")
    print(f"✓ OEE > 0 (自定义值): {row[3]:,} ({row[3]/row[0]*100:.2f}%)")
    if row[4]:
        print(f"  平均值: {row[4]:.2%}")
        print(f"  范围: {row[5]:.2%} - {row[6]:.2%}")
    
    # 样本数据
    cur.execute("""
        SELECT TOP 5 CFN, Operation, OEE
        FROM dbo.raw_sap_routing
        WHERE OEE = 0.77
        ORDER BY id
    """)
    print("\n样本数据（默认 OEE = 77%）:")
    for row in cur.fetchall():
        print(f"  CFN={row[0]}, Op={row[1]}, OEE={row[2]:.2%}")
    
    # 3. 按工厂统计
    print("\n" + "=" * 80)
    print("按工厂统计")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            factory_code,
            COUNT(*) as total,
            COUNT(CASE WHEN StandardTime > 0 THEN 1 END) as has_st,
            COUNT(CASE WHEN OEE = 0.77 THEN 1 END) as default_oee,
            COUNT(CASE WHEN OEE > 0 AND OEE != 0.77 THEN 1 END) as custom_oee,
            AVG(CASE WHEN StandardTime > 0 THEN StandardTime END) as avg_st
        FROM dbo.raw_sap_routing
        GROUP BY factory_code
        ORDER BY factory_code
    """)
    
    print("\n工厂    总数      StandardTime  默认OEE(77%)  自定义OEE    平均ST")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:6s} {row[1]:8,}  {row[2]:8,} ({row[2]/row[1]*100:5.1f}%)  "
              f"{row[3]:8,} ({row[3]/row[1]*100:5.1f}%)  {row[4]:8,} ({row[4]/row[1]*100:5.1f}%)  {row[5]:6.1f}min")
    
    # 4. 综合评估
    print("\n" + "=" * 80)
    print("数据质量评估")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(CASE WHEN StandardTime > 0 THEN 1 END) * 100.0 / COUNT(*) as st_rate,
            COUNT(CASE WHEN OEE IS NOT NULL THEN 1 END) * 100.0 / COUNT(*) as oee_rate
        FROM dbo.raw_sap_routing
    """)
    row = cur.fetchone()
    
    print(f"\n✓ StandardTime 完成率: {row[0]:.2f}%")
    print(f"✓ OEE 完成率: {row[1]:.2f}%")
    print(f"✓ 计算逻辑: 优先 EH_machine，否则 EH_labor")
    print(f"✓ OEE 默认值: 77% (当原值为 0 时)")
    
    if row[0] >= 95:
        print("\n✅ 数据质量优秀，可以用于 MES 指标计算")
    else:
        print("\n⚠️ 数据质量需要改进")
    
    print("=" * 80)
