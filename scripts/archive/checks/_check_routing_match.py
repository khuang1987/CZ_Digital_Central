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
    print("Routing 数据匹配率检查")
    print("=" * 80)
    
    # 1. 检查 raw_sap_routing 表的记录数
    cur.execute("SELECT COUNT(*) FROM dbo.raw_sap_routing")
    routing_count = cur.fetchone()[0]
    print(f"\nSAP Routing 表记录数: {routing_count:,}")
    
    # 2. 检查 raw_mes 表的记录数
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes")
    mes_count = cur.fetchone()[0]
    print(f"MES 原始数据记录数: {mes_count:,}")
    
    # 3. 检查匹配条件的字段分布
    print("\n" + "=" * 80)
    print("匹配字段分布检查")
    print("=" * 80)
    
    # MES 侧的字段分布
    print("\nMES 数据字段分布:")
    
    # CFN 分布
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN CFN IS NOT NULL AND CFN != '' THEN 1 END) as has_cfn
        FROM dbo.raw_mes
    """)
    row = cur.fetchone()
    print(f"  CFN 有值: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    
    # ProductNumber 分布
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN ProductNumber IS NOT NULL AND ProductNumber != '' THEN 1 END) as has_pn
        FROM dbo.raw_mes
    """)
    row = cur.fetchone()
    print(f"  ProductNumber 有值: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    
    # Operation 分布
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN Operation IS NOT NULL AND Operation != '' THEN 1 END) as has_op
        FROM dbo.raw_mes
    """)
    row = cur.fetchone()
    print(f"  Operation 有值: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    
    # Group 分布
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN [Group] IS NOT NULL AND [Group] != '' THEN 1 END) as has_group
        FROM dbo.raw_mes
    """)
    row = cur.fetchone()
    print(f"  Group 有值: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    
    # Routing 侧的字段分布
    print("\nRouting 数据字段分布:")
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN CFN IS NOT NULL AND CFN != '' THEN 1 END) as has_cfn,
            COUNT(CASE WHEN ProductNumber IS NOT NULL AND ProductNumber != '' THEN 1 END) as has_pn,
            COUNT(CASE WHEN Operation IS NOT NULL AND Operation != '' THEN 1 END) as has_op,
            COUNT(CASE WHEN [Group] IS NOT NULL AND [Group] != '' THEN 1 END) as has_group
        FROM dbo.raw_sap_routing
    """)
    row = cur.fetchone()
    print(f"  CFN 有值: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    print(f"  ProductNumber 有值: {row[2]:,} / {row[0]:,} ({row[2]/row[0]*100:.2f}%)")
    print(f"  Operation 有值: {row[3]:,} / {row[0]:,} ({row[3]/row[0]*100:.2f}%)")
    print(f"  Group 有值: {row[4]:,} / {row[0]:,} ({row[4]/row[0]*100:.2f}%)")
    
    # 4. 测试匹配率（使用 V2 视图的匹配逻辑）
    print("\n" + "=" * 80)
    print("Routing 匹配率测试")
    print("=" * 80)
    
    # 方案1: CFN + Operation + Group 三字段匹配
    print("\n方案1: CFN + Operation + Group 匹配")
    cur.execute("""
        SELECT COUNT(*) as matched_count
        FROM dbo.raw_mes m
        INNER JOIN dbo.raw_sap_routing r
            ON m.CFN = r.CFN
            AND m.Operation = r.Operation
            AND m.[Group] = r.[Group]
        WHERE m.CFN IS NOT NULL AND m.CFN != ''
            AND m.Operation IS NOT NULL AND m.Operation != ''
            AND m.[Group] IS NOT NULL AND m.[Group] != ''
    """)
    matched_3field = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(*) 
        FROM dbo.raw_mes
        WHERE CFN IS NOT NULL AND CFN != ''
            AND Operation IS NOT NULL AND Operation != ''
            AND [Group] IS NOT NULL AND [Group] != ''
    """)
    eligible_3field = cur.fetchone()[0]
    
    match_rate_3field = matched_3field / eligible_3field * 100 if eligible_3field > 0 else 0
    print(f"  可匹配记录数: {eligible_3field:,}")
    print(f"  成功匹配数: {matched_3field:,}")
    print(f"  匹配率: {match_rate_3field:.2f}%")
    
    # 方案2: ProductNumber + Operation 两字段匹配（备选）
    print("\n方案2: ProductNumber + Operation 匹配（备选）")
    cur.execute("""
        SELECT COUNT(*) as matched_count
        FROM dbo.raw_mes m
        INNER JOIN dbo.raw_sap_routing r
            ON m.ProductNumber = r.ProductNumber
            AND m.Operation = r.Operation
        WHERE m.ProductNumber IS NOT NULL AND m.ProductNumber != ''
            AND m.Operation IS NOT NULL AND m.Operation != ''
    """)
    matched_2field = cur.fetchone()[0]
    
    cur.execute("""
        SELECT COUNT(*) 
        FROM dbo.raw_mes
        WHERE ProductNumber IS NOT NULL AND ProductNumber != ''
            AND Operation IS NOT NULL AND Operation != ''
    """)
    eligible_2field = cur.fetchone()[0]
    
    match_rate_2field = matched_2field / eligible_2field * 100 if eligible_2field > 0 else 0
    print(f"  可匹配记录数: {eligible_2field:,}")
    print(f"  成功匹配数: {matched_2field:,}")
    print(f"  匹配率: {match_rate_2field:.2f}%")
    
    # 5. 综合匹配率（使用 COALESCE 逻辑）
    print("\n" + "=" * 80)
    print("综合匹配率（三字段优先，两字段备选）")
    print("=" * 80)
    
    cur.execute("""
        SELECT COUNT(*) as total_matched
        FROM dbo.raw_mes m
        WHERE EXISTS (
            SELECT 1 FROM dbo.raw_sap_routing r
            WHERE m.CFN = r.CFN
                AND m.Operation = r.Operation
                AND m.[Group] = r.[Group]
        )
        OR EXISTS (
            SELECT 1 FROM dbo.raw_sap_routing r
            WHERE m.ProductNumber = r.ProductNumber
                AND m.Operation = r.Operation
                AND (m.CFN IS NULL OR m.CFN = '' OR m.[Group] IS NULL OR m.[Group] = '')
        )
    """)
    total_matched = cur.fetchone()[0]
    overall_rate = total_matched / mes_count * 100
    
    print(f"\n总匹配数: {total_matched:,} / {mes_count:,}")
    print(f"总匹配率: {overall_rate:.2f}%")
    
    # 6. 查看未匹配的样本
    print("\n" + "=" * 80)
    print("未匹配样本分析（前10条）")
    print("=" * 80)
    
    cur.execute("""
        SELECT TOP 10
            m.CFN, m.ProductNumber, m.Operation, m.[Group],
            m.Product_Desc, m.factory_name
        FROM dbo.raw_mes m
        WHERE NOT EXISTS (
            SELECT 1 FROM dbo.raw_sap_routing r
            WHERE m.CFN = r.CFN
                AND m.Operation = r.Operation
                AND m.[Group] = r.[Group]
        )
        AND NOT EXISTS (
            SELECT 1 FROM dbo.raw_sap_routing r
            WHERE m.ProductNumber = r.ProductNumber
                AND m.Operation = r.Operation
        )
        AND m.CFN IS NOT NULL AND m.CFN != ''
        ORDER BY m.id
    """)
    
    print("\n未匹配记录样本:")
    for row in cur.fetchall():
        print(f"  CFN={row[0]}, PN={row[1]}, Op={row[2]}, Group={row[3]}")
        print(f"    Desc={row[4]}, Factory={row[5]}")
    
    print("\n" + "=" * 80)
    if overall_rate >= 80:
        print(f"✓ 匹配率 {overall_rate:.2f}% >= 80%，可以创建视图")
    elif overall_rate >= 60:
        print(f"⚠️ 匹配率 {overall_rate:.2f}% 在 60-80% 之间，建议检查后再创建视图")
    else:
        print(f"❌ 匹配率 {overall_rate:.2f}% < 60%，需要先解决匹配问题")
    print("=" * 80)
