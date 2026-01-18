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
    print("诊断 Routing 匹配失败原因")
    print("=" * 80)
    
    # 1. 检查 raw_sap_routing 表的数据
    print("\n检查 raw_sap_routing 表...")
    cur.execute("SELECT COUNT(*) FROM dbo.raw_sap_routing")
    routing_count = cur.fetchone()[0]
    print(f"Routing 表总记录数: {routing_count:,}")
    
    # 2. 检查 routing 表的字段样本
    print("\nRouting 表样本数据（前5条）:")
    cur.execute("""
        SELECT TOP 5 
            CFN, ProductNumber, Operation, [Group], StandardTime
        FROM dbo.raw_sap_routing
        WHERE CFN IS NOT NULL AND CFN != ''
            AND Operation IS NOT NULL AND Operation != ''
            AND [Group] IS NOT NULL AND [Group] != ''
    """)
    
    for row in cur.fetchall():
        print(f"  CFN={row[0]}, PN={row[1]}, Op={row[2]}, Group={row[3]}, ST={row[4]}")
    
    # 3. 检查 MES 表的字段样本
    print("\nMES 表样本数据（前5条）:")
    cur.execute("""
        SELECT TOP 5 
            CFN, ProductNumber, Operation, [Group]
        FROM dbo.raw_mes
        WHERE CFN IS NOT NULL AND CFN != ''
            AND Operation IS NOT NULL AND Operation != ''
            AND [Group] IS NOT NULL AND [Group] != ''
    """)
    
    for row in cur.fetchall():
        print(f"  CFN={row[0]}, PN={row[1]}, Op={row[2]}, Group={row[3]}")
    
    # 4. 尝试直接 JOIN 测试
    print("\n" + "=" * 80)
    print("直接 JOIN 测试")
    print("=" * 80)
    
    cur.execute("""
        SELECT COUNT(*) as match_count
        FROM dbo.raw_mes m
        INNER JOIN dbo.raw_sap_routing r
            ON m.CFN = r.CFN
            AND m.Operation = r.Operation
            AND m.[Group] = r.[Group]
        WHERE m.CFN IS NOT NULL AND m.CFN != ''
            AND m.Operation IS NOT NULL AND m.Operation != ''
            AND m.[Group] IS NOT NULL AND m.[Group] != ''
    """)
    
    direct_match = cur.fetchone()[0]
    print(f"\n直接 JOIN 匹配数: {direct_match:,}")
    
    # 5. 检查视图中的 CTE 逻辑
    print("\n" + "=" * 80)
    print("检查视图 CTE 中的去重逻辑")
    print("=" * 80)
    
    cur.execute("""
        WITH sap_routing_dedup AS (
            SELECT *
            FROM (
                SELECT
                    r.*, 
                    ROW_NUMBER() OVER (
                        PARTITION BY r.CFN, LTRIM(RTRIM(r.Operation)), r.[Group]
                        ORDER BY ISNULL(r.updated_at, r.created_at) DESC
                    ) AS rn
                FROM dbo.raw_sap_routing r
                WHERE r.CFN IS NOT NULL AND r.CFN != ''
                    AND r.Operation IS NOT NULL AND r.Operation != ''
                    AND r.[Group] IS NOT NULL AND r.[Group] != ''
            ) t
            WHERE rn = 1
        )
        SELECT COUNT(*) FROM sap_routing_dedup
    """)
    
    dedup_count = cur.fetchone()[0]
    print(f"\n去重后的 Routing 记录数: {dedup_count:,}")
    
    # 6. 测试 CTE 去重后的匹配
    print("\n测试 CTE 去重后的匹配...")
    cur.execute("""
        WITH sap_routing_dedup AS (
            SELECT *
            FROM (
                SELECT
                    r.CFN, r.ProductNumber, r.Operation, r.[Group], r.StandardTime,
                    ROW_NUMBER() OVER (
                        PARTITION BY r.CFN, LTRIM(RTRIM(r.Operation)), r.[Group]
                        ORDER BY ISNULL(r.updated_at, r.created_at) DESC
                    ) AS rn
                FROM dbo.raw_sap_routing r
                WHERE r.CFN IS NOT NULL AND r.CFN != ''
                    AND r.Operation IS NOT NULL AND r.Operation != ''
                    AND r.[Group] IS NOT NULL AND r.[Group] != ''
            ) t
            WHERE rn = 1
        )
        SELECT COUNT(*) as match_count
        FROM dbo.raw_mes m
        INNER JOIN sap_routing_dedup r
            ON m.CFN = r.CFN
            AND m.Operation = r.Operation
            AND m.[Group] = r.[Group]
    """)
    
    cte_match = cur.fetchone()[0]
    print(f"CTE 去重后匹配数: {cte_match:,}")
    
    # 7. 查看匹配样本
    if cte_match > 0:
        print("\n匹配样本（前3条）:")
        cur.execute("""
            WITH sap_routing_dedup AS (
                SELECT *
                FROM (
                    SELECT
                        r.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY r.CFN, LTRIM(RTRIM(r.Operation)), r.[Group]
                            ORDER BY ISNULL(r.updated_at, r.created_at) DESC
                        ) AS rn
                    FROM dbo.raw_sap_routing r
                    WHERE r.CFN IS NOT NULL AND r.CFN != ''
                        AND r.Operation IS NOT NULL AND r.Operation != ''
                        AND r.[Group] IS NOT NULL AND r.[Group] != ''
                ) t
                WHERE rn = 1
            )
            SELECT TOP 3
                m.CFN, m.Operation, m.[Group], r.StandardTime
            FROM dbo.raw_mes m
            INNER JOIN sap_routing_dedup r
                ON m.CFN = r.CFN
                AND m.Operation = r.Operation
                AND m.[Group] = r.[Group]
        """)
        
        for row in cur.fetchall():
            print(f"  CFN={row[0]}, Op={row[1]}, Group={row[2]}, ST={row[3]}")
    
    print("\n" + "=" * 80)
