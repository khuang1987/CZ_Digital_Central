"""验证修正后的 ST(d) 计算逻辑"""
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
    print("ST(d) 计算验证（按技术文档定义）")
    print("=" * 80)
    
    # 1. ST(d) 完整性
    print("\n" + "=" * 80)
    print("ST(d) 字段完整性")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN ST_d IS NOT NULL AND ST_d > 0 THEN 1 END) as has_valid_st,
            AVG(CASE WHEN ST_d IS NOT NULL AND ST_d > 0 THEN ST_d END) as avg_st,
            MIN(CASE WHEN ST_d IS NOT NULL AND ST_d > 0 THEN ST_d END) as min_st,
            MAX(CASE WHEN ST_d IS NOT NULL AND ST_d > 0 THEN ST_d END) as max_st
        FROM dbo.v_mes_metrics
    """)
    
    row = cur.fetchone()
    print(f"\n✓ ST_d > 0: {row[1]:,} / {row[0]:,} ({row[1]/row[0]*100:.2f}%)")
    if row[2]:
        print(f"  平均值: {row[2]:.4f} 天 ({row[2]*24:.2f} 小时)")
        print(f"  范围: {row[3]:.4f} - {row[4]:.4f} 天")
    
    # 2. 验证 ST(d) 计算逻辑
    print("\n" + "=" * 80)
    print("ST(d) 计算逻辑验证（样本数据）")
    print("=" * 80)
    
    cur.execute("""
        SELECT TOP 10
            BatchNumber,
            Operation,
            CFN,
            TrackOutQuantity,
            ScrapQty,
            IsSetup,
            SetupTime,
            OEE,
            unit_time,
            ST_d,
            -- 手动计算验证
            ROUND(
                (
                    CASE WHEN IsSetup = 'Yes' AND SetupTime IS NOT NULL AND SetupTime > 0 THEN SetupTime ELSE 0 END +
                    ((TrackOutQuantity + ISNULL(ScrapQty, 0)) * unit_time / 3600.0) / ISNULL(OEE, 0.77) +
                    0.5
                ) / 24.0,
                2
            ) as ST_d_expected
        FROM dbo.v_mes_metrics
        WHERE ST_d IS NOT NULL AND ST_d > 0
            AND TrackOutQuantity > 0
        ORDER BY id
    """)
    
    print("\n批次         Op  CFN           Qty  Scrap Setup  ST(d)实际 ST(d)预期")
    print("-" * 90)
    for row in cur.fetchall():
        print(f"{row[0]:12s} {row[1]:3s} {row[2]:12s} {row[3]:4.0f} {row[4] or 0:5.0f} "
              f"{row[5]:3s}  {row[9]:9.4f} {row[10]:9.4f}")
    
    # 3. 检查 SetupTime 的影响
    print("\n" + "=" * 80)
    print("SetupTime 对 ST(d) 的影响")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            IsSetup,
            COUNT(*) as total,
            COUNT(CASE WHEN ST_d IS NOT NULL THEN 1 END) as has_st,
            AVG(CASE WHEN ST_d IS NOT NULL THEN ST_d END) as avg_st,
            AVG(CASE WHEN ST_d IS NOT NULL THEN SetupTime END) as avg_setup
        FROM dbo.v_mes_metrics
        GROUP BY IsSetup
        ORDER BY IsSetup
    """)
    
    print("\nIsSetup  总数      有ST(d)    平均ST(d)    平均SetupTime")
    print("-" * 70)
    for row in cur.fetchall():
        print(f"{row[0]:7s} {row[1]:8,}  {row[2]:8,}  {row[3]:10.4f}天  {row[4] or 0:10.2f}小时")
    
    # 4. 检查 OEE 的影响
    print("\n" + "=" * 80)
    print("OEE 对 ST(d) 的影响")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            CASE 
                WHEN OEE IS NULL THEN '未匹配'
                WHEN OEE = 0.77 THEN '默认(77%)'
                ELSE '自定义'
            END as oee_type,
            COUNT(*) as total,
            COUNT(CASE WHEN ST_d IS NOT NULL THEN 1 END) as has_st,
            AVG(CASE WHEN ST_d IS NOT NULL THEN ST_d END) as avg_st,
            AVG(OEE) as avg_oee
        FROM dbo.v_mes_metrics
        GROUP BY 
            CASE 
                WHEN OEE IS NULL THEN '未匹配'
                WHEN OEE = 0.77 THEN '默认(77%)'
                ELSE '自定义'
            END
        ORDER BY oee_type
    """)
    
    print("\nOEE类型      总数      有ST(d)    平均ST(d)    平均OEE")
    print("-" * 70)
    for row in cur.fetchall():
        avg_oee = row[4] if row[4] is not None else 0
        print(f"{row[0]:10s} {row[1]:8,}  {row[2]:8,}  {row[3] if row[3] else 0:10.4f}天  {avg_oee:8.2%}")
    
    # 5. 检查报废数量的影响
    print("\n" + "=" * 80)
    print("报废数量对 ST(d) 的影响")
    print("=" * 80)
    
    cur.execute("""
        SELECT 
            CASE 
                WHEN ScrapQty IS NULL OR ScrapQty = 0 THEN '无报废'
                WHEN ScrapQty > 0 AND ScrapQty <= 5 THEN '1-5件'
                WHEN ScrapQty > 5 AND ScrapQty <= 10 THEN '6-10件'
                ELSE '>10件'
            END as scrap_range,
            COUNT(*) as total,
            COUNT(CASE WHEN ST_d IS NOT NULL THEN 1 END) as has_st,
            AVG(CASE WHEN ST_d IS NOT NULL THEN ST_d END) as avg_st,
            AVG(CASE WHEN ScrapQty > 0 THEN ScrapQty END) as avg_scrap
        FROM dbo.v_mes_metrics
        GROUP BY 
            CASE 
                WHEN ScrapQty IS NULL OR ScrapQty = 0 THEN '无报废'
                WHEN ScrapQty > 0 AND ScrapQty <= 5 THEN '1-5件'
                WHEN ScrapQty > 5 AND ScrapQty <= 10 THEN '6-10件'
                ELSE '>10件'
            END
        ORDER BY scrap_range
    """)
    
    print("\n报废范围    总数      有ST(d)    平均ST(d)    平均报废")
    print("-" * 70)
    for row in cur.fetchall():
        print(f"{row[0]:8s} {row[1]:8,}  {row[2]:8,}  {row[3]:10.4f}天  {row[4] or 0:8.1f}件")
    
    # 6. CompletionStatus 分布
    print("\n" + "=" * 80)
    print("CompletionStatus 分布（修正后）")
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
    
    print("\n" + "=" * 80)
    print("✓ ST(d) 计算验证完成")
    print("=" * 80)
