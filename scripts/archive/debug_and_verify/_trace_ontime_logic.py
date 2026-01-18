"""详细追踪 OnTime 判断逻辑，找出问题"""
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
    
    print("=" * 100)
    print("OnTime 判断逻辑追踪分析")
    print("=" * 100)
    
    # 1. 查看 OnTime 和 Delayed 的样本数据对比
    print("\n" + "=" * 100)
    print("OnTime vs Delayed 样本对比（各取10条）")
    print("=" * 100)
    
    print("\n【OnTime 样本】")
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
            LT_d,
            ST_d,
            CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) as LT_ST_ratio
        FROM dbo.v_mes_metrics
        WHERE CompletionStatus = 'OnTime'
            AND LT_d IS NOT NULL 
            AND ST_d IS NOT NULL
            AND ST_d > 0
        ORDER BY id
    """)
    
    print("\n批次         Op  CFN          Qty  Scrap Setup  LT(d)   ST(d)   LT/ST")
    print("-" * 100)
    for row in cur.fetchall():
        scrap = row[4] if row[4] else 0
        setup_time = row[6] if row[6] else 0
        oee = row[7] if row[7] else 0
        unit = row[8] if row[8] else 0
        ratio = row[11] if row[11] else 0
        print(f"{row[0]:12s} {row[1]:3s} {row[2]:12s} {row[3]:4.0f} {scrap:5.0f} "
              f"{row[5]:3s}  {row[9]:6.2f}  {row[10]:6.2f}  {ratio:5.2f}")
    
    print("\n【Delayed 样本】")
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
            LT_d,
            ST_d,
            CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) as LT_ST_ratio
        FROM dbo.v_mes_metrics
        WHERE CompletionStatus = 'Delayed'
            AND LT_d IS NOT NULL 
            AND ST_d IS NOT NULL
            AND ST_d > 0
        ORDER BY id
    """)
    
    print("\n批次         Op  CFN          Qty  Scrap Setup  LT(d)   ST(d)   LT/ST")
    print("-" * 100)
    for row in cur.fetchall():
        scrap = row[4] if row[4] else 0
        setup_time = row[6] if row[6] else 0
        oee = row[7] if row[7] else 0
        unit = row[8] if row[8] else 0
        ratio = row[11] if row[11] else 0
        print(f"{row[0]:12s} {row[1]:3s} {row[2]:12s} {row[3]:4.0f} {scrap:5.0f} "
              f"{row[5]:3s}  {row[9]:6.2f}  {row[10]:6.2f}  {ratio:5.2f}")
    
    # 2. LT/ST 比率分布分析
    print("\n" + "=" * 100)
    print("LT(d) / ST(d) 比率分布")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            CASE 
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 0.5 THEN '0-0.5'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 1.0 THEN '0.5-1.0'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 2.0 THEN '1.0-2.0'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 5.0 THEN '2.0-5.0'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 10.0 THEN '5.0-10.0'
                ELSE '>10.0'
            END as ratio_range,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
        FROM dbo.v_mes_metrics
        WHERE LT_d IS NOT NULL 
            AND ST_d IS NOT NULL 
            AND ST_d > 0
            AND CompletionStatus IN ('OnTime', 'Delayed')
        GROUP BY 
            CASE 
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 0.5 THEN '0-0.5'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 1.0 THEN '0.5-1.0'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 2.0 THEN '1.0-2.0'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 5.0 THEN '2.0-5.0'
                WHEN CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 10.0 THEN '5.0-10.0'
                ELSE '>10.0'
            END
        ORDER BY ratio_range
    """)
    
    print("\nLT/ST比率    记录数          占比")
    print("-" * 60)
    for row in cur.fetchall():
        print(f"{row[0]:10s}  {row[1]:10,}  {row[2]:8.2f}%")
    
    # 3. ST(d) 值分布分析
    print("\n" + "=" * 100)
    print("ST(d) 值分布分析")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            CASE 
                WHEN ST_d <= 0.1 THEN '0-0.1天'
                WHEN ST_d <= 0.5 THEN '0.1-0.5天'
                WHEN ST_d <= 1.0 THEN '0.5-1.0天'
                WHEN ST_d <= 2.0 THEN '1.0-2.0天'
                WHEN ST_d <= 5.0 THEN '2.0-5.0天'
                ELSE '>5.0天'
            END as st_range,
            COUNT(*) as count,
            AVG(ST_d) as avg_st,
            AVG(LT_d) as avg_lt,
            COUNT(CASE WHEN CompletionStatus = 'OnTime' THEN 1 END) as ontime_count,
            COUNT(CASE WHEN CompletionStatus = 'Delayed' THEN 1 END) as delayed_count
        FROM dbo.v_mes_metrics
        WHERE ST_d IS NOT NULL AND ST_d > 0
        GROUP BY 
            CASE 
                WHEN ST_d <= 0.1 THEN '0-0.1天'
                WHEN ST_d <= 0.5 THEN '0.1-0.5天'
                WHEN ST_d <= 1.0 THEN '0.5-1.0天'
                WHEN ST_d <= 2.0 THEN '1.0-2.0天'
                WHEN ST_d <= 5.0 THEN '2.0-5.0天'
                ELSE '>5.0天'
            END
        ORDER BY st_range
    """)
    
    print("\nST(d)范围    记录数      平均ST(d)  平均LT(d)  OnTime    Delayed")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:10s}  {row[1]:8,}  {row[2]:9.4f}  {row[3]:9.4f}  {row[4]:8,}  {row[5]:8,}")
    
    # 4. 详细计算验证（选取几条记录）
    print("\n" + "=" * 100)
    print("详细计算验证（手动计算 ST(d)）")
    print("=" * 100)
    
    cur.execute("""
        SELECT TOP 5
            BatchNumber,
            Operation,
            TrackOutQuantity,
            ScrapQty,
            IsSetup,
            SetupTime,
            OEE,
            unit_time,
            ST_d,
            LT_d,
            CompletionStatus,
            -- 手动计算 ST(d)
            (
                CASE WHEN IsSetup = 'Yes' AND SetupTime IS NOT NULL AND SetupTime > 0 THEN SetupTime ELSE 0 END +
                ((TrackOutQuantity + ISNULL(ScrapQty, 0)) * unit_time / 3600.0) / ISNULL(OEE, 0.77) +
                0.5
            ) / 24.0 as ST_d_manual
        FROM dbo.v_mes_metrics
        WHERE ST_d IS NOT NULL 
            AND ST_d > 0
            AND unit_time IS NOT NULL
        ORDER BY id
    """)
    
    print("\n批次         Op  Qty  Scrap Setup SetupTime  OEE    unit_time  ST(d)视图  ST(d)手算  LT(d)   Status")
    print("-" * 120)
    for row in cur.fetchall():
        qty = row[2] if row[2] else 0
        scrap = row[3] if row[3] else 0
        setup = row[4]
        setup_time = row[5] if row[5] else 0
        oee = row[6] if row[6] else 0.77
        unit = row[7] if row[7] else 0
        st_view = row[8] if row[8] else 0
        lt = row[9] if row[9] else 0
        status = row[10]
        st_manual = row[11] if row[11] else 0
        
        print(f"{row[0]:12s} {row[1]:3s} {qty:4.0f} {scrap:5.0f} {setup:3s}  {setup_time:8.2f}h  "
              f"{oee:5.2f}  {unit:9.2f}s  {st_view:9.4f}  {st_manual:9.4f}  {lt:6.2f}  {status}")
    
    # 5. 检查 LT(d) 计算
    print("\n" + "=" * 100)
    print("LT(d) 计算验证")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            AVG(LT_d) as avg_lt,
            MIN(LT_d) as min_lt,
            MAX(LT_d) as max_lt,
            AVG(CASE WHEN CompletionStatus = 'OnTime' THEN LT_d END) as avg_lt_ontime,
            AVG(CASE WHEN CompletionStatus = 'Delayed' THEN LT_d END) as avg_lt_delayed
        FROM dbo.v_mes_metrics
        WHERE LT_d IS NOT NULL
    """)
    
    row = cur.fetchone()
    print(f"\nLT(d) 平均值: {row[0]:.4f} 天 ({row[0]*24:.2f} 小时)")
    print(f"LT(d) 范围: {row[1]:.4f} - {row[2]:.4f} 天")
    print(f"OnTime 平均 LT(d): {row[3]:.4f} 天 ({row[3]*24:.2f} 小时)")
    print(f"Delayed 平均 LT(d): {row[4]:.4f} 天 ({row[4]*24:.2f} 小时)")
    
    # 6. 检查 ST(d) 计算
    print("\n" + "=" * 100)
    print("ST(d) 计算验证")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            AVG(ST_d) as avg_st,
            MIN(ST_d) as min_st,
            MAX(ST_d) as max_st,
            AVG(CASE WHEN CompletionStatus = 'OnTime' THEN ST_d END) as avg_st_ontime,
            AVG(CASE WHEN CompletionStatus = 'Delayed' THEN ST_d END) as avg_st_delayed
        FROM dbo.v_mes_metrics
        WHERE ST_d IS NOT NULL AND ST_d > 0
    """)
    
    row = cur.fetchone()
    print(f"\nST(d) 平均值: {row[0]:.4f} 天 ({row[0]*24:.2f} 小时)")
    print(f"ST(d) 范围: {row[1]:.4f} - {row[2]:.4f} 天")
    print(f"OnTime 平均 ST(d): {row[3]:.4f} 天 ({row[3]*24:.2f} 小时)")
    print(f"Delayed 平均 ST(d): {row[4]:.4f} 天 ({row[4]*24:.2f} 小时)")
    
    # 7. 关键发现
    print("\n" + "=" * 100)
    print("关键发现")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            AVG(LT_d) as avg_lt,
            AVG(ST_d) as avg_st,
            AVG(CAST(LT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0)) as avg_ratio
        FROM dbo.v_mes_metrics
        WHERE LT_d IS NOT NULL 
            AND ST_d IS NOT NULL 
            AND ST_d > 0
            AND CompletionStatus IN ('OnTime', 'Delayed')
    """)
    
    row = cur.fetchone()
    print(f"\n有效记录数: {row[0]:,}")
    print(f"平均 LT(d): {row[1]:.4f} 天 ({row[1]*24:.2f} 小时)")
    print(f"平均 ST(d): {row[2]:.4f} 天 ({row[2]*24:.2f} 小时)")
    print(f"平均 LT/ST 比率: {row[3]:.2f}")
    
    if row[3] > 1.5:
        print(f"\n⚠️ 问题发现：平均 LT/ST 比率 = {row[3]:.2f} > 1.5")
        print("   这意味着实际加工时间普遍远超标准时间")
        print("   可能原因：")
        print("   1. ST(d) 计算过小（OEE、SetupTime、换批时间等参数不合理）")
        print("   2. LT(d) 计算过大（时间起点选择不当）")
        print("   3. 标准工时数据本身偏小")
    
    print("\n" + "=" * 100)
