"""验证修正后的 OnTime 判断逻辑（使用 PT(d) + 8小时容差）"""
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
    print("修正后的 OnTime 判断验证（PT(d) + 8小时容差）")
    print("=" * 100)
    
    # 1. CompletionStatus 分布
    print("\n" + "=" * 100)
    print("CompletionStatus 分布（修正后）")
    print("=" * 100)
    
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
    
    # 2. OnTime vs Overdue 样本对比
    print("\n" + "=" * 100)
    print("OnTime vs Overdue 样本对比")
    print("=" * 100)
    
    print("\n【OnTime 样本】（PT(d) <= ST(d) + 0.33）")
    cur.execute("""
        SELECT TOP 10
            BatchNumber,
            Operation,
            CFN,
            TrackOutQuantity,
            PT_d,
            ST_d,
            CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) as PT_ST_ratio,
            ST_d + 0.33 as ST_with_tolerance
        FROM dbo.v_mes_metrics
        WHERE CompletionStatus = 'OnTime'
            AND PT_d IS NOT NULL 
            AND ST_d IS NOT NULL
            AND ST_d > 0
        ORDER BY id
    """)
    
    print("\n批次         Op  CFN          Qty    PT(d)   ST(d)   容差后    PT/ST")
    print("-" * 90)
    for row in cur.fetchall():
        print(f"{row[0]:12s} {row[1]:3s} {row[2]:12s} {row[3]:4.0f}  {row[4]:6.2f}  {row[5]:6.2f}  {row[7]:6.2f}  {row[6]:5.2f}")
    
    print("\n【Overdue 样本】（PT(d) > ST(d) + 0.33）")
    cur.execute("""
        SELECT TOP 10
            BatchNumber,
            Operation,
            CFN,
            TrackOutQuantity,
            PT_d,
            ST_d,
            CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) as PT_ST_ratio,
            ST_d + 0.33 as ST_with_tolerance
        FROM dbo.v_mes_metrics
        WHERE CompletionStatus = 'Overdue'
            AND PT_d IS NOT NULL 
            AND ST_d IS NOT NULL
            AND ST_d > 0
        ORDER BY id
    """)
    
    print("\n批次         Op  CFN          Qty    PT(d)   ST(d)   容差后    PT/ST")
    print("-" * 90)
    for row in cur.fetchall():
        print(f"{row[0]:12s} {row[1]:3s} {row[2]:12s} {row[3]:4.0f}  {row[4]:6.2f}  {row[5]:6.2f}  {row[7]:6.2f}  {row[6]:5.2f}")
    
    # 3. PT/ST 比率分布
    print("\n" + "=" * 100)
    print("PT(d) / ST(d) 比率分布")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            CASE 
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 0.5 THEN '0-0.5'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 1.0 THEN '0.5-1.0'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 1.5 THEN '1.0-1.5'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 2.0 THEN '1.5-2.0'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 5.0 THEN '2.0-5.0'
                ELSE '>5.0'
            END as ratio_range,
            COUNT(*) as count,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage,
            COUNT(CASE WHEN CompletionStatus = 'OnTime' THEN 1 END) as ontime_count,
            COUNT(CASE WHEN CompletionStatus = 'Overdue' THEN 1 END) as overdue_count
        FROM dbo.v_mes_metrics
        WHERE PT_d IS NOT NULL 
            AND ST_d IS NOT NULL 
            AND ST_d > 0
            AND CompletionStatus IN ('OnTime', 'Overdue')
        GROUP BY 
            CASE 
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 0.5 THEN '0-0.5'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 1.0 THEN '0.5-1.0'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 1.5 THEN '1.0-1.5'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 2.0 THEN '1.5-2.0'
                WHEN CAST(PT_d AS FLOAT) / NULLIF(CAST(ST_d AS FLOAT), 0) <= 5.0 THEN '2.0-5.0'
                ELSE '>5.0'
            END
        ORDER BY ratio_range
    """)
    
    print("\nPT/ST比率    记录数          占比        OnTime      Overdue")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:10s}  {row[1]:10,}  {row[2]:8.2f}%  {row[3]:10,}  {row[4]:10,}")
    
    # 4. 按工厂统计
    print("\n" + "=" * 100)
    print("按工厂统计 SA (Schedule Attainment)")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            factory_name,
            COUNT(*) as total,
            COUNT(CASE WHEN CompletionStatus = 'OnTime' THEN 1 END) as ontime_count,
            COUNT(CASE WHEN CompletionStatus = 'Overdue' THEN 1 END) as overdue_count,
            COUNT(CASE WHEN CompletionStatus = 'OnTime' THEN 1 END) * 100.0 / 
                NULLIF(COUNT(CASE WHEN CompletionStatus IN ('OnTime', 'Overdue') THEN 1 END), 0) as SA_rate
        FROM dbo.v_mes_metrics
        GROUP BY factory_name
        ORDER BY factory_name
    """)
    
    print("\n工厂            总记录数      OnTime      Overdue      SA达成率")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:10,}  {row[2]:10,}  {row[3]:10,}  {row[4]:8.2f}%")
    
    # 5. 关键指标对比
    print("\n" + "=" * 100)
    print("关键指标对比（修正前后）")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN CompletionStatus IN ('OnTime', 'Overdue') THEN 1 END) as valid_records,
            COUNT(CASE WHEN CompletionStatus = 'OnTime' THEN 1 END) as ontime_count,
            COUNT(CASE WHEN CompletionStatus = 'Overdue' THEN 1 END) as overdue_count,
            AVG(CASE WHEN PT_d IS NOT NULL AND ST_d IS NOT NULL AND ST_d > 0 
                THEN CAST(PT_d AS FLOAT) / CAST(ST_d AS FLOAT) END) as avg_pt_st_ratio
        FROM dbo.v_mes_metrics
    """)
    
    row = cur.fetchone()
    total = row[0]
    valid = row[1]
    ontime = row[2]
    overdue = row[3]
    avg_ratio = row[4]
    
    print(f"\n总记录数: {total:,}")
    print(f"有效记录数（OnTime + Overdue）: {valid:,} ({valid/total*100:.2f}%)")
    print(f"OnTime 记录数: {ontime:,} ({ontime/total*100:.2f}%)")
    print(f"Overdue 记录数: {overdue:,} ({overdue/total*100:.2f}%)")
    print(f"\n✓ SA 达成率 (Schedule Attainment): {ontime/valid*100:.2f}%")
    print(f"✓ 平均 PT/ST 比率: {avg_ratio:.2f}")
    
    # 6. 修正效果对比
    print("\n" + "=" * 100)
    print("修正效果总结")
    print("=" * 100)
    
    print("\n【修正前】（使用 LT(d)，无容差）")
    print("  - OnTime 率: ~11.67%")
    print("  - 平均 LT/ST 比率: 34.11")
    print("  - 问题: 使用了错误的时间基准（LT 包含等待时间）")
    
    print("\n【修正后】（使用 PT(d) + 8小时容差）")
    print(f"  - OnTime 率 (SA): {ontime/valid*100:.2f}%")
    print(f"  - 平均 PT/ST 比率: {avg_ratio:.2f}")
    print(f"  - 改进: 使用正确的时间基准（PT 排除等待时间）+ 合理容差")
    
    if ontime/valid*100 > 50:
        print("\n✅ SA 达成率已恢复到合理水平！")
    elif ontime/valid*100 > 30:
        print("\n⚠️ SA 达成率有所改善，但仍需优化")
    else:
        print("\n⚠️ SA 达成率仍然偏低，需要进一步检查数据质量")
    
    print("\n" + "=" * 100)
