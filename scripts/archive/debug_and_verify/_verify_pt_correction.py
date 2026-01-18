"""验证修正后的 PT(d) 计算逻辑和 SA 达成率"""
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
    print("PT(d) 修正后的验证（EffectiveStartTime 逻辑）")
    print("=" * 100)
    
    # 1. CompletionStatus 分布
    print("\n" + "=" * 100)
    print("CompletionStatus 分布（PT(d) 修正后）")
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
    
    # 2. 连续生产 vs 非连续生产统计
    print("\n" + "=" * 100)
    print("连续生产 vs 非连续生产统计")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            CASE 
                WHEN EnterStepTime IS NOT NULL AND EnterStepTime > PreviousBatchEndTime THEN '非连续生产'
                WHEN PreviousBatchEndTime IS NOT NULL THEN '连续生产'
                ELSE '无基准'
            END as production_type,
            COUNT(*) as total,
            AVG(PT_d) as avg_pt,
            AVG(LT_d) as avg_lt,
            AVG(ST_d) as avg_st
        FROM dbo.v_mes_metrics
        WHERE PT_d IS NOT NULL
        GROUP BY 
            CASE 
                WHEN EnterStepTime IS NOT NULL AND EnterStepTime > PreviousBatchEndTime THEN '非连续生产'
                WHEN PreviousBatchEndTime IS NOT NULL THEN '连续生产'
                ELSE '无基准'
            END
        ORDER BY production_type
    """)
    
    print("\n生产类型        记录数      平均PT(d)  平均LT(d)  平均ST(d)")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:12s}  {row[1]:10,}  {row[2]:9.4f}  {row[3]:9.4f}  {row[4]:9.4f}")
    
    # 3. PT(d) 样本对比
    print("\n" + "=" * 100)
    print("PT(d) 计算样本（连续 vs 非连续生产）")
    print("=" * 100)
    
    print("\n【连续生产样本】（EnterStepTime <= PreviousBatchEndTime）")
    cur.execute("""
        SELECT TOP 5
            BatchNumber,
            Operation,
            EnterStepTime,
            TrackInTime,
            PreviousBatchEndTime,
            TrackOutTime,
            PT_d,
            LT_d,
            CompletionStatus
        FROM dbo.v_mes_metrics
        WHERE PT_d IS NOT NULL
            AND EnterStepTime IS NOT NULL
            AND PreviousBatchEndTime IS NOT NULL
            AND EnterStepTime <= PreviousBatchEndTime
        ORDER BY id
    """)
    
    print("\n批次         Op  EnterStep           TrackIn             PrevBatchEnd        TrackOut            PT(d)   LT(d)   Status")
    print("-" * 140)
    for row in cur.fetchall():
        enter = row[2].strftime('%m-%d %H:%M') if row[2] else 'NULL'
        track_in = row[3].strftime('%m-%d %H:%M') if row[3] else 'NULL'
        prev = row[4].strftime('%m-%d %H:%M') if row[4] else 'NULL'
        track_out = row[5].strftime('%m-%d %H:%M') if row[5] else 'NULL'
        print(f"{row[0]:12s} {row[1]:3s} {enter:15s} {track_in:15s} {prev:15s} {track_out:15s} {row[6]:6.2f}  {row[7]:6.2f}  {row[8]}")
    
    print("\n【非连续生产样本】（EnterStepTime > PreviousBatchEndTime）")
    cur.execute("""
        SELECT TOP 5
            BatchNumber,
            Operation,
            EnterStepTime,
            TrackInTime,
            PreviousBatchEndTime,
            TrackOutTime,
            PT_d,
            LT_d,
            CompletionStatus
        FROM dbo.v_mes_metrics
        WHERE PT_d IS NOT NULL
            AND EnterStepTime IS NOT NULL
            AND PreviousBatchEndTime IS NOT NULL
            AND EnterStepTime > PreviousBatchEndTime
        ORDER BY id
    """)
    
    print("\n批次         Op  EnterStep           TrackIn             PrevBatchEnd        TrackOut            PT(d)   LT(d)   Status")
    print("-" * 140)
    for row in cur.fetchall():
        enter = row[2].strftime('%m-%d %H:%M') if row[2] else 'NULL'
        track_in = row[3].strftime('%m-%d %H:%M') if row[3] else 'NULL'
        prev = row[4].strftime('%m-%d %H:%M') if row[4] else 'NULL'
        track_out = row[5].strftime('%m-%d %H:%M') if row[5] else 'NULL'
        print(f"{row[0]:12s} {row[1]:3s} {enter:15s} {track_in:15s} {prev:15s} {track_out:15s} {row[6]:6.2f}  {row[7]:6.2f}  {row[8]}")
    
    # 4. SA 达成率统计
    print("\n" + "=" * 100)
    print("SA 达成率统计（PT(d) 修正后）")
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
    
    # 5. 按工厂统计
    print("\n" + "=" * 100)
    print("按工厂统计 SA 达成率")
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
    
    # 6. PT/ST 比率分布
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
                ELSE '>2.0'
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
                ELSE '>2.0'
            END
        ORDER BY ratio_range
    """)
    
    print("\nPT/ST比率    记录数          占比        OnTime      Overdue")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:10s}  {row[1]:10,}  {row[2]:8.2f}%  {row[3]:10,}  {row[4]:10,}")
    
    # 7. 修正效果总结
    print("\n" + "=" * 100)
    print("修正效果总结")
    print("=" * 100)
    
    print("\n【修正前】（PT(d) 固定使用 PreviousBatchEndTime）")
    print("  - SA 达成率: 83.92%")
    print("  - 平均 PT/ST 比率: 2.99")
    print("  - 问题: 非连续生产时包含了停机等待时间")
    
    print("\n【修正后】（PT(d) 使用 EffectiveStartTime 逻辑）")
    print(f"  - SA 达成率: {ontime/valid*100:.2f}%")
    print(f"  - 平均 PT/ST 比率: {avg_ratio:.2f}")
    print(f"  - 改进: 非连续生产时使用 TrackInTime，排除停机等待")
    
    if avg_ratio < 2.5:
        print("\n✅ PT/ST 比率已优化，更接近实际加工时间！")
    
    print("\n" + "=" * 100)
