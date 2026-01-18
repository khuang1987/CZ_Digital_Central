"""检查 SetupTime 和 unit_time 数据异常"""
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
    print("SetupTime 和 unit_time 数据异常检查")
    print("=" * 100)
    
    # 1. SetupTime 分布
    print("\n" + "=" * 100)
    print("SetupTime 分布（从 raw_sap_routing）")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(CASE WHEN SetupTime IS NOT NULL THEN 1 END) as has_setup,
            AVG(SetupTime) as avg_setup,
            MIN(SetupTime) as min_setup,
            MAX(SetupTime) as max_setup,
            COUNT(CASE WHEN SetupTime > 100 THEN 1 END) as over_100h,
            COUNT(CASE WHEN SetupTime > 500 THEN 1 END) as over_500h
        FROM dbo.raw_sap_routing
    """)
    
    row = cur.fetchone()
    print(f"\n总记录数: {row[0]:,}")
    print(f"有 SetupTime: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
    print(f"平均 SetupTime: {row[2]:.2f} 小时")
    print(f"范围: {row[3]:.2f} - {row[4]:.2f} 小时")
    print(f"⚠️ SetupTime > 100h: {row[5]:,} ({row[5]/row[0]*100:.2f}%)")
    print(f"⚠️ SetupTime > 500h: {row[6]:,} ({row[6]/row[0]*100:.2f}%)")
    
    # 2. 查看异常 SetupTime 样本
    print("\n" + "=" * 100)
    print("异常 SetupTime 样本（> 100 小时）")
    print("=" * 100)
    
    cur.execute("""
        SELECT TOP 20
            CFN,
            Operation,
            [Group],
            SetupTime,
            StandardTime,
            EH_machine,
            EH_labor,
            OEE,
            factory_code
        FROM dbo.raw_sap_routing
        WHERE SetupTime > 100
        ORDER BY SetupTime DESC
    """)
    
    print("\nCFN              Op    Group     SetupTime  StandardTime  EH_machine  EH_labor  OEE    Factory")
    print("-" * 110)
    for row in cur.fetchall():
        print(f"{row[0]:15s} {row[1]:5s} {row[2]:8s}  {row[3]:9.2f}h  {row[4]:12.2f}  "
              f"{row[5]:10.2f}  {row[6]:8.2f}  {row[7]:5.2f}  {row[8]}")
    
    # 3. unit_time 分布
    print("\n" + "=" * 100)
    print("unit_time 分布（从 raw_sap_routing）")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            AVG(CASE WHEN EH_machine > 0 THEN EH_machine ELSE EH_labor END) as avg_unit_time,
            MIN(CASE WHEN EH_machine > 0 THEN EH_machine ELSE EH_labor END) as min_unit_time,
            MAX(CASE WHEN EH_machine > 0 THEN EH_machine ELSE EH_labor END) as max_unit_time,
            COUNT(CASE WHEN (CASE WHEN EH_machine > 0 THEN EH_machine ELSE EH_labor END) > 3600 THEN 1 END) as over_1h
        FROM dbo.raw_sap_routing
        WHERE EH_machine > 0 OR EH_labor > 0
    """)
    
    row = cur.fetchone()
    print(f"\n总记录数: {row[0]:,}")
    print(f"平均 unit_time: {row[1]:.2f} 秒 ({row[1]/60:.2f} 分钟)")
    print(f"范围: {row[2]:.2f} - {row[3]:.2f} 秒")
    print(f"⚠️ unit_time > 3600s (1小时): {row[4]:,} ({row[4]/row[0]*100:.2f}%)")
    
    # 4. 查看异常 unit_time 样本
    print("\n" + "=" * 100)
    print("异常 unit_time 样本（> 3600 秒）")
    print("=" * 100)
    
    cur.execute("""
        SELECT TOP 20
            CFN,
            Operation,
            [Group],
            EH_machine,
            EH_labor,
            CASE WHEN EH_machine > 0 THEN EH_machine ELSE EH_labor END as unit_time,
            SetupTime,
            OEE,
            factory_code
        FROM dbo.raw_sap_routing
        WHERE (CASE WHEN EH_machine > 0 THEN EH_machine ELSE EH_labor END) > 3600
        ORDER BY (CASE WHEN EH_machine > 0 THEN EH_machine ELSE EH_labor END) DESC
    """)
    
    print("\nCFN              Op    Group     EH_machine  EH_labor  unit_time  SetupTime  OEE    Factory")
    print("-" * 110)
    for row in cur.fetchall():
        setup = row[6] if row[6] is not None else 0
        print(f"{row[0]:15s} {row[1]:5s} {row[2]:8s}  {row[3]:10.2f}  {row[4]:8.2f}  "
              f"{row[5]:9.2f}s  {setup:9.2f}h  {row[7]:5.2f}  {row[8]}")
    
    # 5. 检查视图中的 SetupTime 使用情况
    print("\n" + "=" * 100)
    print("视图中 SetupTime 的影响")
    print("=" * 100)
    
    cur.execute("""
        SELECT 
            IsSetup,
            COUNT(*) as total,
            AVG(SetupTime) as avg_setup,
            AVG(ST_d) as avg_st_d,
            COUNT(CASE WHEN SetupTime > 100 THEN 1 END) as over_100h
        FROM dbo.v_mes_metrics
        WHERE ST_d IS NOT NULL
        GROUP BY IsSetup
        ORDER BY IsSetup
    """)
    
    print("\nIsSetup  总记录数      平均SetupTime  平均ST(d)    SetupTime>100h")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:7s}  {row[1]:10,}  {row[2]:13.2f}h  {row[3]:11.4f}天  {row[4]:14,}")
    
    # 6. 技术文档中的定义
    print("\n" + "=" * 100)
    print("技术文档定义 vs 实际数据")
    print("=" * 100)
    
    print("\n【技术文档定义】")
    print("- SetupTime: 调试时间（小时），应该是合理的换型调试时间")
    print("- unit_time: 单件工时（秒），应该是单个产品的加工时间")
    print("- ST(d) = (SetupTime + (Qty × unit_time / 3600 / OEE) + 0.5) / 24")
    
    print("\n【实际数据问题】")
    print("- SetupTime 最大值达到数百小时，不合理")
    print("- unit_time 最大值超过 1 小时（3600秒），可能是总工时而非单件工时")
    
    print("\n" + "=" * 100)
    print("建议")
    print("=" * 100)
    print("\n1. SetupTime 可能单位错误：")
    print("   - 如果原始数据单位是分钟，需要除以 60")
    print("   - 如果原始数据单位是秒，需要除以 3600")
    print("   - 建议检查 SAP Routing 原始 Excel 文件")
    
    print("\n2. unit_time 可能是总工时而非单件工时：")
    print("   - 需要确认 EH_machine/EH_labor 的单位和含义")
    print("   - 如果是总工时，需要除以数量")
    
    print("\n3. 建议添加合理性检查：")
    print("   - SetupTime 上限：例如 < 24 小时")
    print("   - unit_time 上限：例如 < 3600 秒（1小时）")
    
    print("\n" + "=" * 100)
