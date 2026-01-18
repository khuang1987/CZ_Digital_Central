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
    print("MES 数据完整性检查")
    print("=" * 80)
    
    # 1. 总记录数
    cur.execute("SELECT COUNT(*) FROM dbo.raw_mes")
    total = cur.fetchone()[0]
    print(f"\n总记录数: {total:,}")
    
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
    
    print("\n" + "=" * 80)
    print("问题 1: 批次号缺失检查")
    print("=" * 80)
    
    # 检查 BatchNumber 为 NULL 或空字符串的记录
    cur.execute("""
        SELECT COUNT(*) as missing_count
        FROM dbo.raw_mes
        WHERE BatchNumber IS NULL OR BatchNumber = ''
    """)
    missing_batch = cur.fetchone()[0]
    print(f"\nBatchNumber 缺失的记录数: {missing_batch:,} ({missing_batch/total*100:.2f}%)")
    
    if missing_batch > 0:
        # 查看缺失批次号的样本数据
        cur.execute("""
            SELECT TOP 10 
                Operation, Machine, ProductNumber, CFN, 
                TrackOutTime, factory_name
            FROM dbo.raw_mes
            WHERE BatchNumber IS NULL OR BatchNumber = ''
        """)
        print("\n缺失批次号的样本数据:")
        for row in cur.fetchall():
            print(f"  Operation={row[0]}, Machine={row[1]}, Product={row[2]}, CFN={row[3]}, Time={row[4]}, Factory={row[5]}")
    
    print("\n" + "=" * 80)
    print("问题 2: VSM 字段缺失检查")
    print("=" * 80)
    
    # 检查 VSM 字段
    cur.execute("""
        SELECT COUNT(*) as missing_vsm
        FROM dbo.raw_mes
        WHERE VSM IS NULL OR VSM = ''
    """)
    missing_vsm = cur.fetchone()[0]
    print(f"\nVSM 缺失的记录数: {missing_vsm:,} ({missing_vsm/total*100:.2f}%)")
    
    # 检查 VSM 字段是否存在
    cur.execute("""
        SELECT TOP 5 VSM
        FROM dbo.raw_mes
        WHERE VSM IS NOT NULL AND VSM != ''
    """)
    vsm_samples = cur.fetchall()
    if vsm_samples:
        print("\nVSM 有值的样本:")
        for row in vsm_samples:
            print(f"  {row[0]}")
    else:
        print("\n⚠️ 所有记录的 VSM 字段都为空！")
    
    print("\n" + "=" * 80)
    print("问题 3: created_at 和 updated_at 时间戳检查")
    print("=" * 80)
    
    # 检查 created_at
    cur.execute("""
        SELECT COUNT(*) as missing_created
        FROM dbo.raw_mes
        WHERE created_at IS NULL
    """)
    missing_created = cur.fetchone()[0]
    print(f"\ncreated_at 缺失的记录数: {missing_created:,} ({missing_created/total*100:.2f}%)")
    
    # 检查 updated_at
    cur.execute("""
        SELECT COUNT(*) as missing_updated
        FROM dbo.raw_mes
        WHERE updated_at IS NULL
    """)
    missing_updated = cur.fetchone()[0]
    print(f"updated_at 缺失的记录数: {missing_updated:,} ({missing_updated/total*100:.2f}%)")
    
    # 查看有时间戳的样本
    cur.execute("""
        SELECT TOP 5 created_at, updated_at
        FROM dbo.raw_mes
        WHERE created_at IS NOT NULL OR updated_at IS NOT NULL
    """)
    timestamp_samples = cur.fetchall()
    if timestamp_samples:
        print("\n有时间戳的样本:")
        for row in timestamp_samples:
            print(f"  created_at={row[0]}, updated_at={row[1]}")
    else:
        print("\n⚠️ 所有记录的 created_at 和 updated_at 都为空！")
    
    print("\n" + "=" * 80)
    print("其他字段完整性检查")
    print("=" * 80)
    
    # 检查关键字段的缺失情况
    key_fields = ['Operation', 'ProductNumber', 'CFN', 'TrackOutTime', 'StepInQuantity', 'TrackOutQuantity']
    for field in key_fields:
        cur.execute(f"""
            SELECT COUNT(*) as missing_count
            FROM dbo.raw_mes
            WHERE {field} IS NULL
        """)
        missing = cur.fetchone()[0]
        print(f"\n{field} 缺失: {missing:,} ({missing/total*100:.2f}%)")
