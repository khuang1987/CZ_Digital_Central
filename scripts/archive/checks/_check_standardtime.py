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
    print("检查 raw_sap_routing 表的 StandardTime 字段")
    print("=" * 80)
    
    # 1. 检查 StandardTime 字段的完整性
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(StandardTime) as has_st,
            COUNT(CASE WHEN StandardTime IS NOT NULL AND StandardTime > 0 THEN 1 END) as has_valid_st
        FROM dbo.raw_sap_routing
    """)
    
    row = cur.fetchone()
    print(f"\nRouting 表总记录数: {row[0]:,}")
    print(f"StandardTime 非空记录: {row[1]:,} ({row[1]/row[0]*100:.2f}%)")
    print(f"StandardTime > 0 记录: {row[2]:,} ({row[2]/row[0]*100:.2f}%)")
    
    # 2. 检查表结构
    print("\n" + "=" * 80)
    print("raw_sap_routing 表结构")
    print("=" * 80)
    
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'raw_sap_routing' AND TABLE_SCHEMA = 'dbo'
        ORDER BY ORDINAL_POSITION
    """)
    
    print("\n列名                          数据类型        可空")
    print("-" * 70)
    for row in cur.fetchall():
        print(f"{row[0]:30s} {row[1]:15s} {row[2]}")
    
    # 3. 查看样本数据（所有字段）
    print("\n" + "=" * 80)
    print("样本数据（前3条）")
    print("=" * 80)
    
    cur.execute("""
        SELECT TOP 3 *
        FROM dbo.raw_sap_routing
        WHERE CFN IS NOT NULL
    """)
    
    # 获取列名
    columns = [desc[0] for desc in cur.description]
    print(f"\n列名: {columns}")
    
    for i, row in enumerate(cur.fetchall(), 1):
        print(f"\n记录 {i}:")
        for col, val in zip(columns, row):
            if val is not None:
                print(f"  {col}: {val}")
    
    # 4. 检查是否有其他时间相关字段
    print("\n" + "=" * 80)
    print("查找可能的时间字段")
    print("=" * 80)
    
    cur.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'raw_sap_routing' 
            AND TABLE_SCHEMA = 'dbo'
            AND (COLUMN_NAME LIKE '%time%' 
                OR COLUMN_NAME LIKE '%Time%'
                OR COLUMN_NAME LIKE '%duration%'
                OR COLUMN_NAME LIKE '%Duration%'
                OR COLUMN_NAME LIKE '%std%'
                OR COLUMN_NAME LIKE '%standard%')
        ORDER BY ORDINAL_POSITION
    """)
    
    time_cols = [row[0] for row in cur.fetchall()]
    if time_cols:
        print(f"\n找到可能的时间字段: {time_cols}")
        
        # 检查这些字段的数据
        for col in time_cols:
            cur.execute(f"""
                SELECT COUNT(CASE WHEN [{col}] IS NOT NULL THEN 1 END) as has_value
                FROM dbo.raw_sap_routing
            """)
            count = cur.fetchone()[0]
            print(f"  {col}: {count:,} 条非空记录")
    else:
        print("\n未找到时间相关字段")
    
    print("\n" + "=" * 80)
