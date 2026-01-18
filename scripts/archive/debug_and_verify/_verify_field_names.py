"""验证 SQL Server 视图字段名是否与 M 代码一致"""
import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

print("=" * 100)
print("验证 v_mes_metrics 视图字段名")
print("=" * 100)

with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    # 获取视图的所有列名
    cur.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'v_mes_metrics'
        ORDER BY ORDINAL_POSITION
    """)
    
    columns = cur.fetchall()
    
    print(f"\n视图列数: {len(columns)}")
    print("\n列名列表:")
    print("-" * 100)
    
    # M 代码期望的字段名
    m_code_fields = {
        'LT(d)', 'PT(d)', 'ST(d)', 
        'Checkin_SFC', 'Setup Time (h)', 
        'Operation description', 'machine', 'Setup'
    }
    
    found_fields = set()
    
    for col in columns:
        col_name = col[0]
        data_type = col[1]
        
        # 检查是否是 M 代码期望的字段
        if col_name in m_code_fields:
            print(f"✓ {col_name:30s} ({data_type:15s}) - M 代码字段")
            found_fields.add(col_name)
        else:
            print(f"  {col_name:30s} ({data_type:15s})")
    
    # 检查是否所有 M 代码字段都存在
    print("\n" + "=" * 100)
    print("M 代码字段检查")
    print("=" * 100)
    
    missing_fields = m_code_fields - found_fields
    
    if missing_fields:
        print(f"\n❌ 缺失的 M 代码字段: {missing_fields}")
    else:
        print(f"\n✅ 所有 M 代码字段都已存在！")
    
    # 查询示例数据
    print("\n" + "=" * 100)
    print("示例数据（前 5 条）")
    print("=" * 100)
    
    cur.execute("""
        SELECT TOP 5
            BatchNumber,
            Operation,
            machine,
            [Operation description],
            Setup,
            [Setup Time (h)],
            [LT(d)],
            [PT(d)],
            [ST(d)],
            Checkin_SFC,
            CompletionStatus
        FROM dbo.v_mes_metrics
        WHERE [PT(d)] IS NOT NULL
        ORDER BY id
    """)
    
    print("\nBatchNumber  Op   machine  OpDesc          Setup  SetupTime  LT(d)  PT(d)  ST(d)  Status")
    print("-" * 100)
    
    for row in cur.fetchall():
        batch = row[0][:12] if row[0] else 'NULL'
        op = row[1][:4] if row[1] else 'NULL'
        machine = row[2][:8] if row[2] else 'NULL'
        op_desc = row[3][:15] if row[3] else 'NULL'
        setup = row[4][:3] if row[4] else 'NULL'
        setup_time = f"{row[5]:.2f}" if row[5] is not None else 'NULL'
        lt = f"{row[6]:.2f}" if row[6] is not None else 'NULL'
        pt = f"{row[7]:.2f}" if row[7] is not None else 'NULL'
        st = f"{row[8]:.2f}" if row[8] is not None else 'NULL'
        status = row[10][:10] if row[10] else 'NULL'
        
        print(f"{batch:12s} {op:4s} {machine:8s} {op_desc:15s} {setup:3s}    {setup_time:6s}   {lt:5s}  {pt:5s}  {st:5s}  {status}")

print("\n" + "=" * 100)
print("字段名验证完成")
print("=" * 100)
