"""直接查询视图的实际列名"""
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
    
    # 直接查询一条记录，获取列名
    cur.execute("SELECT TOP 1 * FROM dbo.v_mes_metrics")
    
    columns = [column[0] for column in cur.description]
    
    print("=" * 100)
    print("视图实际列名（共 {} 列）".format(len(columns)))
    print("=" * 100)
    
    for i, col in enumerate(columns, 1):
        print(f"{i:2d}. {col}")
    
    print("\n" + "=" * 100)
    print("检查关键字段")
    print("=" * 100)
    
    m_code_fields = {
        'LT(d)': 'LT_d',
        'PT(d)': 'PT_d', 
        'ST(d)': 'ST_d',
        'Checkin_SFC': 'TrackIn_SFC',
        'Setup Time (h)': 'SetupTime',
        'Operation description': 'OperationDesc',
        'machine': 'Machine',
        'Setup': 'IsSetup'
    }
    
    print("\nM代码期望 -> SQL实际")
    print("-" * 60)
    
    for m_name, sql_name in m_code_fields.items():
        if m_name in columns:
            print(f"✓ {m_name:25s} -> 已存在")
        elif sql_name in columns:
            print(f"✗ {m_name:25s} -> {sql_name} (需要修改)")
        else:
            print(f"✗ {m_name:25s} -> 不存在")
