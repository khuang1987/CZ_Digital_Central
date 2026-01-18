"""检查视图的实际定义"""
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
    
    # 获取视图定义
    cur.execute("""
        SELECT OBJECT_DEFINITION(OBJECT_ID('dbo.v_mes_metrics'))
    """)
    
    view_def = cur.fetchone()[0]
    
    print("视图定义:")
    print("=" * 100)
    
    # 只显示 SELECT 部分（从第二个 SELECT 开始）
    lines = view_def.split('\n')
    in_final_select = False
    
    for i, line in enumerate(lines):
        if 'FROM mes_with_prev' in line:
            break
        if in_final_select:
            print(f"{i:3d}: {line}")
        if line.strip().startswith('SELECT') and 'mes_with_prev' not in lines[max(0, i-5):i+5]:
            if any('FROM mes_with_prev' in l for l in lines[i:i+100]):
                in_final_select = True
                print(f"{i:3d}: {line}")
