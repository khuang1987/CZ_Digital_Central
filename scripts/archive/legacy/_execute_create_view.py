import pyodbc
import sys
from pathlib import Path

# SQL 脚本路径
sql_file = Path(__file__).parent / "_create_mes_view_sqlserver.sql"

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

print("=" * 80)
print("创建 v_mes_metrics 视图")
print("=" * 80)

# 读取 SQL 脚本
print(f"\n读取 SQL 脚本: {sql_file}")
with open(sql_file, 'r', encoding='utf-8') as f:
    sql_content = f.read()

# 按 GO 分割 SQL 语句
sql_statements = [stmt.strip() for stmt in sql_content.split('GO') if stmt.strip()]

print(f"找到 {len(sql_statements)} 个 SQL 语句")

# 执行 SQL 语句
with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    for i, stmt in enumerate(sql_statements, 1):
        print(f"\n执行语句 {i}/{len(sql_statements)}...")
        try:
            cur.execute(stmt)
            conn.commit()
            print(f"✓ 语句 {i} 执行成功")
        except Exception as e:
            print(f"✗ 语句 {i} 执行失败: {e}")
            sys.exit(1)

print("\n" + "=" * 80)
print("✓ 视图创建成功")
print("=" * 80)

# 验证视图
print("\n验证视图...")
with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    
    # 检查视图是否存在
    cur.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.VIEWS 
        WHERE TABLE_NAME = 'v_mes_metrics' AND TABLE_SCHEMA = 'dbo'
    """)
    view_exists = cur.fetchone()[0] > 0
    
    if view_exists:
        print("✓ 视图 v_mes_metrics 已创建")
        
        # 查询视图记录数
        cur.execute("SELECT COUNT(*) FROM dbo.v_mes_metrics")
        view_count = cur.fetchone()[0]
        print(f"✓ 视图记录数: {view_count:,}")
        
        # 查询视图列数
        cur.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'v_mes_metrics' AND TABLE_SCHEMA = 'dbo'
        """)
        col_count = cur.fetchone()[0]
        print(f"✓ 视图列数: {col_count}")
        
    else:
        print("✗ 视图创建失败")
        sys.exit(1)

print("\n" + "=" * 80)
print("视图创建和验证完成")
print("=" * 80)
