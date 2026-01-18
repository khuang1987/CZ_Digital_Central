"""执行创建 v_mes_metrics 视图的 SQL 脚本（V2 版本，按技术文档定义）"""
import pyodbc
from pathlib import Path

def _resolve_sql_file(filename: str) -> Path:
    current_dir = Path(__file__).resolve().parent
    return current_dir / filename


SQL_FILE = _resolve_sql_file("_init_mes_metrics_materialized.sql")

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

print("=" * 80)
print("创建 v_mes_metrics 视图（物化快照 - BI 秒开）")
print("=" * 80)

# 读取 SQL 脚本
print(f"\n读取 SQL 脚本: {SQL_FILE}")
with open(SQL_FILE, 'r', encoding='utf-8') as f:
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
            raise

print("\n" + "=" * 80)
print("✓ 数据库对象初始化完成")
print("=" * 80)

# 验证视图
print("\n验证视图...")
with pyodbc.connect(conn_str) as conn:
    conn.timeout = 30
    cur = conn.cursor()
    cur.execute("SET LOCK_TIMEOUT 5000")
    
    # 检查视图是否存在
    cur.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.VIEWS 
        WHERE TABLE_NAME = 'v_mes_metrics'
    """)
    
    if cur.fetchone()[0] > 0:
        print("✓ 视图 v_mes_metrics 已创建")

        # 检查列数
        cur.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'v_mes_metrics'
        """)
        col_count = cur.fetchone()[0]
        print(f"✓ 视图列数: {col_count}")

        try:
            cur.execute("SELECT TOP 0 * FROM dbo.v_mes_metrics")
            print("✓ 视图可查询(Top0): True")
        except Exception as e:
            print(f"✗ 视图查询(Top0)失败: {e}")
        
    else:
        print("✗ 视图创建失败")

print("\n" + "=" * 80)
print("视图创建和验证完成")
print("=" * 80)
