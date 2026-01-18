import sqlite3
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "data_pipelines" / "database" / "mddap_v2.db"

conn = sqlite3.connect(str(db_path))

print("=== 测试 CFN 匹配（修正 Operation 格式）===")

# 测试：将 MES 的 Operation 转为整数字符串后匹配
cfn_match = conn.execute("""
    SELECT COUNT(DISTINCT m.id) as cnt
    FROM raw_mes m
    INNER JOIN raw_sap_routing r
        ON m.CFN = r.CFN
        AND CAST(CAST(m.Operation AS REAL) AS INTEGER) = CAST(r.Operation AS INTEGER)
        AND m."Group" = r."Group"
    WHERE r.CFN IS NOT NULL
""").fetchone()

print(f"通过 CFN 匹配（修正 Operation 格式）: {cfn_match[0]} 条")

# 测试：只匹配 CFN + Operation（不要求 Group）
cfn_match_no_group = conn.execute("""
    SELECT COUNT(DISTINCT m.id) as cnt
    FROM raw_mes m
    INNER JOIN raw_sap_routing r
        ON m.CFN = r.CFN
        AND CAST(CAST(m.Operation AS REAL) AS INTEGER) = CAST(r.Operation AS INTEGER)
    WHERE r.CFN IS NOT NULL
""").fetchone()

print(f"通过 CFN + Operation 匹配（不要求 Group）: {cfn_match_no_group[0]} 条")

# 测试：只匹配 CFN（不要求 Operation 和 Group）
cfn_only = conn.execute("""
    SELECT COUNT(DISTINCT m.id) as cnt
    FROM raw_mes m
    INNER JOIN raw_sap_routing r
        ON m.CFN = r.CFN
    WHERE r.CFN IS NOT NULL
""").fetchone()

print(f"只通过 CFN 匹配: {cfn_only[0]} 条")

# MES 总记录数
total = conn.execute("SELECT COUNT(*) as cnt FROM raw_mes").fetchone()
print(f"\nMES 总记录数: {total[0]}")

# 检查有多少 MES 记录的 CFN 在 SAP Routing 中不存在
no_match = conn.execute("""
    SELECT COUNT(*) as cnt
    FROM raw_mes m
    WHERE NOT EXISTS (
        SELECT 1 FROM raw_sap_routing r
        WHERE m.CFN = r.CFN
        AND CAST(CAST(m.Operation AS REAL) AS INTEGER) = CAST(r.Operation AS INTEGER)
        AND m."Group" = r."Group"
    )
""").fetchone()

print(f"无法匹配的 MES 记录数: {no_match[0]}")
print(f"匹配率: {(cfn_match[0] / total[0] * 100):.2f}%")

# 查看一些无法匹配的样本
print("\n=== 无法匹配的 MES 记录样本 ===")
no_match_samples = conn.execute("""
    SELECT m.CFN, m.Operation, m."Group", COUNT(*) as cnt
    FROM raw_mes m
    WHERE NOT EXISTS (
        SELECT 1 FROM raw_sap_routing r
        WHERE m.CFN = r.CFN
        AND CAST(CAST(m.Operation AS REAL) AS INTEGER) = CAST(r.Operation AS INTEGER)
        AND m."Group" = r."Group"
    )
    GROUP BY m.CFN, m.Operation, m."Group"
    ORDER BY cnt DESC
    LIMIT 10
""").fetchall()

for row in no_match_samples:
    cfn, op, grp, cnt = row
    # 检查这个 CFN 在 routing 中是否存在
    routing_exists = conn.execute("""
        SELECT COUNT(*) FROM raw_sap_routing
        WHERE CFN = ? AND CAST(Operation AS INTEGER) = ?
    """, (cfn, int(float(op)))).fetchone()[0]
    
    print(f"  CFN={cfn}, Op={op}, Group={grp}, Count={cnt}, Routing存在={routing_exists > 0}")

conn.close()
