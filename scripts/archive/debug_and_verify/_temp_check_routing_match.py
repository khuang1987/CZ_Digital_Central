import sqlite3
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "data_pipelines" / "database" / "mddap_v2.db"

conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row

print("=== 检查 MES 数据样本 ===")
mes_sample = conn.execute("""
    SELECT CFN, ProductNumber, Operation, "Group", COUNT(*) as cnt
    FROM raw_mes
    GROUP BY CFN, ProductNumber, Operation, "Group"
    ORDER BY cnt DESC
    LIMIT 10
""").fetchall()

print("\nMES 数据样本（CFN, ProductNumber, Operation, Group）:")
for row in mes_sample:
    print(f"  CFN={row['CFN']}, ProductNumber={row['ProductNumber']}, Op={row['Operation']}, Group={row['Group']}, Count={row['cnt']}")

print("\n=== 检查 SAP Routing 数据 ===")
routing_sample = conn.execute("""
    SELECT CFN, ProductNumber, Operation, "Group", COUNT(*) as cnt
    FROM raw_sap_routing
    GROUP BY CFN, ProductNumber, Operation, "Group"
    ORDER BY cnt DESC
    LIMIT 10
""").fetchall()

print("\nSAP Routing 数据样本（CFN, ProductNumber, Operation, Group）:")
for row in routing_sample:
    print(f"  CFN={row['CFN']}, ProductNumber={row['ProductNumber']}, Op={row['Operation']}, Group={row['Group']}, Count={row['cnt']}")

print("\n=== 检查匹配率 ===")
# 检查有多少 MES 记录能通过 CFN 匹配到 routing
cfn_match = conn.execute("""
    SELECT COUNT(*) as cnt
    FROM raw_mes m
    INNER JOIN raw_sap_routing r
        ON m.CFN = r.CFN
        AND TRIM(m.Operation) = TRIM(r.Operation)
        AND m."Group" = r."Group"
""").fetchone()

print(f"通过 CFN 匹配到 routing 的记录数: {cfn_match['cnt']}")

# 检查有多少 MES 记录能通过 ProductNumber 匹配到 routing
product_match = conn.execute("""
    SELECT COUNT(*) as cnt
    FROM raw_mes m
    INNER JOIN raw_sap_routing r
        ON m.ProductNumber = r.ProductNumber
        AND TRIM(m.Operation) = TRIM(r.Operation)
        AND m."Group" = r."Group"
""").fetchone()

print(f"通过 ProductNumber 匹配到 routing 的记录数: {product_match['cnt']}")

# 总记录数
total = conn.execute("SELECT COUNT(*) as cnt FROM raw_mes").fetchone()
print(f"MES 总记录数: {total['cnt']}")

# 检查当前视图中有多少记录有 unit_time
view_with_time = conn.execute("""
    SELECT COUNT(*) as cnt
    FROM v_mes_metrics
    WHERE unit_time IS NOT NULL AND unit_time > 0
""").fetchone()

print(f"\n当前视图中有 unit_time 的记录数: {view_with_time['cnt']}")

conn.close()
