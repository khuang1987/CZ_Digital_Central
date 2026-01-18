import sqlite3
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[1]
db_path = project_root / "data_pipelines" / "database" / "mddap_v2.db"

conn = sqlite3.connect(str(db_path))
conn.row_factory = sqlite3.Row

print("=== SAP Routing 表结构 ===")
schema = conn.execute("PRAGMA table_info(raw_sap_routing)").fetchall()
print("\n字段列表:")
for col in schema:
    print(f"  {col['name']}: {col['type']}")

print("\n=== SAP Routing 数据样本（前5条完整记录）===")
routing_rows = conn.execute("SELECT * FROM raw_sap_routing LIMIT 5").fetchall()
for i, row in enumerate(routing_rows, 1):
    print(f"\n记录 {i}:")
    for key in row.keys():
        val = row[key]
        if val is not None and val != '':
            print(f"  {key}: {val}")

print("\n=== 检查 SAP Routing 中有多少条记录有 CFN ===")
cfn_count = conn.execute("SELECT COUNT(*) as cnt FROM raw_sap_routing WHERE CFN IS NOT NULL AND CFN != ''").fetchone()
print(f"有 CFN 的记录数: {cfn_count['cnt']}")

print("\n=== 检查 MES 数据中的 CFN 和 ProductNumber 样本 ===")
mes_sample = conn.execute("""
    SELECT DISTINCT CFN, ProductNumber, Operation
    FROM raw_mes
    WHERE CFN IS NOT NULL
    LIMIT 10
""").fetchall()

print("\nMES 样本:")
for row in mes_sample:
    print(f"  CFN={row['CFN']}, ProductNumber={row['ProductNumber']}, Op={row['Operation']}")

conn.close()
