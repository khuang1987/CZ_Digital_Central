"""Maintenance: Clear raw SAP routing table in SQLite.

Purpose:
  Remove all rows from the SQLite table raw_sap_routing.
  Useful when you want to re-import routing data into SQLite.

Effects:
  - Deletes all rows from raw_sap_routing in data_pipelines/database/mddap_v2.db.

Run:
  python scripts/maintenance/_clear_sqlite_routing.py
"""

import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data_pipelines" / "database" / "mddap_v2.db"

print("=" * 80)
print("清空 SQLite 中的 raw_sap_routing 表")
print("=" * 80)

with sqlite3.connect(DB_PATH) as conn:
    cur = conn.cursor()
    
    # 检查当前记录数
    cur.execute("SELECT COUNT(*) FROM raw_sap_routing")
    before_count = cur.fetchone()[0]
    print(f"\n当前记录数: {before_count:,}")
    
    if before_count > 0:
        # 清空表
        print("\n清空表数据...")
        cur.execute("DELETE FROM raw_sap_routing")
        conn.commit()
        
        # 验证
        cur.execute("SELECT COUNT(*) FROM raw_sap_routing")
        after_count = cur.fetchone()[0]
        print(f"✓ 清空完成，当前记录数: {after_count:,}")
    else:
        print("\n表已经是空的")

print("\n" + "=" * 80)
print("✓ SQLite 表已清空")
print("=" * 80)
