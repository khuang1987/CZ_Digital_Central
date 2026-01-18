import sqlite3
from pathlib import Path

db_path = Path(__file__).parent.parent / 'data_pipelines' / 'database' / 'mddap_v2.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()
print(f"Tables in mddap_v2.db ({len(tables)} total):")
for i, t in enumerate(tables):
    cursor.execute(f"SELECT COUNT(*) FROM {t[0]}")
    count = cursor.fetchone()[0]
    print(f"  {i+1}. {t[0]} ({count} rows)")
conn.close()
