import sqlite3
import pandas as pd
from pathlib import Path

# Use absolute path relative to script execution or determine dynamically
# For this script we will assume run from project root or find relative to file
DB_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\data_pipelines\database\mddap_v2.db')

def inspect_view():
    print(f"Connecting to {DB_PATH}")
    if not DB_PATH.exists():
        print("Database file not found!")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        # Get list of columns
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(v_mes_metrics)")
        columns = cursor.fetchall()
        print("Columns in v_mes_metrics:")
        for col in columns:
            print(f"- {col[1]}")
            
        # Also print the SQL used to create the view
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='view' AND name='v_mes_metrics'")
        view_sql = cursor.fetchone()
        if view_sql:
            print("\nView Definition:")
            print(view_sql[0])
        else:
            print("\nView v_mes_metrics not found in sqlite_master.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    inspect_view()
