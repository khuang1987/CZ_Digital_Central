import os
import sys
import pyodbc
import time
from pathlib import Path

# Add project root to path
project_root = str(Path(__file__).resolve().parents[1])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
from shared_infrastructure.utils.etl_utils import load_config

def debug_mtime():
    print("Debug Mtime Issue...")
    
    # Load config to get one file path
    config_path = os.path.join(project_root, "data_pipelines", "sources", "sap", "config", "config_sap_routing.yaml")
    cfg = load_config(config_path)
    factory_files = cfg.get("source", {}).get("factory_files", [])
    if not factory_files:
        print("No factory files found in config")
        return

    excel_base_dir = cfg.get("source", {}).get("excel_base_dir", "")
    f_cfg = factory_files[0]
    file_name = f_cfg.get("file", "")
    factory_code = f_cfg.get("factory_code", "")
    
    file_path = os.path.join(excel_base_dir, file_name)
    file_path = os.path.normpath(os.path.abspath(file_path))
    etl_name = f"sap_routing_raw_{factory_code}"
    
    print(f"Target File: {file_path}")
    print(f"ETL Name: {etl_name}")
    
    if not os.path.exists(file_path):
        print("File does not exist on disk!")
        return

    curr_mtime = os.path.getmtime(file_path)
    curr_size = os.path.getsize(file_path)
    print(f"Current FS Mtime: {curr_mtime}")
    print(f"Current FS Size: {curr_size}")
    
    db = SQLServerOnlyManager(sql_db="mddap_v2")
    
    # Check DB
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT file_mtime, file_size, file_path FROM dbo.etl_file_state WHERE etl_name = ?", (etl_name,))
        rows = cursor.fetchall()
        print(f"Found {len(rows)} records in DB for {etl_name}:")
        for r in rows:
            print(f"  DB Path: {r[2]}")
            print(f"  DB Mtime: {r[0]}")
            print(f"  DB Size: {r[1]}")
            print(f"  Path Match: {r[2] == file_path}")
            print(f"  Mtime Diff: {abs(r[0] - curr_mtime) if r[0] is not None else 'None'}")
            print("-" * 20)
            
    # Try Mark Processed
    print("Calling db.mark_file_processed...")
    db.mark_file_processed(etl_name, file_path)
    
    # Check DB Again
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT file_mtime, file_size FROM dbo.etl_file_state WHERE etl_name = ? AND file_path = ?", (etl_name, file_path))
        rows = cursor.fetchall()
        print(f"Found {len(rows)} records in DB matching exact path:")
        for r in rows:
            print(f"  DB Mtime: {r[0]}")
            print(f"  Mtime Diff: {abs(r[0] - curr_mtime)}")
            
    # Check Filter Changed Files
    changed = db.filter_changed_files(etl_name, [file_path])
    print(f"db.filter_changed_files returned: {changed}")
    if not changed:
        print("SUCCESS: File is skipped.")
    else:
        print("FAILURE: File is NOT skipped.")

if __name__ == "__main__":
    debug_mtime()
