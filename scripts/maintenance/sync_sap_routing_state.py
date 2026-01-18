import os
import glob
import logging
import sys
from pathlib import Path

# Add project root to path
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
from shared_infrastructure.utils.etl_utils import load_config

def sync_sap_routing_state():
    print("Syncing SAP Routing file state...")
    db = SQLServerOnlyManager(sql_db="mddap_v2")
    
    # Load config to get file paths
    # Config is at data_pipelines/sources/sap/config/config_sap_routing.yaml
    config_path = os.path.join(project_root, "data_pipelines", "sources", "sap", "config", "config_sap_routing.yaml")
    cfg = load_config(config_path)
    
    source_cfg = cfg.get("source", {})
    excel_base_dir = source_cfg.get("excel_base_dir", "")
    factory_files = source_cfg.get("factory_files", [])
    
    for factory_cfg in factory_files:
        file_name = factory_cfg.get("file", "")
        factory_code = factory_cfg.get("factory_code", "")
        
        file_path = os.path.join(excel_base_dir, file_name)
        file_path = os.path.normpath(os.path.abspath(file_path))
        
        if os.path.exists(file_path):
            print(f"Marking processed: {file_path}")
            db.mark_file_processed(f"sap_routing_raw_{factory_code}", file_path)
            
            # Verify
            changed = db.filter_changed_files(f"sap_routing_raw_{factory_code}", [file_path])
            if not changed:
                print(f"  Verified: File is now considered skipped (unchanged).")
            else:
                print(f"  WARNING: File still considered changed!")
        else:
            print(f"File not found: {file_path}")

if __name__ == "__main__":
    sync_sap_routing_state()
