import os
import sys
from pathlib import Path

# Fix path to include project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

print(f"--- MDDAP Path Diagnostics ---")
print(f"Project Root: {PROJECT_ROOT}")
print(f"Current User: {os.getlogin() if hasattr(os, 'getlogin') else 'unknown'}")
print(f"Home Dir: {os.path.expanduser('~')}")

from shared_infrastructure.env_utils import get_onedrive_root, ENV_FILE

print(f"--- Env Info ---")
print(f".env Path: {ENV_FILE}")
print(f".env Exists: {ENV_FILE.exists()}")
print(f"MDDAP_ONEDRIVE_ROOT (os.getenv): {os.getenv('MDDAP_ONEDRIVE_ROOT')}")
print(f"get_onedrive_root(): {get_onedrive_root()}")

print(f"--- Config Checks ---")
planner_config = PROJECT_ROOT / "data_pipelines" / "sources" / "planner" / "config" / "config_planner_tasks.yaml"
print(f"Planner Config Path: {planner_config}")
print(f"Planner Config Exists: {planner_config.exists()}")

if planner_config.exists():
    from shared_infrastructure.env_utils import load_yaml_with_env
    try:
        config = load_yaml_with_env(planner_config)
        print(f"Resolved planner_path: {config.get('source', {}).get('planner_path')}")
        p = Path(config.get('source', {}).get('planner_path'))
        print(f"Path object exists: {p.exists()}")
    except Exception as e:
        print(f"Error loading config: {e}")
