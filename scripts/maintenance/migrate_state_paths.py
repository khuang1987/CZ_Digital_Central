import os
import json
import re
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_PIPELINES = PROJECT_ROOT / "data_pipelines"

# Paths to search for state files (typically in sources/*/state/)
STATE_DIRS = list(DATA_PIPELINES.glob("sources/*/state"))

# Standard placeholder
PLACEHOLDER = "${MDDAP_ONEDRIVE_ROOT}"

# Roots to migrate
LEGACY_ROOTS = [
    r"C:/Users/huangk14/OneDrive - Medtronic PLC",
    r"C:\Users\huangk14\OneDrive - Medtronic PLC",
    r"C:/Users/czxmfg/OneDrive - Medtronic PLC",
    r"C:\Users\czxmfg\OneDrive - Medtronic PLC"
]

def migrate_content(content_str):
    """Replace all legacy roots with placeholder in a string"""
    new_content = content_str
    for legacy in LEGACY_ROOTS:
        # Escape for regex and handle both slash types
        escaped = re.escape(legacy).replace(r'\/', r'[/\\]').replace(r'\\', r'[/\\]')
        new_content = re.sub(escaped, PLACEHOLDER, new_content, flags=re.IGNORECASE)
    return new_content

def process_state_file(file_path):
    logging.info(f"Checking state file: {file_path.relative_to(PROJECT_ROOT)}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
        new_content = migrate_content(content)
        
        if new_content != content:
            # Check if it's valid JSON before writing back
            try:
                json.loads(new_content)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                logging.info(f"  [FIXED] Normalized paths in {file_path.name}")
            except json.JSONDecodeError as je:
                logging.error(f"  [ERROR] Migration resulted in invalid JSON for {file_path.name}: {je}")
        else:
            logging.info(f"  [OK] No legacy paths found.")
            
    except Exception as e:
        logging.error(f"  [FAILED] Could not process {file_path.name}: {e}")

def main():
    logging.info(f"Starting state path migration in {PROJECT_ROOT}...")
    
    found_files = []
    for state_dir in STATE_DIRS:
        found_files.extend(list(state_dir.glob("*.json")))

    if not found_files:
        logging.warning("No state files (.json) found in source state directories.")
        return

    logging.info(f"Found {len(found_files)} potential state files.")
    
    for file_path in found_files:
        process_state_file(file_path)
        
    logging.info("Migration complete.")

if __name__ == "__main__":
    main()
