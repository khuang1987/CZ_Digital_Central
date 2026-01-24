
import os
import sys
import yaml
import zipfile
import quopri
from pathlib import Path
import pandas as pd
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

def get_config():
    config_path = PROJECT_ROOT / "data_pipelines/sources/sap/config/config_sap_labor.yaml"
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}

def test_mhtml_read():
    config = get_config()
    
    # Get paths
    try:
        base_path = Path(config['source']['labor_hour_path'])
    except KeyError:
        print("❌ Could not find path in config config_sap_labor.yaml")
        return

    file_patterns = config['source']['file_patterns']
    zip_name = file_patterns.get('zip_source')
    xls_name = file_patterns.get('excel_intermediate') # This is the MHTML file
    
    zip_path = base_path / zip_name
    xls_path = base_path / xls_name
    
    print(f"Target Base Path: {base_path}")
    print(f"Target XLS Path: {xls_path}")

    # Ensure .xls exists (unzip if needed)
    if not xls_path.exists():
        if zip_path.exists():
            print(f"ℹ️ .xls not found, extracting from {zip_name}...")
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(base_path)
        else:
            print(f"❌ Both .xls and .zip are missing in {base_path}")
            return

    if not xls_path.exists():
         print("❌ Failed to extract .xls file")
         return

    print("✅ Found .xls file. Attempting to parse as MHTML...")

    try:
        with open(xls_path, 'rb') as f:
            content_bytes = f.read()

        # Step 1: Decode quoted-printable
        # MHTML often uses quoted-printable. We can use quopri to decode it.
        # Alternatively, open as text with errors='replace', but binary + decode is safer for QP.
        
        try:
            decoded_bytes = quopri.decodestring(content_bytes)
            decoded_str = decoded_bytes.decode('utf-8', errors='replace')
        except Exception as e:
            print(f"⚠️ Quopri decoding failed: {e}. Trying raw read...")
            decoded_str = content_bytes.decode('utf-8', errors='replace')

        # Step 2: Use Pandas read_html on the decoded string
        # read_html returns a list of dataframes
        dfs = pd.read_html(decoded_str)
        
        if dfs:
            print(f"✅ Success! Found {len(dfs)} tables.")
            df = dfs[0]
            print("Preview of first table:")
            print(df.head())
            print(f"\nShape: {df.shape}")
            print("\nColumns:")
            print(df.columns.tolist())
            return True
        else:
            print("❌ No tables found in the file.")
            
    except Exception as e:
        print(f"❌ Failed to read MHTML: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_mhtml_read()
