import pandas as pd
import os
from pathlib import Path

# Path to the raw extracted XLS file
# Based on previous context: .../40-SAP工时/YPP_M03_Q5003_00000.xls
base_dir = Path(r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\40-SAP工时")
xls_file = base_dir / "YPP_M03_Q5003_00000.xls"

print(f"Testing read of: {xls_file}")

if not xls_file.exists():
    print("❌ File not found. Please ensure the .xls file exists (run the formatter first if needed, it extracts it).")
    exit(1)

# Method 1: Standard read_excel (tries xlrd for .xls)
print("\n--- Method 1: standard read_excel ---")
try:
    # engine='xlrd' is default for .xls, but let's be explicit.
    # Note: xlrd >= 2.0.0 only supports .xls, not .xlsx.
    df = pd.read_excel(xls_file, engine='xlrd')
    print("✅ Success! Head:")
    print(df.head())
except Exception as e:
    print(f"❌ Failed: {e}")

# Method 2: HTML (SAP sometimes exports HTML as .xls)
print("\n--- Method 2: read_html ---")
try:
    dfs = pd.read_html(str(xls_file))
    print(f"✅ Success! Found {len(dfs)} tables.")
    if len(dfs) > 0:
        print(dfs[0].head())
except Exception as e:
    print(f"❌ Failed: {e}")

# Method 3: Text/XML check
print("\n--- Method 3: Content Peek ---")
try:
    with open(xls_file, 'r', errors='ignore') as f:
        head = f.read(200)
    print(f"File Header: {head!r}")
except Exception as e:
    print(f"❌ Failed to read text: {e}")
