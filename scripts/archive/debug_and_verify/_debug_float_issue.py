import pandas as pd
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.etl_utils import read_sharepoint_excel

# Read 2025 CZM file
file_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\CMES_Product_Output_CZM_2025.xlsx"

print("Reading file...")
df = read_sharepoint_excel(file_path, max_rows=100)

print(f"\nDataFrame shape: {df.shape}")
print(f"\nColumn names and types:")
for i, col in enumerate(df.columns, 1):
    dtype = df[col].dtype
    print(f"  {i}. {col}: {dtype}")
    
    # Check for problematic values in numeric columns
    if dtype in ['float64', 'int64']:
        has_nan = df[col].isna().any()
        has_inf = False
        try:
            has_inf = (df[col] == float('inf')).any() or (df[col] == float('-inf')).any()
        except:
            pass
        
        if has_nan or has_inf:
            print(f"     WARNING: has NaN={has_nan}, has Inf={has_inf}")
            print(f"     Sample values: {df[col].head(5).tolist()}")

# Check column 14 specifically
if len(df.columns) >= 14:
    col_14 = df.columns[13]  # 0-indexed
    print(f"\n=== Column 14 (index 13): {col_14} ===")
    print(f"Type: {df[col_14].dtype}")
    print(f"Sample values:")
    print(df[col_14].head(20))
    print(f"\nUnique values count: {df[col_14].nunique()}")
    print(f"Null count: {df[col_14].isna().sum()}")
