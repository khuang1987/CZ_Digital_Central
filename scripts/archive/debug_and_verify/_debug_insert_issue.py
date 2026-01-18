import pandas as pd
import sys
from pathlib import Path
import pyodbc

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from data_pipelines.sources.mes.etl.etl_mes_batch_output_raw import clean_mes_data
from shared_infrastructure.utils.etl_utils import read_sharepoint_excel

# Read and clean data
file_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\CMES_Product_Output_CZM_2025.xlsx"

print("Reading file...")
df = read_sharepoint_excel(file_path, max_rows=10)
df["factory_source"] = "1"
df["factory_name"] = "工厂1-CZM"
df["source_file"] = file_path

print("\nCleaning data...")
df_clean = clean_mes_data(df)

print(f"\nCleaned DataFrame shape: {df_clean.shape}")
print(f"Columns: {list(df_clean.columns)}")

# Get SQL Server table column order
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

with pyodbc.connect(conn_str) as conn:
    cur = conn.cursor()
    cur.execute("""
        SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'raw_mes' AND TABLE_SCHEMA = 'dbo'
        AND COLUMN_NAME != 'id'
        ORDER BY ORDINAL_POSITION
    """)
    
    table_cols = [(row[0], row[1], row[2]) for row in cur.fetchall()]
    
    print("\n=== SQL Server table columns ===")
    for pos, name, dtype in table_cols:
        print(f"{pos:2d}. {name:30s} {dtype}")

# Filter to valid columns in SQL Server order
valid_cols = [name for _, name, _ in table_cols if name in df_clean.columns]
df_insert = df_clean[valid_cols]

print(f"\n=== DataFrame columns after reordering ===")
for i, col in enumerate(df_insert.columns, 1):
    print(f"{i:2d}. {col}")

print(f"\n=== First row values ===")
first_row = df_insert.iloc[0]
for i, (col, val) in enumerate(first_row.items(), 1):
    val_type = type(val).__name__
    print(f"{i:2d}. {col:30s} = {val!r:50s} (type: {val_type})")
    
    if i == 14:
        print(f"    ^^^ THIS IS PARAMETER 14")
        print(f"    Value: {val}")
        print(f"    Type: {type(val)}")
        print(f"    Is NaN: {pd.isna(val)}")
        if isinstance(val, float):
            import math
            print(f"    Is finite: {math.isfinite(val)}")
