
import sys
import glob
import os
import pandas as pd
from pathlib import Path

# Path configuration
SOURCE_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\40-SAP工时')

def check_excel_date():
    # Find latest YPP file
    files = list(SOURCE_PATH.glob('YPP*.xlsx'))
    if not files:
        print("No YPP files found.")
        return

    latest_file = max(files, key=lambda x: x.stat().st_mtime)
    print(f"Checking Source File: {latest_file}")
    
    # Read headers to find Date column
    # Skip 7 rows based on known format
    df = pd.read_excel(latest_file, skiprows=7)
    
    # Identify Posting Date column (usually index 17 or named 'Posting Date')
    # Let's look for likely candidates
    date_col = None
    for col in df.columns:
        if 'Posting' in str(col) and 'Date' in str(col):
            date_col = col
            break
            
    if not date_col:
        # Fallback to column index 17 if available
        if len(df.columns) > 17:
            date_col = df.columns[17]
            print(f"Column name not matched, using index 17: {date_col}")
        else:
            print("Could not identify Posting Date column.")
            print(f"Columns: {df.columns.tolist()}")
            return

    print(f"Found Date Column: {date_col}")
    
    # Parse dates
    # Assuming standard SAP format (often DD.MM.YYYY)
    dates = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
    
    print(f"Max Date in File: {dates.max()}")
    print(f"Unique Dates (Last 5): {sorted(dates.dropna().unique())[-5:]}")
    
    # Check specifically for 2026-01-31
    has_target = (dates == '2026-01-31').any()
    print(f"Has 2026-01-31 data? {has_target}")

if __name__ == "__main__":
    check_excel_date()
