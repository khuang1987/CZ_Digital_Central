
import pandas as pd
from pathlib import Path

# Path determined from previous steps
SOURCE_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\40-SAP工时')
FILE_NAME = "SAP_laborhour_20260201.xlsx"

def check_specific_file():
    file_path = SOURCE_PATH / FILE_NAME
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return

    print(f"Reading file: {file_path}")
    df = pd.read_excel(file_path)
    
    print(f"Columns: {list(df.columns)}")
    
    if 'PostingDate' in df.columns:
        df['PostingDate'] = pd.to_datetime(df['PostingDate'], errors='coerce')
        print(f"Max Posting Date in File: {df['PostingDate'].max()}")
        print(f"Row Count: {len(df)}")
        
        # Check for 2026-01-31 specifically
        jan31 = df[df['PostingDate'] == '2026-01-31']
        print(f"Rows for 2026-01-31: {len(jan31)}")
    else:
        print("PostingDate column not found.")

if __name__ == "__main__":
    check_specific_file()
