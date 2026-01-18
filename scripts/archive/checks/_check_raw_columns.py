import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.etl_utils import read_sharepoint_excel

# 检查原始文件的列名
file_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\CMES_Product_Output_CZM_2025.xlsx"

print("读取原始文件...")
df = read_sharepoint_excel(file_path, max_rows=5)

print(f"\n原始文件列名 ({len(df.columns)} 列):")
for i, col in enumerate(df.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n检查是否有 VSM 相关的列:")
vsm_cols = [col for col in df.columns if 'vsm' in col.lower() or 'value' in col.lower() or 'stream' in col.lower()]
if vsm_cols:
    print("  找到可能的 VSM 列:")
    for col in vsm_cols:
        print(f"    - {col}")
else:
    print("  ❌ 未找到 VSM 相关的列")

print("\n样本数据（前2行）:")
print(df.head(2).T)
