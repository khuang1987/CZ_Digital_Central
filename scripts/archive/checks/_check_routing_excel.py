import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# 读取 SAP routing 原始文件
file_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记\1303 Routing及机加工产品清单.xlsx"

print("=" * 80)
print("检查 SAP Routing 原始文件")
print("=" * 80)

# 读取 Routing 表
df = pd.read_excel(file_path, sheet_name="Routing")

print(f"\n文件: {file_path}")
print(f"Sheet: Routing")
print(f"行数: {len(df)}")
print(f"列数: {len(df.columns)}")

print("\n列名:")
for i, col in enumerate(df.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n前3行样本数据:")
print(df.head(3).T)

# 检查是否有 StandardTime 或相关字段
print("\n" + "=" * 80)
print("查找时间相关字段")
print("=" * 80)

time_related = [col for col in df.columns if any(keyword in str(col).lower() 
                for keyword in ['time', 'machine', 'labor', '时间', '工时', 'standard', 'std'])]

if time_related:
    print(f"\n找到时间相关字段: {time_related}")
    
    for col in time_related:
        print(f"\n{col}:")
        print(f"  数据类型: {df[col].dtype}")
        print(f"  非空数量: {df[col].notna().sum()}")
        print(f"  样本值: {df[col].dropna().head(3).tolist()}")
else:
    print("\n未找到时间相关字段")

print("\n" + "=" * 80)
