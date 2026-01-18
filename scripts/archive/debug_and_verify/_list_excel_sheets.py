"""列出 Excel 文件中的所有 sheet 名称"""
import pandas as pd
from pathlib import Path

routing_file = Path(r"c:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记\1303 Routing及机加工产品清单.xlsx")

print("=" * 100)
print("检查 Excel 文件中的 sheet 名称")
print("=" * 100)

if routing_file.exists():
    print(f"\n文件: {routing_file.name}")
    
    # 读取所有 sheet 名称
    xl = pd.ExcelFile(routing_file)
    print(f"\nSheet 列表:")
    for i, sheet in enumerate(xl.sheet_names):
        print(f"  {i}: {sheet}")
    
    # 读取第一个 sheet 的列名
    if len(xl.sheet_names) > 0:
        first_sheet = xl.sheet_names[0]
        print(f"\n读取第一个 sheet: {first_sheet}")
        df = pd.read_excel(routing_file, sheet_name=first_sheet, nrows=5)
        print(f"\n列名:")
        for col in df.columns:
            print(f"  - {col}")
        
        print(f"\n前5行数据:")
        print(df)
    
    # 如果有第二个 sheet
    if len(xl.sheet_names) > 1:
        second_sheet = xl.sheet_names[1]
        print(f"\n读取第二个 sheet: {second_sheet}")
        df2 = pd.read_excel(routing_file, sheet_name=second_sheet, nrows=5)
        print(f"\n列名:")
        for col in df2.columns:
            print(f"  - {col}")

else:
    print(f"\n文件不存在: {routing_file}")

print("\n" + "=" * 100)
