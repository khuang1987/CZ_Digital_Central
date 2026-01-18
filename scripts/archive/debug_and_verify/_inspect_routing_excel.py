import pandas as pd
from pathlib import Path

# 检查 1303 工厂的 Routing 和机加工清单
file_path = r"C:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记\1303 Routing及机加工产品清单.xlsx"

print("=" * 80)
print("检查 SAP Routing Excel 文件结构")
print("=" * 80)

# 获取所有 sheet 名称
xl_file = pd.ExcelFile(file_path)
print(f"\n文件: {file_path}")
print(f"Sheet 列表: {xl_file.sheet_names}")

# 读取 Routing 表
print("\n" + "=" * 80)
print("1303 Routing 表")
print("=" * 80)

df_routing = pd.read_excel(file_path, sheet_name="1303 Routing")
print(f"\n行数: {len(df_routing)}")
print(f"列数: {len(df_routing.columns)}")
print("\n列名:")
for i, col in enumerate(df_routing.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n前3行样本:")
print(df_routing.head(3))

# 读取机加工清单表
print("\n" + "=" * 80)
print("1303机加工清单 表")
print("=" * 80)

df_machining = pd.read_excel(file_path, sheet_name="1303机加工清单")
print(f"\n行数: {len(df_machining)}")
print(f"列数: {len(df_machining.columns)}")
print("\n列名:")
for i, col in enumerate(df_machining.columns, 1):
    print(f"  {i:2d}. {col}")

print("\n前3行样本:")
print(df_machining.head(3))

# 检查匹配字段
print("\n" + "=" * 80)
print("匹配字段分析")
print("=" * 80)

# 查找可能的匹配键
routing_keys = [col for col in df_routing.columns if any(k in str(col).lower() 
                for k in ['material', 'product', 'cfn', '物料', '图纸'])]
machining_keys = [col for col in df_machining.columns if any(k in str(col).lower() 
                  for k in ['material', 'product', 'cfn', '物料', '图纸'])]

print(f"\nRouting 表可能的匹配键: {routing_keys}")
print(f"机加工清单可能的匹配键: {machining_keys}")

# 查找 SetupTime 和 OEE 相关字段
setup_cols = [col for col in df_machining.columns if any(k in str(col).lower() 
              for k in ['setup', 'oee', '调试', '换型', '效率'])]
print(f"\n机加工清单中的 SetupTime/OEE 相关字段: {setup_cols}")

# 检查数据样本
if setup_cols:
    print("\n样本数据:")
    sample_cols = machining_keys + setup_cols
    sample_cols = [c for c in sample_cols if c in df_machining.columns]
    if sample_cols:
        print(df_machining[sample_cols].head(5))

print("\n" + "=" * 80)
