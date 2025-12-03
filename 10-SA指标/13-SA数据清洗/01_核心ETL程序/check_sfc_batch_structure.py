import pandas as pd

# 读取SFC批次报工表
sfc_batch_path = r'C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/SFC_batch_report_latest.parquet'
df = pd.read_parquet(sfc_batch_path)

print("SFC批次报工表字段列表:")
for i, col in enumerate(df.columns):
    print(f"{i+1:2d}. {col}")

print("\n关键字段检查:")
key_fields = ['BatchNumber', 'Operation', 'Operation description', 'TrackOutTime']
for col in key_fields:
    if col in df.columns:
        print(f"[OK] {col}: {df[col].dtype}")
    else:
        print(f"[ERROR] {col}: 不存在")

print("\n相关字段样本数据:")
sample_fields = ['BatchNumber', 'Operation', 'Operation description', 'TrackOutTime']
existing_fields = [col for col in sample_fields if col in df.columns]

if existing_fields:
    print(df[existing_fields].head(3).to_string())
else:
    print("关键字段都不存在，显示所有包含相关词的字段:")
    related_fields = [col for col in df.columns if any(keyword in col.lower() for keyword in ['batch', 'operation', 'time', 'track'])]
    print(related_fields)
    if related_fields:
        print("\n相关字段样本数据:")
        print(df[related_fields].head(3).to_string())
