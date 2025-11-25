import pandas as pd

# 读取当前CSV文件
input_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序管理区域分类_完整版.csv'
df = pd.read_csv(input_path, encoding='utf-8-sig')

print('原始数据统计:')
print(f'总行数: {len(df)}')
print(f'ERPCode分布:')
print(df['ERPCode'].value_counts().sort_index())

# 读取MES数据获取清洗后的工序名称
mes_df = pd.read_parquet(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish\MES_batch_report_latest.parquet')

# 获取清洗后的CZM和CKH工序
czm_cleaned = sorted(mes_df[mes_df['factory_source'] == 'CZM']['Operation description'].dropna().unique())
ckh_cleaned = sorted(mes_df[mes_df['factory_source'] == 'CKH']['Operation description'].dropna().unique())

print(f'\n清洗后的CZM工序: {len(czm_cleaned)}个')
print(f'清洗后的CKH工序: {len(ckh_cleaned)}个')

# 分离数据
# 1. 清洗后的CZM工序（从现有表中找出）
czm_cleaned_rows = df[df['SAP Routing'].isin(czm_cleaned) & (df['ERPCode'] == 1303)].copy()

# 2. 清洗后的CKH工序
ckh_cleaned_rows = df[df['SAP Routing'].isin(ckh_cleaned) & (df['ERPCode'] == 9997)].copy()

# 3. 原始的CZM工序（不在清洗后列表中的1303工序）
czm_original_operations = set(df[df['ERPCode'] == 1303]['SAP Routing']) - set(czm_cleaned)
czm_original_rows = df[df['SAP Routing'].isin(czm_original_operations) & (df['ERPCode'] == 1303)].copy()

print(f'\n分类统计:')
print(f'清洗后CZM工序: {len(czm_cleaned_rows)}行')
print(f'清洗后CKH工序: {len(ckh_cleaned_rows)}行')
print(f'原始CZM工序: {len(czm_original_rows)}行')

# 按要求重新排序和编号
result_rows = []

# 1. 清洗后的CZM工序
czm_cleaned_rows = czm_cleaned_rows.sort_values('SAP Routing')
for i, (_, row) in enumerate(czm_cleaned_rows.iterrows(), 1):
    row_copy = row.copy()
    row_copy['No'] = i
    result_rows.append(row_copy)

# 2. 清洗后的CKH工序
ckh_cleaned_rows = ckh_cleaned_rows.sort_values('SAP Routing')
for i, (_, row) in enumerate(ckh_cleaned_rows.iterrows(), len(result_rows) + 1):
    row_copy = row.copy()
    row_copy['No'] = i
    result_rows.append(row_copy)

# 3. 原始的CZM工序
czm_original_rows = czm_original_rows.sort_values('SAP Routing')
for i, (_, row) in enumerate(czm_original_rows.iterrows(), len(result_rows) + 1):
    row_copy = row.copy()
    row_copy['No'] = i
    result_rows.append(row_copy)

# 创建最终DataFrame
final_df = pd.DataFrame(result_rows)

print(f'\n重新组织后总行数: {len(final_df)}')

# 显示各部分的示例
print('\n=== 清洗后的CZM工序示例 ===')
print(final_df[final_df['No'] <= min(5, len(czm_cleaned_rows))][['No', 'SAP Routing', 'ERPCode']].to_string(index=False))

print('\n=== 清洗后的CKH工序示例 ===')
ckh_start_no = len(czm_cleaned_rows) + 1
ckh_end_no = ckh_start_no + min(4, len(ckh_cleaned_rows) - 1)
ckh_sample = final_df[(final_df['No'] >= ckh_start_no) & (final_df['No'] <= ckh_end_no)]
print(ckh_sample[['No', 'SAP Routing', 'ERPCode']].to_string(index=False))

print('\n=== 原始CZM工序示例 ===')
original_start_no = len(czm_cleaned_rows) + len(ckh_cleaned_rows) + 1
original_end_no = original_start_no + min(4, len(czm_original_rows) - 1)
original_sample = final_df[(final_df['No'] >= original_start_no) & (final_df['No'] <= original_end_no)]
print(original_sample[['No', 'SAP Routing', 'ERPCode']].to_string(index=False))

# 保存重新组织后的文件
output_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序管理区域分类_重新排序.csv'
final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'\n重新排序的CSV文件已保存至: {output_path}')
