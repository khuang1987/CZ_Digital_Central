import pandas as pd

# 读取优化版CSV文件
input_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序管理区域分类_优化版.csv'
df = pd.read_csv(input_path, encoding='utf-8-sig')

print('优化前的Area字段:')
unique_areas_before = df['Area'].dropna().unique()
for area in sorted(unique_areas_before):
    print(f'  {area}')

# 应用最终的Area标准化
final_area_mapping = {
    '终检-FIP': '终检 FI',  # Final Inspection - 标准缩写
    '无菌': '无菌 STR',     # Sterile - 新增缩写
}

print(f'\n应用最终Area标准化...')
df['Area'] = df['Area'].replace(final_area_mapping)

# 验证修改结果
unique_areas_after = df['Area'].dropna().unique()
print(f'\n最终标准化后的Area字段 ({len(unique_areas_after)}个):')
for area in sorted(unique_areas_after):
    print(f'  {area}')

# 显示修改统计
changes_made = []
for old_val, new_val in final_area_mapping.items():
    if old_val in unique_areas_before:
        changes_made.append(f'{old_val} → {new_val}')

print(f'\n最终修改统计:')
for change in changes_made:
    print(f'  ✅ {change}')

# 保存最终版本
output_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序管理区域分类_最终版.csv'
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f'\n✅ 最终优化版CSV文件已保存至: {output_path}')
print(f'\n📊 文件统计:')
print(f'  总行数: {len(df)}')
print(f'  CZM工序 (1303): {len(df[df["ERPCode"] == 1303])}行')
print(f'  CKH工序 (9997): {len(df[df["ERPCode"] == 9997])}行')
print(f'  唯一Area数量: {len(unique_areas_after)}')

# 显示各Area的工序数量分布
print(f'\n📋 Area工序分布:')
area_counts = df['Area'].value_counts().sort_index()
for area, count in area_counts.items():
    print(f'  {area}: {count}个工序')
