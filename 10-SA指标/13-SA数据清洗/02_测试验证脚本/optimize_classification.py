import pandas as pd
import re

# 读取重新排序的CSV文件
input_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序管理区域分类_重新排序.csv'
df = pd.read_csv(input_path, encoding='utf-8-sig')

print('原始列名:')
print(list(df.columns))

# 1. 更新列B的标题为英文
df.columns = ['No', 'Operation Name', 'ST Routing', 'Area', 'LT', 'ERPCode']
print('\n更新后的列名:')
print(list(df.columns))

# 2. 分析所有Area字段，找出没有英文缩写的
unique_areas = df['Area'].dropna().unique()
print(f'\n所有唯一的Area值 ({len(unique_areas)}个):')
for area in sorted(unique_areas):
    print(f'  {area}')

# 识别没有英文缩写的Area（格式：中文名称 英文缩写）
areas_needing_abbreviation = []
for area in unique_areas:
    if pd.notna(area) and not re.search(r'[A-Z]{2,4}$', str(area).strip()):
        areas_needing_abbreviation.append(area)

print(f'\n需要添加英文缩写的Area ({len(areas_needing_abbreviation)}个):')
for area in areas_needing_abbreviation:
    print(f'  {area}')

# 定义缩写映射
abbreviation_mapping = {
    '线切割': '线切割 WED',  # Wire EDM
    '车削&磨削': '车削&磨削 TG',  # Turning & Grinding (already has TG in some, but standardize)
    '后处理': '后处理 EOL',  # End of Line
    '纵切': '纵切 ST',  # Slitting Turning
    '加工中心': '加工中心 MCT',  # Machining Center
    '无区域': '无区域 NA',  # No Area
    '外协': '外协 OS',  # Outsource
}

print(f'\n应用缩写映射...')
# 应用缩写映射
df['Area'] = df['Area'].replace(abbreviation_mapping)

# 验证修改结果
updated_areas = df['Area'].dropna().unique()
print(f'\n更新后的Area值 ({len(updated_areas)}个):')
for area in sorted(updated_areas):
    print(f'  {area}')

# 保存优化后的文件
output_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序管理区域分类_优化版.csv'
df.to_csv(output_path, index=False, encoding='utf-8-sig')

print(f'\n优化后的CSV文件已保存至: {output_path}')

# 显示修改统计
changes_count = len(areas_needing_abbreviation)
print(f'\n修改统计:')
print(f'- 列标题更新: 1项')
print(f'- Area缩写添加: {changes_count}项')
print(f'- 总计修改: {changes_count + 1}项')
