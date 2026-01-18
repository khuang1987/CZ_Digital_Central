import pandas as pd

df = pd.read_excel(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\Product Output -CZM -FY26.xlsx')
print('MES数据列名:')
for i, col in enumerate(df.columns):
    print(f'{i+1:2d}. {col}')

print(f'\n数据行数: {len(df)}')

# 查找可能的批次号字段
batch_fields = [col for col in df.columns if 'batch' in col.lower() or 'lot' in col.lower() or '批' in col]
print(f'\n可能的批次字段: {batch_fields}')

# 查找可能的工序字段
operation_fields = [col for col in df.columns if 'operation' in col.lower() or 'step' in col.lower() or '工序' in col]
print(f'可能的工序字段: {operation_fields}')
