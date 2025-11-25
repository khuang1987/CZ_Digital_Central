import pandas as pd

# 读取现有的工序标准分类表
file_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序标准分类.xlsx'

# 读取工序名称工作表
df_existing = pd.read_excel(file_path, sheet_name='工序名称')
print('现有CZM工序数据（ERPCode 1303）:')
print(f'行数: {len(df_existing)}')

# 读取MES数据获取CKH工序
mes_df = pd.read_parquet(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish\MES_batch_report_latest.parquet')
ckh_operations = sorted(mes_df[mes_df['factory_source'] == 'CKH']['Operation description'].dropna().unique())

print(f'CKH工序数量: {len(ckh_operations)}')

# 创建CKH工序数据，ERPCode设为9997
next_no = df_existing['No'].max() + 1 if not df_existing['No'].isna().all() else 1
ckh_rows = []
for i, op in enumerate(ckh_operations):
    ckh_rows.append({
        'No': next_no + i,
        'SAP Routing': op,
        'ST Routing': '',
        'Area': '',
        'LT': '',
        'ERPCode': 9997  # 修正为9997（CKH工厂）
    })

ckh_df = pd.DataFrame(ckh_rows)

# 合并现有数据和CKH数据
unified_df = pd.concat([df_existing, ckh_df], ignore_index=True)

print(f'\n合并后总行数: {len(unified_df)}')
czm_count = len(unified_df[unified_df['ERPCode'] == 1303])
ckh_count = len(unified_df[unified_df['ERPCode'] == 9997])
print(f'CZM工序 (ERPCode 1303): {czm_count} 行')
print(f'CKH工序 (ERPCode 9997): {ckh_count} 行')

print('\nCKH工序示例:')
ckh_sample = unified_df[unified_df['ERPCode'] == 9997].head(5)
print(ckh_sample[['No', 'SAP Routing', 'ERPCode']].to_string(index=False))

# 输出为CSV
output_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\Huangkai Files\B1_Project\250418_MDDAP_project\10-SA指标\11数据模板\工序管理区域分类_完整版.csv'
unified_df.to_csv(output_path, index=False, encoding='utf-8-sig')
print(f'\n修正后的CSV文件已保存至: {output_path}')
