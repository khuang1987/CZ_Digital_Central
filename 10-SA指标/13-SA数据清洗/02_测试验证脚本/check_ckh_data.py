import pandas as pd

def check_mes_data_completeness():
    """检查MES数据中CZM和CKH的完整性"""
    
    # 读取最新的MES数据
    mes_path = r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish\MES_batch_report_latest.parquet'
    df = pd.read_parquet(mes_path)
    
    print('=== 原始MES数据分析 ===')
    print(f'总记录数: {len(df):,}')
    
    print(f'\n=== 工厂分布 ===')
    factory_counts = df['factory_source'].value_counts()
    for factory, count in factory_counts.items():
        print(f'  {factory}: {count:,} 条 ({count/len(df)*100:.1f}%)')
    
    print(f'\n=== ERPCode分布 ===')
    erp_counts = df['ERPCode'].value_counts()
    for erp, count in erp_counts.items():
        print(f'  ERPCode {erp}: {count:,} 条')
    
    print(f'\n=== CZM工厂详细信息 ===')
    czm_df = df[df['factory_source'] == 'CZM']
    print(f'CZM记录数: {len(czm_df):,}')
    print(f'唯一工序数: {czm_df["Operation description"].nunique()}')
    if 'TrackOutDate' in czm_df.columns:
        print(f'时间范围: {czm_df["TrackOutDate"].min()} 到 {czm_df["TrackOutDate"].max()}')
    
    print(f'\n=== CKH工厂详细信息 ===')
    ckh_df = df[df['factory_source'] == 'CKH']
    print(f'CKH记录数: {len(ckh_df):,}')
    print(f'唯一工序数: {ckh_df["Operation description"].nunique()}')
    if 'TrackOutDate' in ckh_df.columns:
        print(f'时间范围: {ckh_df["TrackOutDate"].min()} 到 {ckh_df["TrackOutDate"].max()}')
    
    # 检查CKH工序名称
    print(f'\n=== CKH工序列表 ===')
    ckh_operations = ckh_df['Operation description'].value_counts()
    for op, count in ckh_operations.items():
        print(f'  {op}: {count:,} 条')
    
    # 检查是否有空值或异常
    print(f'\n=== 数据质量检查 ===')
    print(f'CKH空值统计:')
    ckh_nulls = ckh_df.isnull().sum()
    for col, null_count in ckh_nulls.items():
        if null_count > 0:
            print(f'  {col}: {null_count} 个空值')
    
    return df, czm_df, ckh_df

if __name__ == "__main__":
    df, czm_df, ckh_df = check_mes_data_completeness()
