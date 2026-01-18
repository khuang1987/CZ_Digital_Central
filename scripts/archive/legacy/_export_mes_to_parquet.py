"""导出修正后的 MES 数据到 Parquet 文件（供 Power BI 使用）"""
import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

if __name__ == '__main__':
    print("_export_mes_to_parquet.py 已弃用。请使用统一导出脚本 export_core_to_a1.py（SQL Server only）。")
    print("示例：python .\\scripts\\export_core_to_a1.py --mode all --months all")
    sys.exit(1)

# 配置
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=localhost\SQLEXPRESS;"
    "DATABASE=mddap_v2;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

# 输出目录 - 直接导出到 Power BI 数据源目录
output_dir = Path(r"c:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\A1_ETL_Output\02_CURATED_PARTITIONED\mes_batch_report")
output_dir.mkdir(parents=True, exist_ok=True)

print("=" * 100)
print("导出 MES 数据到 Parquet 文件")
print("=" * 100)

with pyodbc.connect(conn_str) as conn:
    
    # 1. 导出完整的 v_mes_metrics 视图
    print("\n" + "=" * 100)
    print("1. 导出 v_mes_metrics 完整数据")
    print("=" * 100)
    
    query = """
        SELECT *
        FROM dbo.v_mes_metrics
        ORDER BY id
    """
    
    print(f"\n读取数据...")
    df = pd.read_sql(query, conn)
    
    print(f"✓ 读取完成: {len(df):,} 条记录")
    print(f"  列数: {len(df.columns)}")
    
    # 2. 导出按月分区的数据（便于 Power BI 增量刷新）
    print("\n" + "=" * 100)
    print("2. 导出按月分区的数据")
    print("=" * 100)
    
    # 创建分区目录
    partition_dir = output_dir
    partition_dir.mkdir(parents=True, exist_ok=True)
    
    # 按月分组
    df['YearMonth'] = pd.to_datetime(df['TrackOutDate']).dt.to_period('M')
    
    for period, group_df in df.groupby('YearMonth'):
        if pd.isna(period):
            continue
        
        year_month = str(period)  # 格式: 2025-01
        year = year_month[:4]
        yyyymm = year_month.replace('-', '')
        partition_year_dir = partition_dir / year
        partition_year_dir.mkdir(parents=True, exist_ok=True)
        partition_file = partition_year_dir / f"mes_metrics_{yyyymm}.parquet"
        
        # 删除临时列
        group_df_clean = group_df.drop(columns=['YearMonth'])
        
        group_df_clean.to_parquet(partition_file, index=False, engine='pyarrow', compression='snappy')
        
        file_size_kb = partition_file.stat().st_size / 1024
        print(f"  ✓ {year_month}: {len(group_df):,} 条记录, {file_size_kb:.1f} KB")
    
    # 3. 导出汇总数据（跳过，Power BI 中直接聚合）
    print("\n" + "=" * 100)
    print("3. 导出汇总数据 - 已跳过（Power BI 中直接聚合）")
    print("=" * 100)
    
    # 4. 数据质量报告
    print("\n" + "=" * 100)
    print("4. 数据质量报告")
    print("=" * 100)
    
    print(f"\n总记录数: {len(df):,}")
    print(f"\n按状态分布:")
    status_counts = df['CompletionStatus'].value_counts()
    for status, count in status_counts.items():
        print(f"  {status:15s}: {count:8,} ({count/len(df)*100:5.2f}%)")
    
    print(f"\n按工厂分布:")
    factory_counts = df['factory_name'].value_counts()
    for factory, count in factory_counts.items():
        print(f"  {factory:15s}: {count:8,} ({count/len(df)*100:5.2f}%)")
    
    print(f"\n按月份分布:")
    month_counts = df['YearMonth'].value_counts().sort_index()
    for month, count in month_counts.items():
        if pd.notna(month):
            print(f"  {str(month):10s}: {count:8,}")
    
    # 5. 关键指标
    print("\n" + "=" * 100)
    print("5. 关键指标")
    print("=" * 100)
    
    valid_records = df[df['CompletionStatus'].isin(['OnTime', 'Overdue'])]
    ontime_records = df[df['CompletionStatus'] == 'OnTime']
    
    sa_rate = len(ontime_records) / len(valid_records) * 100 if len(valid_records) > 0 else 0
    
    print(f"\n✓ SA 达成率: {sa_rate:.2f}%")
    print(f"✓ 有效记录数: {len(valid_records):,} / {len(df):,} ({len(valid_records)/len(df)*100:.2f}%)")
    
    # 计算平均值（排除 NULL）
    avg_pt = df['PT(d)'].mean()
    avg_st = df['ST(d)'].mean()
    avg_lt = df['LT(d)'].mean()
    
    print(f"\n平均指标:")
    print(f"  PT(d): {avg_pt:.4f} 天 ({avg_pt*24:.2f} 小时)")
    print(f"  ST(d): {avg_st:.4f} 天 ({avg_st*24:.2f} 小时)")
    print(f"  LT(d): {avg_lt:.4f} 天 ({avg_lt*24:.2f} 小时)")
    
    # PT/ST 比率
    df_with_ratio = df[(df['PT(d)'].notna()) & (df['ST(d)'].notna()) & (df['ST(d)'] > 0)]
    if len(df_with_ratio) > 0:
        df_with_ratio['PT_ST_ratio'] = df_with_ratio['PT(d)'] / df_with_ratio['ST(d)']
        avg_ratio = df_with_ratio['PT_ST_ratio'].mean()
        print(f"  PT/ST 比率: {avg_ratio:.2f}")

print("\n" + "=" * 100)
print("导出完成")
print("=" * 100)

print(f"\n输出目录: {output_dir}")
print(f"\n文件清单:")
print(f"  1. YYYY/mes_metrics_YYYYMM.parquet - 按月分区数据")

print("\n" + "=" * 100)
print("Power BI 导入说明")
print("=" * 100)

print("""
1. 按月分区导入（推荐）:
   - 目录: YYYY/
   - 用途: 增量刷新、性能优化
   - 方法: 使用 Folder 数据源，合并所有 Parquet 文件

关键字段说明:
  - CompletionStatus: OnTime, Overdue, NoStandard, Abnormal, NoBaseline, NoQuantity
  - PT(d): 实际加工时间（天）
  - ST(d): 标准加工时间（天）
  - LT(d): 制造周期（天）
  - SA_rate: 达成率（仅在汇总表中）
""")

print("\n" + "=" * 100)
