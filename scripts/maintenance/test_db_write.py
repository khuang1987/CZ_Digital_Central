"""Maintenance/Test: Database write smoke test.

Purpose:
  Quick sanity check that the database write path works.
  Reads an existing MES parquet file and writes a small sample to the configured DB manager.

Scope / Effects:
  - Reads: data_pipelines/sources/mes/publish/MES_batch_report_latest.parquet
  - Writes: depends on get_default_db_manager() configuration.
    (Historically used to validate dual-write behavior.)

Run:
  python scripts/maintenance/test_db_write.py
"""

import os
import sys

# 添加项目根目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

import pandas as pd
from shared_infrastructure.utils.db_utils import get_default_db_manager

def test_db_write():
    """测试数据库写入"""
    
    # 1. 读取现有的 Parquet 文件
    parquet_path = os.path.join(
        project_root, 
        "data_pipelines", "sources", "mes", "publish", 
        "MES_batch_report_latest.parquet"
    )
    
    if not os.path.exists(parquet_path):
        print(f"Parquet 文件不存在: {parquet_path}")
        return
    
    print(f"读取 Parquet 文件: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"总记录数: {len(df)}")
    
    # 2. 只取前1000条测试
    test_df = df.head(1000).copy()
    print(f"测试记录数: {len(test_df)}")
    
    # 3. 准备数据库字段映射
    column_mapping = {
        'BatchNumber': 'batch_number',
        'Operation': 'operation',
        'machine': 'machine',
        'TrackOutTime': 'track_out_time',
        'factory_source': 'factory_source',
        'factory_name': 'factory_name',
        'ProductCode': 'product_code',
        'ProductName': 'product_name',
        'ProductGroup': 'product_group',
        'TrackInTime': 'track_in_time',
        'TrackOutDate': 'track_out_date',
        'PreviousBatchEndTime': 'previous_batch_end_time',
        'LT(d)': 'lt_days',
        'PT(d)': 'pt_days',
        'ST(d)': 'st_days',
        'OEE': 'oee',
        'DueTime': 'due_time',
        'NonWorkday(d)': 'nonworkday_days',
        'Tolerance(h)': 'tolerance_hours',
        'CompletionStatus': 'completion_status',
        'QtyIn': 'qty_in',
        'QtyOut': 'qty_out',
        'QtyScrap': 'qty_scrap',
        'Operator': 'operator',
        'Machine(#)': 'machine_number',
        'source_file': 'source_file',
    }
    
    # 4. 筛选存在的列并重命名
    existing_cols = [c for c in column_mapping.keys() if c in test_df.columns]
    db_df = test_df[existing_cols].copy()
    db_df = db_df.rename(columns={k: v for k, v in column_mapping.items() if k in existing_cols})
    
    print(f"数据库列: {list(db_df.columns)}")
    
    # 5. 生成 record_hash
    key_fields = ['batch_number', 'operation', 'machine', 'track_out_time']
    available_keys = [k for k in key_fields if k in db_df.columns]
    if available_keys:
        hash_df = db_df[available_keys].fillna('').astype(str)
        record_hash = hash_df.iloc[:, 0]
        for col in hash_df.columns[1:]:
            record_hash = record_hash + '|' + hash_df[col]
        db_df['record_hash'] = record_hash
    
    # 6. 写入数据库
    print("开始写入数据库...")
    db = get_default_db_manager()
    
    # 使用 bulk_insert 替换模式
    inserted_count = db.bulk_insert(db_df, 'mes_batch_report', if_exists='replace')
    print(f"写入完成: {inserted_count} 条记录")
    
    # 7. 验证
    count = db.get_table_count('mes_batch_report')
    print(f"数据库中记录数: {count}")
    
    # 8. 查询前5条数据
    sample = db.execute_query("SELECT batch_number, operation, machine, track_out_time FROM mes_batch_report LIMIT 5")
    print("\n前5条数据:")
    for row in sample:
        print(f"  {row}")
    
    print("\n测试完成!")


if __name__ == "__main__":
    test_db_write()
