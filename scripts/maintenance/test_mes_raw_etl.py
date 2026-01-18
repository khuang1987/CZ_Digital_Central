"""Maintenance/Test: MES raw ETL (V2) smoke test.

Purpose:
  Test the MES raw import pipeline with a small sample (first 1000 rows)
  from an existing parquet file.

Scope / Effects:
  - Reads: data_pipelines/sources/mes/publish/MES_batch_report_latest.parquet
  - Writes: uses DatabaseManager (SQLite) to validate schema/mapping and record_hash behavior.

Run:
  python scripts/maintenance/test_mes_raw_etl.py
"""

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)

import pandas as pd
from shared_infrastructure.utils.db_utils import DatabaseManager

DB_PATH = os.path.join(project_root, "data_pipelines", "database", "mddap_v2.db")
PARQUET_PATH = os.path.join(project_root, "data_pipelines", "sources", "mes", "publish", "MES_batch_report_latest.parquet")


def test_import():
    """从现有 Parquet 导入1000条测试数据"""
    
    print("=" * 60)
    print("测试 MES 原始数据导入 (V2)")
    print("=" * 60)
    
    # 1. 读取现有数据
    print(f"\n读取 Parquet: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"总记录数: {len(df)}")
    
    # 2. 取1000条测试
    test_df = df.head(1000).copy()
    print(f"测试记录数: {len(test_df)}")
    
    # 3. 映射到原始表结构
    column_mapping = {
        'BatchNumber': 'BatchNumber',
        'Operation': 'Operation',
        'machine': 'machine',
        'EnterStepTime': 'EnterStepTime',
        'TrackInTime': 'TrackInTime',
        'TrackOutTime': 'TrackOutTime',
        'ProductCode': 'ProductCode',
        'ProductName': 'ProductName',
        'ProductGroup': 'ProductGroup',
        'CFN': 'CFN',
        'ProductionOrder': 'ProductionOrder',
        'Operation description': 'OperationDescription',
        'Group': 'Group',
        'StepInQuantity': 'StepInQuantity',
        'TrackOutQuantity': 'TrackOutQuantity',
        'Operator': 'Operator',
        'Setup': 'Setup',
        'Setup Time (h)': 'SetupTime',
        'OEE': 'OEE',
        'EH_machine(s)': 'EH_machine',
        'EH_labor(s)': 'EH_labor',
        'factory_source': 'factory_source',
        'factory_name': 'factory_name',
        'VSM': 'VSM',
        'ERPCode': 'ERPCode',
        'Product_Description': 'Product_Description',
    }
    
    # 重命名列
    rename_dict = {k: v for k, v in column_mapping.items() if k in test_df.columns}
    raw_df = test_df.rename(columns=rename_dict)
    
    # 生成 record_hash
    key_fields = ['BatchNumber', 'Operation', 'machine', 'TrackOutTime']
    available_keys = [k for k in key_fields if k in raw_df.columns]
    hash_df = raw_df[available_keys].fillna('').astype(str)
    record_hash = hash_df.iloc[:, 0]
    for col in hash_df.columns[1:]:
        record_hash = record_hash + '|' + hash_df[col]
    raw_df['record_hash'] = record_hash
    
    # 选择目标列
    target_columns = [
        'BatchNumber', 'Operation', 'machine',
        'EnterStepTime', 'TrackInTime', 'TrackOutTime',
        'ProductCode', 'ProductName', 'ProductGroup', 'CFN', 'ProductionOrder',
        'OperationDescription', 'Group',
        'StepInQuantity', 'TrackOutQuantity',
        'Operator',
        'Setup', 'SetupTime', 'OEE', 'EH_machine', 'EH_labor',
        'factory_source', 'factory_name',
        'VSM', 'ERPCode', 'Product_Description',
        'record_hash'
    ]
    existing_cols = [c for c in target_columns if c in raw_df.columns]
    raw_df = raw_df[existing_cols]
    
    print(f"目标列: {len(existing_cols)} 列")
    
    # 4. 写入数据库（使用 append 模式保持表结构）
    print("\n写入数据库...")
    db = DatabaseManager(DB_PATH)
    inserted = db.bulk_insert(raw_df, 'raw_mes', if_exists='append')
    print(f"写入完成: {inserted} 条")
    
    # 5. 验证原始表
    count = db.get_table_count('raw_mes')
    print(f"\nraw_mes 表记录数: {count}")
    
    # 6. 测试视图查询
    print("\n测试视图 v_mes_metrics...")
    try:
        result = db.execute_query("""
            SELECT 
                BatchNumber, 
                Operation, 
                machine, 
                TrackOutTime,
                PreviousBatchEndTime,
                "LT(d)",
                "PT(d)"
            FROM v_mes_metrics 
            LIMIT 5
        """)
        
        print("视图查询结果（前5条）:")
        for row in result:
            print(f"  批次: {row['BatchNumber']}, 机台: {row['machine']}")
            print(f"    TrackOutTime: {row['TrackOutTime']}")
            print(f"    PreviousBatchEndTime: {row['PreviousBatchEndTime']}")
            print(f"    LT(d): {row['LT(d)']}, PT(d): {row['PT(d)']}")
            print()
            
    except Exception as e:
        print(f"视图查询失败: {e}")
    
    print("=" * 60)
    print("测试完成!")
    print("=" * 60)


if __name__ == "__main__":
    test_import()
