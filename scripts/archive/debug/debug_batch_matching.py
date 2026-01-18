"""
调试BatchNumber匹配问题
"""

import os
import sys
import pandas as pd
import logging

# 添加核心ETL程序目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '01_核心ETL程序'))
from etl_utils import load_config
from etl_dataclean_mes_batch_report import process_mes_data

def debug_batch_matching():
    """调试BatchNumber匹配问题"""
    
    # 加载配置
    cfg = load_config(os.path.join(os.path.dirname(__file__), '..', '03_配置文件', 'config', 'config_mes_batch_report.yaml'))
    
    print("调试BatchNumber匹配问题")
    print("="*50)
    
    # 读取SFC数据
    sfc_latest_file = cfg.get("source", {}).get("sfc_latest_file", "")
    if os.path.exists(sfc_latest_file):
        sfc_df = pd.read_parquet(sfc_latest_file)
        print(f"SFC数据BatchNumber字段:")
        print(f"  数据类型: {sfc_df['BatchNumber'].dtype}")
        print(f"  唯一值数量: {sfc_df['BatchNumber'].nunique()}")
        print(f"  范围: {sfc_df['BatchNumber'].min()} - {sfc_df['BatchNumber'].max()}")
        print(f"  示例: {list(sfc_df['BatchNumber'].unique()[:10])}")
        
        sfc_batches = set(sfc_df['BatchNumber'].unique())
    
    print("\n" + "-"*50 + "\n")
    
    # 读取MES数据
    mes_path = cfg.get("source", {}).get("mes_path", "")
    if os.path.exists(mes_path):
        mes_df = pd.read_excel(mes_path)
        processed_mes = process_mes_data(mes_df, cfg)
        
        print(f" MES数据BatchNumber字段:")
        print(f"  数据类型: {processed_mes['BatchNumber'].dtype}")
        print(f"  唯一值数量: {processed_mes['BatchNumber'].nunique()}")
        print(f"  范围: {processed_mes['BatchNumber'].min()} - {processed_mes['BatchNumber'].max()}")
        print(f"  示例: {list(processed_mes['BatchNumber'].unique()[:10])}")
        
        mes_batches = set(processed_mes['BatchNumber'].unique())
    
    print("\n" + "-"*50 + "\n")
    
    # 检查BatchNumber匹配情况
    if 'sfc_batches' in locals() and 'mes_batches' in locals():
        common_batches = sfc_batches.intersection(mes_batches)
        sfc_only_batches = sfc_batches - mes_batches
        mes_only_batches = mes_batches - sfc_batches
        
        print(f"BatchNumber匹配统计:")
        print(f"  SFC唯一批次数: {len(sfc_batches)}")
        print(f"  MES唯一批次数: {len(mes_batches)}")
        print(f"  共同批次数: {len(common_batches)}")
        print(f"  匹配率: {len(common_batches) / len(mes_batches) * 100:.1f}%")
        
        print(f"\n仅在SFC中的批次（前5个）:")
        for batch in list(sfc_only_batches)[:5]:
            print(f"  {batch}")
        
        print(f"\n仅在 MES中的批次（前5个）:")
        for batch in list(mes_only_batches)[:5]:
            print(f"  {batch}")
        
        # 分析批次号格式
        print(f"\n批次号格式分析:")
        sfc_formats = {}
        for batch in list(sfc_batches)[:100]:  # 取样100个
            prefix = batch[:3] if len(batch) >= 3 else batch
            sfc_formats[prefix] = sfc_formats.get(prefix, 0) + 1
        
        mes_formats = {}
        for batch in list(mes_batches)[:100]:  # 取样100个
            prefix = batch[:3] if len(batch) >= 3 else batch
            mes_formats[prefix] = mes_formats.get(prefix, 0) + 1
        
        print(f"SFC批次号前缀分布: {dict(sorted(sfc_formats.items())[:10])}")
        print(f" MES批次号前缀分布: {dict(sorted(mes_formats.items())[:10])}")

if __name__ == "__main__":
    debug_batch_matching()
