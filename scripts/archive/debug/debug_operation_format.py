"""
调试Operation字段格式问题
"""

import os
import sys
import pandas as pd
import logging

# 添加核心ETL程序目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '01_核心ETL程序'))
from etl_utils import load_config

def debug_operation_format():
    """调试Operation字段格式问题"""
    
    # 加载配置
    cfg = load_config(os.path.join(os.path.dirname(__file__), '..', '03_配置文件', 'config', 'config_mes_batch_report.yaml'))
    
    print("调试Operation字段格式问题")
    print("="*50)
    
    # 读取SFC数据
    sfc_latest_file = cfg.get("source", {}).get("sfc_latest_file", "")
    if os.path.exists(sfc_latest_file):
        sfc_df = pd.read_parquet(sfc_latest_file)
        print(f"SFC数据Operation字段:")
        print(f"  数据类型: {sfc_df['Operation'].dtype}")
        print(f"  唯一值数量: {sfc_df['Operation'].nunique()}")
        print(f"  唯一值: {sorted([op for op in sfc_df['Operation'].unique() if op is not None])}")
        
        # 检查是否有数值型数据
        sfc_operations = sfc_df['Operation'].dropna().unique()
        sfc_str_ops = [str(op) for op in sfc_operations if op is not None]
        print(f"  转为字符串后: {sorted(sfc_str_ops)}")
    
    print("\n" + "-"*50 + "\n")
    
    # 读取MES数据
    mes_path = cfg.get("source", {}).get("mes_path", "")
    if os.path.exists(mes_path):
        mes_df = pd.read_excel(mes_path)
        
        # 应用字段映射
        column_mapping = cfg.get("mes_mapping", {})
        column_mapping = {k: v for k, v in column_mapping.items() if v and not str(v).startswith('#')}
        mes_df = mes_df.rename(columns=column_mapping)
        
        print(f" MES数据Operation字段:")
        print(f"  数据类型: {mes_df['Operation'].dtype}")
        print(f"  唯一值数量: {mes_df['Operation'].nunique()}")
        print(f"  唯一值: {sorted(mes_df['Operation'].dropna().unique())}")
        
        # 检查转换为4位字符串后的结果
        mes_operations = mes_df['Operation'].dropna().unique()
        mes_str_ops = []
        for op in mes_operations:
            try:
                op_num = float(op)
                mes_str_ops.append(f"{int(op_num):04d}")
            except (ValueError, TypeError):
                mes_str_ops.append(str(op).strip().zfill(4) if str(op).strip() else None)
        
        print(f"  转为4位字符串后: {sorted(set(mes_str_ops))}")
    
    print("\n" + "-"*50 + "\n")
    
    # 检查匹配情况
    if os.path.exists(sfc_latest_file) and os.path.exists(mes_path):
        sfc_df = pd.read_parquet(sfc_latest_file)
        mes_df = pd.read_excel(mes_path)
        mes_df = mes_df.rename(columns=column_mapping)
        
        # 标准化Operation字段
        sfc_df['Operation_std'] = sfc_df['Operation'].astype(str)
        
        mes_ops_std = []
        for op in mes_df['Operation']:
            try:
                op_num = float(op)
                mes_ops_std.append(f"{int(op_num):04d}")
            except (ValueError, TypeError):
                mes_ops_std.append(str(op).strip().zfill(4) if str(op).strip() else None)
        
        mes_df['Operation_std'] = mes_ops_std
        
        print("标准化后的匹配检查:")
        sfc_ops_set = set(sfc_df['Operation_std'].unique())
        mes_ops_set = set(mes_df['Operation_std'].unique())
        
        print(f"SFC标准化Operation: {sorted(sfc_ops_set)}")
        print(f" MES标准化Operation: {sorted(mes_ops_set)}")
        
        common_ops = sfc_ops_set.intersection(mes_ops_set)
        print(f"共同Operation: {sorted(common_ops)}")
        print(f"匹配率: {len(common_ops) / len(mes_ops_set) * 100:.1f}%")

if __name__ == "__main__":
    debug_operation_format()
