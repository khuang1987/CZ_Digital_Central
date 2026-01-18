"""
调试数据匹配问题
检查MES和SFC数据的匹配情况
"""

import os
import sys
import pandas as pd
import logging

# 添加核心ETL程序目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '01_核心ETL程序'))
from etl_utils import load_config

def debug_sfc_mes_matching():
    """调试SFC和MES数据匹配问题"""
    
    # 加载配置
    cfg = load_config(os.path.join(os.path.dirname(__file__), '..', '03_配置文件', 'config', 'config_mes_batch_report.yaml'))
    
    # 读取SFC数据
    sfc_latest_file = cfg.get("source", {}).get("sfc_latest_file", "")
    print(f"SFC文件路径: {sfc_latest_file}")
    
    if os.path.exists(sfc_latest_file):
        sfc_df = pd.read_parquet(sfc_latest_file)
        print(f"SFC数据行数: {len(sfc_df)}")
        print(f"SFC数据列: {list(sfc_df.columns)}")
        
        if 'BatchNumber' in sfc_df.columns and 'Operation' in sfc_df.columns:
            print(f"SFC数据BatchNumber范围: {sfc_df['BatchNumber'].min()} - {sfc_df['BatchNumber'].max()}")
            print(f"SFC数据Operation唯一值数量: {sfc_df['Operation'].nunique()}")
            print(f"SFC数据Operation前10个: {list(sfc_df['Operation'].unique()[:10])}")
            
            # 检查Checkin_SFC字段
            if 'Checkin_SFC' in sfc_df.columns:
                print(f"SFC数据Checkin_SFC非空数量: {sfc_df['Checkin_SFC'].notna().sum()}")
            elif 'CheckInTime' in sfc_df.columns:
                print(f"SFC数据CheckInTime非空数量: {sfc_df['CheckInTime'].notna().sum()}")
            else:
                print("SFC数据中既没有Checkin_SFC也没有CheckInTime字段")
        else:
            print("SFC数据缺少BatchNumber或Operation字段")
    else:
        print(f"SFC文件不存在: {sfc_latest_file}")
        return
    
    print("\n" + "="*50 + "\n")
    
    # 读取MES数据
    mes_path = cfg.get("source", {}).get("mes_path", "")
    print(f" MES文件路径: {mes_path}")
    
    if os.path.exists(mes_path):
        mes_df = pd.read_excel(mes_path)
        print(f" MES数据行数: {len(mes_df)}")
        print(f" MES数据列: {list(mes_df.columns)}")
        
        if 'BatchNumber' in mes_df.columns and 'Operation' in mes_df.columns:
            print(f" MES数据BatchNumber范围: {mes_df['BatchNumber'].min()} - {mes_df['BatchNumber'].max()}")
            print(f" MES数据Operation唯一值数量: {mes_df['Operation'].nunique()}")
            print(f" MES数据Operation前10个: {list(mes_df['Operation'].unique()[:10])}")
        else:
            print(" MES数据缺少BatchNumber或Operation字段")
    else:
        print(f" MES文件不存在: {mes_path}")
        return
    
    print("\n" + "="*50 + "\n")
    
    # 检查匹配情况
    if os.path.exists(sfc_latest_file) and os.path.exists(mes_path):
        sfc_df = pd.read_parquet(sfc_latest_file)
        mes_df = pd.read_excel(mes_path)
        
        # 创建匹配键
        sfc_keys = set(zip(sfc_df['BatchNumber'], sfc_df['Operation']))
        mes_keys = set(zip(mes_df['BatchNumber'], mes_df['Operation']))
        
        print(f"SFC数据匹配键数量: {len(sfc_keys)}")
        print(f" MES数据匹配键数量: {len(mes_keys)}")
        
        # 计算交集
        common_keys = sfc_keys.intersection(mes_keys)
        print(f"共同匹配键数量: {len(common_keys)}")
        print(f"匹配率: {len(common_keys) / len(mes_keys) * 100:.1f}%")
        
        # 显示一些不匹配的例子
        mes_only_keys = mes_keys - sfc_keys
        sfc_only_keys = sfc_keys - mes_keys
        
        print(f"\n仅在 MES中的匹配键（前5个）:")
        for key in list(mes_only_keys)[:5]:
            print(f"  BatchNumber: {key[0]}, Operation: {key[1]}")
        
        print(f"\n仅在 SFC中的匹配键（前5个）:")
        for key in list(sfc_only_keys)[:5]:
            print(f"  BatchNumber: {key[0]}, Operation: {key[1]}")

def debug_standard_time_matching():
    """调试标准时间表匹配问题"""
    
    # 加载配置
    cfg = load_config(os.path.join(os.path.dirname(__file__), '..', '03_配置文件', 'config', 'config_mes_batch_report.yaml'))
    
    # 读取标准时间表
    std_time_path = cfg.get("source", {}).get("standard_time_path", "")
    print(f"标准时间表路径: {std_time_path}")
    
    if os.path.exists(std_time_path):
        std_df = pd.read_parquet(std_time_path)
        print(f"标准时间表行数: {len(std_df)}")
        print(f"标准时间表列: {list(std_df.columns)}")
        
        if 'CFN' in std_df.columns and 'Operation' in std_df.columns:
            print(f"标准时间表CFN唯一值数量: {std_df['CFN'].nunique()}")
            print(f"标准时间表Operation唯一值数量: {std_df['Operation'].nunique()}")
            print(f"标准时间表CFN前5个: {list(std_df['CFN'].unique()[:5])}")
            print(f"标准时间表Operation前10个: {list(std_df['Operation'].unique()[:10])}")
    else:
        print(f"标准时间表文件不存在: {std_time_path}")

if __name__ == "__main__":
    print("调试数据匹配问题")
    print("="*50)
    
    debug_sfc_mes_matching()
    print("\n" + "="*80 + "\n")
    debug_standard_time_matching()
