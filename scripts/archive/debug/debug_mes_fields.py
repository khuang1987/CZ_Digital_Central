"""
调试MES数据字段问题
"""

import os
import sys
import pandas as pd
import logging

# 添加核心ETL程序目录到路径
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '01_核心ETL程序'))
from etl_utils import load_config

def debug_mes_fields():
    """调试MES数据字段问题"""
    
    # 加载配置
    cfg = load_config(os.path.join(os.path.dirname(__file__), '..', '03_配置文件', 'config', 'config_mes_batch_report.yaml'))
    
    # 读取MES数据
    mes_path = cfg.get("source", {}).get("mes_path", "")
    print(f" MES文件路径: {mes_path}")
    
    try:
        mes_df = pd.read_excel(mes_path)
        print(f" MES数据行数: {len(mes_df)}")
        print(f" MES数据列数: {len(mes_df.columns)}")
        
        print(f"\n MES数据所有列名:")
        for i, col in enumerate(mes_df.columns):
            print(f"{i+1:2d}. '{col}'")
        
        # 检查映射配置中的源字段是否存在
        mapping = cfg.get("mes_mapping", {})
        print(f"\n 字段映射检查:")
        for source_field, target_field in mapping.items():
            if source_field in mes_df.columns:
                print(f"✓ '{source_field}' -> '{target_field}' (存在)")
            else:
                print(f"✗ '{source_field}' -> '{target_field}' (不存在)")
        
        # 查找可能的批次号字段
        batch_candidates = []
        for col in mes_df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['batch', 'lot', 'material', 'product', 'name']):
                batch_candidates.append(col)
        
        print(f"\n 可能的批次号字段:")
        for col in batch_candidates:
            unique_count = mes_df[col].nunique()
            sample_values = list(mes_df[col].dropna().unique()[:3])
            print(f"  '{col}': {unique_count}个唯一值, 示例: {sample_values}")
        
        # 查找可能的工序字段
        operation_candidates = []
        for col in mes_df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['operation', 'step', 'erp']):
                operation_candidates.append(col)
        
        print(f"\n 可能的工序字段:")
        for col in operation_candidates:
            unique_count = mes_df[col].nunique()
            sample_values = list(mes_df[col].dropna().unique()[:5])
            print(f"  '{col}': {unique_count}个唯一值, 示例: {sample_values}")
        
        return mes_df
        
    except Exception as e:
        print(f"读取 MES数据失败: {e}")
        return None

def simulate_field_mapping(mes_df):
    """模拟字段映射过程"""
    
    cfg = load_config(os.path.join(os.path.dirname(__file__), '..', '03_配置文件', 'config', 'config_mes_batch_report.yaml'))
    
    print(f"\n 模拟字段映射过程:")
    print(f"原始列名: {list(mes_df.columns)}")
    
    # 清理列名空格
    mes_df.columns = mes_df.columns.str.strip()
    print(f"清理空格后: {list(mes_df.columns)}")
    
    # 字段映射
    column_mapping = cfg.get("mes_mapping", {})
    column_mapping = {k: v for k, v in column_mapping.items() if v and not str(v).startswith('#')}
    
    if column_mapping:
        # 检查哪些映射可以应用
        valid_mapping = {}
        for source_field, target_field in column_mapping.items():
            if source_field in mes_df.columns:
                valid_mapping[source_field] = target_field
        
        print(f"有效映射: {valid_mapping}")
        
        if valid_mapping:
            mapped_df = mes_df.rename(columns=valid_mapping)
            print(f"映射后列名: {list(mapped_df.columns)}")
            
            # 检查关键字段
            key_fields = ["BatchNumber", "Operation", "CFN", "Group"]
            print(f"\n 关键字段检查:")
            for field in key_fields:
                if field in mapped_df.columns:
                    unique_count = mapped_df[field].nunique()
                    sample_values = list(mapped_df[field].dropna().unique()[:3])
                    print(f"✓ {field}: {unique_count}个唯一值, 示例: {sample_values}")
                else:
                    print(f"✗ {field}: 字段不存在")
            
            return mapped_df
        else:
            print("没有有效的字段映射")
            return mes_df
    else:
        print("字段映射配置为空")
        return mes_df

if __name__ == "__main__":
    print("调试MES数据字段问题")
    print("="*50)
    
    mes_df = debug_mes_fields()
    
    if mes_df is not None:
        print("\n" + "="*80 + "\n")
        simulate_field_mapping(mes_df)
