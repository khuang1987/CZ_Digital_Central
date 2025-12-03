"""
重复数据检测脚本
分析输出数据中的重复模式，找出去重逻辑失效的原因
"""

import pandas as pd
import numpy as np

def detect_duplicates():
    """检测输出数据中的重复模式"""
    
    print("=" * 80)
    print("重复数据检测分析")
    print("=" * 80)
    
    # 1. 读取处理后的数据文件
    output_file = r"C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/SFC_Team_PassRate_latest.parquet"
    
    try:
        df = pd.read_parquet(output_file)
        print(f"✅ 成功读取输出文件: {len(df)} 条记录")
    except Exception as e:
        print(f"❌ 读取输出文件失败: {e}")
        return
    
    # 2. 显示基本信息
    print(f"\n📊 数据基本信息:")
    print(f"  总记录数: {len(df)}")
    print(f"  批次数: {df['批次号'].nunique()}")
    print(f"  班组数: {df['班组'].nunique()}")
    print(f"  工序数: {df['工序编号'].nunique()}")
    
    # 3. 检查基于当前业务键的重复
    print(f"\n🔍 检查当前业务键重复情况:")
    business_keys = ['批次号', '产品序号', '工序编号', '工序名称']
    
    # 检查业务键是否存在
    missing_keys = [key for key in business_keys if key not in df.columns]
    if missing_keys:
        print(f"❌ 缺少业务键字段: {missing_keys}")
        business_keys = [key for key in business_keys if key in df.columns]
    
    print(f"  使用的业务键: {business_keys}")
    
    # 统计业务键重复
    duplicate_groups = df.groupby(business_keys).size()
    duplicates = duplicate_groups[duplicate_groups > 1]
    
    if len(duplicates) == 0:
        print(f"✅ 基于业务键 {business_keys} 无重复数据")
    else:
        print(f"❌ 发现 {len(duplicates)} 组重复数据:")
        print(f"  重复记录总数: {duplicates.sum()}")
        print(f"  重复组数: {len(duplicates)}")
        
        # 显示前5组重复数据
        print(f"\n📋 重复数据示例 (前5组):")
        print("-" * 100)
        for i, (idx, count) in enumerate(duplicates.head().items()):
            if isinstance(idx, tuple):
                key_str = " | ".join([str(x) for x in idx])
            else:
                key_str = str(idx)
            print(f"  {i+1}. {key_str} -> {count} 条记录")
            
            # 显示该组的详细数据
            if len(business_keys) == 4:
                batch_num, product_num, operation_num, operation_name = idx
                group_data = df[
                    (df['批次号'] == batch_num) & 
                    (df['产品序号'] == product_num) & 
                    (df['工序编号'] == operation_num) & 
                    (df['工序名称'] == operation_name)
                ]
            else:
                # 处理字段不存在的情况
                group_data = df[df[business_keys[0]] == idx] if len(business_keys) == 1 else pd.DataFrame()
            
            if not group_data.empty:
                print(f"     详细记录:")
                for _, row in group_data.iterrows():
                    print(f"       班组: {row.get('班组', 'N/A')}, 合格数: {row.get('合格数', 'N/A')}, 不合格数: {row.get('不合格数', 'N/A')}, 批次合格率: {row.get('批次合格率', 'N/A')}")
            print()
    
    # 4. 检查不同维度的重复
    print(f"\n🔍 检查不同维度的重复情况:")
    
    # 检查批次+工序重复
    batch_op_duplicates = df.groupby(['批次号', '工序编号']).size()
    batch_op_dup_count = (batch_op_duplicates > 1).sum()
    print(f"  批次+工序重复组数: {batch_op_dup_count}")
    
    # 检查批次重复
    batch_duplicates = df.groupby('批次号').size()
    batch_dup_count = (batch_duplicates > 1).sum()
    print(f"  批次重复组数: {batch_dup_count}")
    
    # 5. 分析重复原因
    if len(duplicates) > 0:
        print(f"\n🔬 重复原因分析:")
        
        # 取一个重复组进行详细分析
        sample_duplicate_key = duplicates.index[0]
        sample_group = pd.DataFrame()
        
        if isinstance(sample_duplicate_key, tuple):
            if len(business_keys) == 3:  # 当前实际使用的业务键
                batch_num, operation_num, operation_name = sample_duplicate_key
                sample_group = df[
                    (df['批次号'] == batch_num) & 
                    (df['工序编号'] == operation_num) & 
                    (df['工序名称'] == operation_name)
                ]
        else:
            sample_group = df[df[business_keys[0]] == sample_duplicate_key]
        
        if sample_group.empty:
            print(f"  ❌ 无法获取样本重复组数据")
            return
        
        print(f"  样本重复组详细字段对比:")
        print("-" * 80)
        
        # 显示所有字段的值，找出差异
        for col in sample_group.columns:
            unique_values = sample_group[col].unique()
            if len(unique_values) > 1:
                print(f"    {col}: {unique_values}")
            else:
                print(f"    {col}: {unique_values[0]}")
        
        print(f"\n💡 可能的重复原因:")
        
        # 检查是否是班组不同导致的重复
        if '班组' in sample_group.columns and sample_group['班组'].nunique() > 1:
            print(f"  ❌ 同一业务键存在多个班组记录")
            print(f"  💡 建议: 将 '班组' 加入业务键进行去重")
        
        # 检查是否是产品号不同导致的重复
        if '产品号' in sample_group.columns and sample_group['产品号'].nunique() > 1:
            print(f"  ❌ 同一批次工序存在多个产品号")
            print(f"  💡 建议: 检查是否应该包含 '产品号' 在业务键中")
        
        # 检查是否是文件来源不同导致的重复
        if 'source_file' in sample_group.columns and sample_group['source_file'].nunique() > 1:
            print(f"  ❌ 同一业务键来自多个源文件")
            print(f"  💡 建议: 检查文件读取逻辑")
        
        # 检查是否是时间戳不同导致的重复
        if 'file_mod_time' in sample_group.columns and sample_group['file_mod_time'].nunique() > 1:
            print(f"  ❌ 同一业务键有多个修改时间")
            print(f"  💡 建议: 确认时间排序逻辑")
    
    # 6. 去重建议
    print(f"\n🛠️ 去重建议:")
    
    if len(duplicates) > 0:
        print(f"  1. 当前业务键: {business_keys}")
        print(f"  2. 发现重复: {len(duplicates)} 组")
        
        # 建议新的业务键
        suggested_keys = business_keys.copy()
        if '班组' in df.columns and '班组' not in suggested_keys:
            suggested_keys.append('班组')
        
        print(f"  3. 建议业务键: {suggested_keys}")
        
        # 测试新业务键的去重效果
        new_duplicate_groups = df.groupby(suggested_keys).size()
        new_duplicates = new_duplicate_groups[new_duplicate_groups > 1]
        
        print(f"  4. 新业务键重复数: {len(new_duplicates)} 组")
        
        if len(new_duplicates) == 0:
            print(f"  ✅ 新业务键可以完全消除重复")
        else:
            print(f"  ⚠️ 新业务键仍有 {len(new_duplicates)} 组重复，需要进一步分析")
    
    print(f"\n" + "=" * 80)
    print("检测完成")
    print("=" * 80)

if __name__ == "__main__":
    detect_duplicates()
