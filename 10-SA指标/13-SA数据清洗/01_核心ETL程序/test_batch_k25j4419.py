"""
批次K25J4419专项测试脚本
验证批次合格率计算逻辑的正确性
"""

import pandas as pd
import numpy as np
import sys
import os

def test_batch_k25j4419():
    """测试批次K25J4419的合格率计算"""
    
    print("=" * 60)
    print("批次K25J4419专项测试")
    print("=" * 60)
    
    # 1. 读取处理后的数据文件
    output_file = r"C:/Users/huangk14/OneDrive - Medtronic PLC/CZ Production - 文档/General/POWER BI 数据源 V2/30-MES导出数据/publish/SFC_Team_PassRate_latest.parquet"
    
    try:
        df = pd.read_parquet(output_file)
        print(f"✅ 成功读取输出文件: {len(df)} 条记录")
    except Exception as e:
        print(f"❌ 读取输出文件失败: {e}")
        return
    
    # 2. 筛选批次K25J4419的数据
    batch_data = df[df['批次号'] == 'K25J4419'].copy()
    
    if batch_data.empty:
        print(f"❌ 未找到批次K25J4419的数据")
        print("🔍 可用的批次号示例:")
        print(df['批次号'].unique()[:10])
        return
    
    print(f"✅ 找到批次K25J4419: {len(batch_data)} 条记录")
    
    # 3. 显示批次基本信息
    print("\n📊 批次基本信息:")
    print(f"批次号: K25J4419")
    print(f"产品号: {batch_data['产品号'].iloc[0]}")
    print(f"班组数: {batch_data['班组'].nunique()}")
    print(f"工序数: {batch_data['工序编号'].nunique()}")
    
    # 4. 按工序显示详细数据
    print("\n📋 按工序详细数据:")
    print("-" * 80)
    print(f"{'工序编号':<8} {'工序名称':<20} {'班组':<10} {'合格数':<8} {'不合格数':<8} {'批次合格率':<10}")
    print("-" * 80)
    
    # 按工序编号排序显示（转换为数字排序）
    batch_data['工序编号_排序'] = pd.to_numeric(batch_data['工序编号'], errors='coerce')
    batch_sorted = batch_data.sort_values('工序编号_排序')
    
    for _, row in batch_sorted.iterrows():
        print(f"{row['工序编号']:<8} {row['工序名称']:<20} {row['班组']:<10} {row['合格数']:<8} {row['不合格数']:<8} {row['批次合格率']:<10}")
    
    # 5. 手动计算验证
    print("\n🧮 手动计算验证:")
    print("-" * 40)
    
    # 检查可用列名
    print("📋 可用列名:")
    for i, col in enumerate(batch_data.columns):
        print(f"  {i+1}. {col}")
    
    # 找到最终工序（使用工序编号转换为数字）
    batch_data['工序编号_排序'] = pd.to_numeric(batch_data['工序编号'], errors='coerce')
    max_operation_num = batch_data['工序编号_排序'].max()
    final_operation_records = batch_data[batch_data['工序编号_排序'] == max_operation_num]
    
    print(f"\n最终工序编号: {max_operation_num}")
    print(f"最终工序记录数: {len(final_operation_records)}")
    
    # 计算各指标
    total_unqualified = batch_data['不合格数'].sum()
    
    # 显示最终工序的各班组合格数
    print("\n最终工序各班组合格数:")
    for _, row in final_operation_records.iterrows():
        print(f"  {row['班组']}: {row['合格数']}")
    
    # 使用idxmax选择的班组合格数（当前算法）
    max_idx = batch_data['工序编号_排序'].idxmax()
    selected_final_op = batch_data.loc[max_idx]
    selected_qualified = selected_final_op['合格数']
    selected_team = selected_final_op['班组']
    
    print(f"\n当前算法选择:")
    print(f"  选择的班组: {selected_team}")
    print(f"  合格数: {selected_qualified}")
    print(f"  总不合格数: {total_unqualified}")
    
    # 计算合格率
    total_defects = selected_qualified + total_unqualified
    calculated_pass_rate = (selected_qualified / total_defects * 100).round(2) if total_defects > 0 else 0
    calculated_pass_rate_percentage = round(calculated_pass_rate / 100, 4) if calculated_pass_rate > 0 else 0
    
    print(f"\n计算过程:")
    print(f"  总缺陷数 = {selected_qualified} + {total_unqualified} = {total_defects}")
    print(f"  合格率(%) = {selected_qualified} / {total_defects} * 100 = {calculated_pass_rate}")
    print(f"  合格率(小数) = {calculated_pass_rate} / 100 = {calculated_pass_rate_percentage}")
    
    # 6. 与存储值对比
    stored_pass_rate = batch_data['批次合格率'].iloc[0]
    print(f"\n📊 结果对比:")
    print(f"  计算值: {calculated_pass_rate_percentage}")
    print(f"  存储值: {stored_pass_rate}")
    print(f"  是否一致: {'✅ 是' if abs(calculated_pass_rate_percentage - stored_pass_rate) < 0.0001 else '❌ 否'}")
    
    # 7. 显示其他可能的计算方式
    print(f"\n🔍 其他计算方式对比:")
    
    # 方案1: 使用最终工序合格数最多的班组
    max_qualified_record = final_operation_records.loc[final_operation_records['合格数'].idxmax()]
    max_qualified = max_qualified_record['合格数']
    max_qualified_team = max_qualified_record['班组']
    max_qualified_rate = round((max_qualified / (max_qualified + total_unqualified) * 100 / 100), 4)
    
    print(f"方案1 - 最终工序合格数最多的班组:")
    print(f"  班组: {max_qualified_team}, 合格数: {max_qualified}, 合格率: {max_qualified_rate}")
    
    # 方案2: 使用最终工序所有班组合格数总和
    sum_final_qualified = final_operation_records['合格数'].sum()
    sum_qualified_rate = round((sum_final_qualified / (sum_final_qualified + total_unqualified) * 100 / 100), 4)
    
    print(f"方案2 - 最终工序所有班组合格数总和:")
    print(f"  总合格数: {sum_final_qualified}, 合格率: {sum_qualified_rate}")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)

if __name__ == "__main__":
    test_batch_k25j4419()
