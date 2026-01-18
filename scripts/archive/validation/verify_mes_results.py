#!/usr/bin/env python3
"""
验证MES数据处理结果的正确性
"""

import pandas as pd
import numpy as np

def verify_overdue_logic(row):
    """验证超期判断逻辑"""
    pt = row.get('PT(d)')
    st = row.get('ST(d)')
    tolerance = row.get('Tolerance(h)', 8.0)
    setup = row.get('Setup')
    setup_time = row.get('Setup Time (h)')
    actual_status = row.get('CompletionStatus')
    
    if pd.isna(pt) or pd.isna(st):
        return None
    
    # 计算换型时间
    changeover_time = 0.5
    if setup == 'Yes' and pd.notna(setup_time):
        changeover_time = setup_time
    
    # 转换为小时
    st_hours = st * 24
    
    # 模拟calculate_completion_status的PT计算逻辑
    previous_batch_end = row.get('PreviousBatchEndTime')
    trackout = row.get('TrackOutTime')
    
    if pd.isna(previous_batch_end) or pd.isna(trackout):
        # 如果缺少必要字段，使用原始PT值（已经按天计算）
        pt_workday_hours = pt * 24
    else:
        # 重新计算从PreviousBatchEndTime到TrackOutTime之间的工作日小时数
        start_dt = pd.to_datetime(previous_batch_end)
        end_dt = pd.to_datetime(trackout)
        
        if end_dt > start_dt:
            # 总时间（小时）
            total_hours = (end_dt - start_dt).total_seconds() / 3600
            # 简化处理：不扣除非工作日时间（因为验证脚本没有日历数据）
            pt_workday_hours = total_hours
        else:
            pt_workday_hours = pt * 24
    
    threshold = st_hours + tolerance + changeover_time
    
    # 判断
    expected_status = 'Overdue' if pt_workday_hours > threshold else 'OnTime'
    
    return {
        'pt_hours': pt_workday_hours,  # 使用重新计算的PT值
        'st_hours': st_hours,
        'tolerance': tolerance,
        'changeover_time': changeover_time,
        'threshold': threshold,
        'expected_status': expected_status,
        'actual_status': actual_status,
        'is_correct': expected_status == actual_status,
        'original_pt': pt * 24,  # 原始PT值
        'recalculated_pt': pt_workday_hours  # 重新计算的PT值
    }

def main():
    # 读取数据
    mes_file = r'c:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish\MES_batch_report_latest.parquet'
    df = pd.read_parquet(mes_file)
    
    print('=== MES数据处理结果验证 ===')
    print(f'总记录数: {len(df)}')
    print()
    
    # 检查关键字段
    key_fields = ['BatchNumber', 'PT(d)', 'ST(d)', 'CompletionStatus', 'Tolerance(h)', 'Setup', 'Setup Time (h)']
    missing_fields = [field for field in key_fields if field not in df.columns]
    if missing_fields:
        print(f'❌ 缺少关键字段: {missing_fields}')
        return
    else:
        print('✅ 所有关键字段都存在')
    
    print()
    print('=== CompletionStatus 分布 ===')
    status_counts = df['CompletionStatus'].value_counts()
    for status, count in status_counts.items():
        print(f'{status}: {count} ({count/len(df)*100:.1f}%)')
    
    print()
    print('=== 案例验证 ===')
    
    # 检查前5条记录
    for idx in range(min(5, len(df))):
        row = df.iloc[idx]
        result = verify_overdue_logic(row)
        
        if result:
            batch_num = row.get('BatchNumber', 'N/A')
            print(f'记录 {idx}: BatchNumber={batch_num}')
            print(f'  原始PT={result["original_pt"]:.1f}h, 重新计算PT={result["recalculated_pt"]:.1f}h, ST={result["st_hours"]:.1f}h')
            print(f'  容差={result["tolerance"]}h, 换批/换型={result["changeover_time"]}h')
            print(f'  阈值={result["threshold"]:.1f}h')
            print(f'  判断: {result["recalculated_pt"]:.1f} > {result["threshold"]:.1f}? {result["recalculated_pt"] > result["threshold"]}')
            print(f'  期望状态: {result["expected_status"]}, 实际状态: {result["actual_status"]}')
            status = '✅ 正确' if result['is_correct'] else '❌ 错误'
            print(f'  {status}')
            if result['original_pt'] != result['recalculated_pt']:
                print(f'  ⚠️  PT值被重新计算（原始={result["original_pt"]:.1f}h → 重新计算={result["recalculated_pt"]:.1f}h）')
            print()
    
    # 整体验证
    print('=== 整体验证 ===')
    correct_count = 0
    total_count = 0
    
    for idx in range(min(1000, len(df))):  # 检查前1000条记录
        row = df.iloc[idx]
        result = verify_overdue_logic(row)
        if result:
            total_count += 1
            if result['is_correct']:
                correct_count += 1
    
    print(f'检查了 {total_count} 条记录')
    print(f'正确判断: {correct_count} 条 ({correct_count/total_count*100:.1f}%)')
    print(f'错误判断: {total_count - correct_count} 条 ({(total_count - correct_count)/total_count*100:.1f}%)')
    
    if correct_count == total_count:
        print('✅ 所有检查的记录超期判断都正确!')
    else:
        print('❌ 发现错误的超期判断，需要进一步检查')
        
        # 显示一些错误案例
        print()
        print('=== 错误案例 ===')
        error_count = 0
        for idx in range(min(1000, len(df))):
            row = df.iloc[idx]
            result = verify_overdue_logic(row)
            if result and not result['is_correct']:
                batch_num = row.get('BatchNumber', 'N/A')
                print(f'错误记录 {idx}: BatchNumber={batch_num}')
                print(f'  PT={result["pt_hours"]:.1f}h, ST={result["st_hours"]:.1f}h, 阈值={result["threshold"]:.1f}h')
                print(f'  期望: {result["expected_status"]}, 实际: {result["actual_status"]}')
                error_count += 1
                if error_count >= 5:  # 只显示前5个错误
                    break

if __name__ == "__main__":
    main()
