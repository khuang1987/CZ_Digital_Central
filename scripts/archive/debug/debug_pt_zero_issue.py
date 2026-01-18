#!/usr/bin/env python3
"""
调试PT=0但被判断为Overdue的问题
"""

import pandas as pd
import numpy as np

def main():
    # 读取数据
    mes_file = r'c:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish\MES_batch_report_latest.parquet'
    df = pd.read_parquet(mes_file)
    
    print('=== 检查PT=0的错误记录 ===')
    print()
    
    # 找出PT=0但状态为Overdue的记录
    pt_zero_overdue = df[(df['PT(d)'] == 0) & (df['CompletionStatus'] == 'Overdue')]
    print(f'PT=0但状态为Overdue的记录数: {len(pt_zero_overdue)}')
    
    if len(pt_zero_overdue) > 0:
        print('前5条记录的详细信息:')
        cols_to_show = ['BatchNumber', 'PT(d)', 'ST(d)', 'CompletionStatus', 'Setup', 'Setup Time (h)', 
                       'PreviousBatchEndTime', 'TrackOutTime', 'EnterStepTime']
        available_cols = [col for col in cols_to_show if col in pt_zero_overdue.columns]
        print(pt_zero_overdue[available_cols].head().to_string())
        print()
        
        # 检查第一条错误记录的时间字段
        first_error = pt_zero_overdue.iloc[0]
        print('第一条错误记录的时间信息:')
        print(f'BatchNumber: {first_error.get("BatchNumber")}')
        print(f'PreviousBatchEndTime: {first_error.get("PreviousBatchEndTime")}')
        print(f'TrackOutTime: {first_error.get("TrackOutTime")}')
        print(f'EnterStepTime: {first_error.get("EnterStepTime")}')
        
        # 检查时间差
        prev_end = first_error.get('PreviousBatchEndTime')
        trackout = first_error.get('TrackOutTime')
        
        if pd.notna(prev_end) and pd.notna(trackout):
            time_diff = pd.to_datetime(trackout) - pd.to_datetime(prev_end)
            print(f'时间差: {time_diff}')
            print(f'时间差（小时）: {time_diff.total_seconds() / 3600:.2f}')
            
            # 如果时间差为正，说明PT不应该为0
            if time_diff.total_seconds() > 0:
                print('⚠️  发现问题：时间差为正但PT=0，可能是PT计算错误')
            else:
                print('✓ 时间差为0或负，PT=0是合理的')
        
        print()
    
    # 检查PT=0但状态为OnTime的记录
    pt_zero_ontime = df[(df['PT(d)'] == 0) & (df['CompletionStatus'] == 'OnTime')]
    print(f'PT=0且状态为OnTime的记录数: {len(pt_zero_ontime)}')
    
    print()
    print('=== 分析CompletionStatus计算逻辑问题 ===')
    
    # 模拟calculate_completion_status函数的逻辑
    def debug_completion_status(row):
        pt = row.get('PT(d)')
        st = row.get('ST(d)')
        tolerance_h = row.get('Tolerance(h)', 8.0)
        
        if pd.isna(pt) or pd.isna(st):
            return 'Missing data'
        
        # 获取时间字段
        previous_batch_end = row.get('PreviousBatchEndTime')
        trackout = row.get('TrackOutTime')
        
        print(f'  调试信息:')
        print(f'    PT(d): {pt}')
        print(f'    ST(d): {st}')
        print(f'    PreviousBatchEndTime: {previous_batch_end}')
        print(f'    TrackOutTime: {trackout}')
        
        if pd.isna(previous_batch_end) or pd.isna(trackout):
            print(f'    缺少时间字段，使用原始PT值: {pt} * 24 = {pt * 24}小时')
            pt_workday_hours = pt * 24
        else:
            start_dt = pd.to_datetime(previous_batch_end)
            end_dt = pd.to_datetime(trackout)
            
            if end_dt > start_dt:
                total_hours = (end_dt - start_dt).total_seconds() / 3600
                print(f'    重新计算时间差: {total_hours:.2f}小时')
                # 这里应该调用calculate_nonworkday_hours，但我们简化处理
                pt_workday_hours = total_hours  # 简化版本
            else:
                print(f'    结束时间 <= 开始时间，使用原始PT值: {pt} * 24 = {pt * 24}小时')
                pt_workday_hours = pt * 24
        
        # 计算换型时间
        changeover_time = 0.5
        if row.get('Setup') == 'Yes' and pd.notna(row.get('Setup Time (h)')):
            changeover_time = row.get('Setup Time (h)', 0.5) or 0.5
        
        st_hours = st * 24
        tolerance_and_changeover = tolerance_h + changeover_time
        threshold = st_hours + tolerance_and_changeover
        
        print(f'    ST(小时): {st_hours:.1f}')
        print(f'    容差+换批时间: {tolerance_and_changeover:.1f}')
        print(f'    阈值: {threshold:.1f}')
        print(f'    PT工作日小时: {pt_workday_hours:.1f}')
        print(f'    判断: {pt_workday_hours:.1f} > {threshold:.1f}? {pt_workday_hours > threshold}')
        
        expected_status = 'Overdue' if pt_workday_hours > threshold else 'OnTime'
        actual_status = row.get('CompletionStatus')
        
        print(f'    期望状态: {expected_status}')
        print(f'    实际状态: {actual_status}')
        
        return expected_status == actual_status
    
    # 调试几个错误案例
    print('调试前3个错误案例:')
    error_count = 0
    for idx in range(min(100, len(df))):
        row = df.iloc[idx]
        if row.get('PT(d)') == 0 and row.get('CompletionStatus') == 'Overdue':
            print(f'\\n--- 错误记录 {idx} ---')
            is_correct = debug_completion_status(row)
            print(f'    结果: {"✅ 正确" if is_correct else "❌ 错误"}')
            error_count += 1
            if error_count >= 3:
                break

if __name__ == "__main__":
    main()
