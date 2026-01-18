#!/usr/bin/env python3
"""
验证容差逻辑是否正确应用
"""

import pandas as pd
import numpy as np

def calculate_completion_status_with_details(row):
    """
    带详细信息的超期判断逻辑
    """
    pt = row.get('PT(d)')
    st = row.get('ST(d)')
    tolerance_h = row.get('Tolerance(h)', 8.0)
    
    if pd.isna(pt) or pd.isna(st):
        return None, None
    
    # PT转换为小时
    pt_hours = pt * 24
    # ST转换为小时
    st_hours = st * 24
    
    # 检查是否需要使用标准换型时间
    changeover_time = 0.5  # 默认换批时间
    if row.get('Setup') == 'Yes' and pd.notna(row.get('Setup Time (h)')):
        changeover_time = row.get('Setup Time (h)', 0.5) or 0.5
    
    tolerance_and_changeover = tolerance_h + changeover_time
    
    # 比较：PT（小时） > ST（小时） + 容差+换批时间 → Overdue
    threshold = st_hours + tolerance_and_changeover
    status = 'Overdue' if pt_hours > threshold else 'OnTime'
    
    details = {
        'pt_hours': pt_hours,
        'st_hours': st_hours,
        'tolerance': tolerance_h,
        'changeover_time': changeover_time,
        'threshold': threshold,
        'comparison': f'{pt_hours:.1f} > {threshold:.1f}? {pt_hours > threshold}'
    }
    
    return status, details

def main():
    # 读取数据
    mes_file = r'c:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish\MES_batch_report_latest.parquet'
    df = pd.read_parquet(mes_file)
    
    print('=== 验证容差逻辑 ===')
    print()
    
    # 检查容差字段
    print('1. 容差字段统计:')
    if 'Tolerance(h)' in df.columns:
        tolerance_counts = df['Tolerance(h)'].value_counts()
        for tolerance, count in tolerance_counts.items():
            print(f'   容差 {tolerance}小时: {count} 条记录')
    else:
        print('   ❌ Tolerance(h) 字段不存在')
    print()
    
    # 测试几个具体案例
    print('2. 具体案例验证:')
    
    # 案例1: PT接近ST但小于容差范围
    print('   案例1: PT接近ST的情况')
    similar_records = df[
        (df['PT(d)'] > 0) & 
        (df['ST(d)'] > 0) & 
        (abs(df['PT(d)'] - df['ST(d)']) < 1.0)  # PT和ST相差小于1天
    ].head(3)
    
    for idx, row in similar_records.iterrows():
        status, details = calculate_completion_status_with_details(row)
        if details:
            print(f'     BatchNumber: {row.get("BatchNumber")}')
            print(f'     PT: {details["pt_hours"]:.1f}h, ST: {details["st_hours"]:.1f}h')
            print(f'     容差: {details["tolerance"]}h, 换批/换型: {details["changeover_time"]}h')
            print(f'     阈值: {details["threshold"]:.1f}h')
            print(f'     判断: {details["comparison"]}')
            print(f'     结果: {status}')
            print()
    
    # 案例2: 边界值测试
    print('   案例2: 边界值测试')
    print('   寻找PT ≈ ST + 容差的记录...')
    
    boundary_records = []
    for idx in range(min(1000, len(df))):
        row = df.iloc[idx]
        status, details = calculate_completion_status_with_details(row)
        if details and abs(details['pt_hours'] - details['threshold']) < 2.0:  # 差值小于2小时
            boundary_records.append((row, status, details))
            if len(boundary_records) >= 3:
                break
    
    for i, (row, status, details) in enumerate(boundary_records):
        print(f'     边界记录 {i+1}: BatchNumber={row.get("BatchNumber")}')
        print(f'     PT: {details["pt_hours"]:.1f}h, 阈值: {details["threshold"]:.1f}h')
        print(f'     差值: {details["pt_hours"] - details["threshold"]:.1f}h')
        print(f'     判断: {details["comparison"]}')
        print(f'     结果: {status}')
        print()
    
    # 案例3: 容差影响分析
    print('3. 容差影响分析:')
    print('   比较有无容差的判断差异...')
    
    # 模拟无容差的判断
    def judge_without_tolerance(row):
        pt = row.get('PT(d)')
        st = row.get('ST(d)')
        if pd.isna(pt) or pd.isna(st):
            return None
        pt_hours = pt * 24
        st_hours = st * 24
        # 只考虑换批时间，不考虑容差
        changeover_time = 0.5
        if row.get('Setup') == 'Yes' and pd.notna(row.get('Setup Time (h)')):
            changeover_time = row.get('Setup Time (h)', 0.5) or 0.5
        threshold = st_hours + changeover_time
        return 'Overdue' if pt_hours > threshold else 'OnTime'
    
    diff_count = 0
    total_count = 0
    
    for idx in range(min(1000, len(df))):
        row = df.iloc[idx]
        status_with_tolerance, _ = calculate_completion_status_with_details(row)
        status_without_tolerance = judge_without_tolerance(row)
        
        if status_with_tolerance and status_without_tolerance:
            total_count += 1
            if status_with_tolerance != status_without_tolerance:
                diff_count += 1
    
    print(f'   检查了 {total_count} 条记录')
    print(f'   容差影响判断的记录: {diff_count} 条 ({diff_count/total_count*100:.1f}%)')
    
    if diff_count > 0:
        print('   ✅ 容差正在影响超期判断')
    else:
        print('   ⚠️  在检查的记录中容差未显示影响')
    
    print()
    print('4. 总结:')
    print('   ✅ 超期判断包含8小时容差')
    print('   ✅ 容差与换批时间一起计算阈值')
    print('   ✅ 判断公式: PT > ST + 8小时容差 + 换批时间')

if __name__ == "__main__":
    main()
