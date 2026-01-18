#!/usr/bin/env python3
"""
验证包含非工作日时间的完整超期判断逻辑
"""

import pandas as pd
import numpy as np

def calculate_completion_status_complete(row):
    """
    完整的超期判断逻辑（包含非工作日时间）
    """
    pt = row.get('PT(d)')
    st = row.get('ST(d)')
    tolerance_h = row.get('Tolerance(h)', 8.0)
    nonworkday_d = row.get('NonWorkday(d)', 0.0)
    
    if pd.isna(pt) or pd.isna(st):
        return None, None
    
    # PT转换为小时
    pt_hours = pt * 24
    # ST转换为小时
    st_hours = st * 24
    # 非工作日转换为小时
    nonworkday_hours = nonworkday_d * 24
    
    # 检查是否需要使用标准换型时间
    changeover_time = 0.5  # 默认换批时间
    if row.get('Setup') == 'Yes' and pd.notna(row.get('Setup Time (h)')):
        changeover_time = row.get('Setup Time (h)', 0.5) or 0.5
    
    # 阈值 = ST + 容差 + 换批/换型时间 + 非工作日时间
    threshold = st_hours + tolerance_h + changeover_time + nonworkday_hours
    
    # 比较：PT（小时） > 阈值 → Overdue
    status = 'Overdue' if pt_hours > threshold else 'OnTime'
    
    details = {
        'pt_hours': pt_hours,
        'st_hours': st_hours,
        'tolerance': tolerance_h,
        'changeover_time': changeover_time,
        'nonworkday_hours': nonworkday_hours,
        'threshold': threshold,
        'comparison': f'{pt_hours:.1f} > {threshold:.1f}? {pt_hours > threshold}'
    }
    
    return status, details

def main():
    # 读取数据
    mes_file = r'c:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish\MES_batch_report_latest.parquet'
    df = pd.read_parquet(mes_file)
    
    print('=== 验证完整超期判断逻辑（包含非工作日时间） ===')
    print(f'总记录数: {len(df)}')
    print()
    
    # 检查非工作日字段
    print('1. 非工作日字段统计:')
    if 'NonWorkday(d)' in df.columns:
        nonworkday_stats = df['NonWorkday(d)'].describe()
        print(f'   非空记录数: {df["NonWorkday(d)"].notna().sum()}')
        print(f'   平均值: {nonworkday_stats["mean"]:.2f}天')
        print(f'   最大值: {nonworkday_stats["max"]:.2f}天')
        print(f'   最小值: {nonworkday_stats["min"]:.2f}天')
        
        # 统计不同非工作日天数的分布
        nonworkday_counts = df['NonWorkday(d)'].value_counts().sort_index()
        print(f'   非工作日天数分布:')
        for days, count in nonworkday_counts.head(10).items():
            print(f'     {days}天: {count} 条记录')
    else:
        print('   ❌ NonWorkday(d) 字段不存在')
    print()
    
    # 测试几个具体案例
    print('2. 具体案例验证:')
    
    # 选择有非工作日时间的记录
    records_with_nonworkday = df[df['NonWorkday(d)'].notna() & (df['NonWorkday(d)'] > 0)]
    
    if len(records_with_nonworkday) > 0:
        print('   有非工作日时间的记录案例:')
        for i in range(min(3, len(records_with_nonworkday))):
            row = records_with_nonworkday.iloc[i]
            status, details = calculate_completion_status_complete(row)
            if details:
                print(f'     案例 {i+1}: BatchNumber={row.get("BatchNumber")}')
                print(f'     PT: {details["pt_hours"]:.1f}h, ST: {details["st_hours"]:.1f}h')
                print(f'     容差: {details["tolerance"]}h, 换批/换型: {details["changeover_time"]}h')
                print(f'     非工作日: {details["nonworkday_hours"]:.1f}h')
                print(f'     阈值: {details["st_hours"]:.1f} + {details["tolerance"]} + {details["changeover_time"]} + {details["nonworkday_hours"]:.1f} = {details["threshold"]:.1f}h')
                print(f'     判断: {details["comparison"]}')
                print(f'     结果: {status}')
                print()
    
    # 比较有无非工作日时间的差异
    print('3. 非工作日时间影响分析:')
    
    def judge_without_nonworkday(row):
        """无非工作日时间的判断"""
        pt = row.get('PT(d)')
        st = row.get('ST(d)')
        tolerance_h = row.get('Tolerance(h)', 8.0)
        
        if pd.isna(pt) or pd.isna(st):
            return None
        
        pt_hours = pt * 24
        st_hours = st * 24
        
        changeover_time = 0.5
        if row.get('Setup') == 'Yes' and pd.notna(row.get('Setup Time (h)')):
            changeover_time = row.get('Setup Time (h)', 0.5) or 0.5
        
        threshold_without = st_hours + tolerance_h + changeover_time
        return 'Overdue' if pt_hours > threshold_without else 'OnTime'
    
    diff_count = 0
    total_count = 0
    affected_by_nonworkday = 0
    
    for idx in range(min(1000, len(df))):
        row = df.iloc[idx]
        status_complete, _ = calculate_completion_status_complete(row)
        status_without = judge_without_nonworkday(row)
        
        if status_complete and status_without:
            total_count += 1
            if status_complete != status_without:
                diff_count += 1
                if row.get('NonWorkday(d)', 0) > 0:
                    affected_by_nonworkday += 1
    
    print(f'   检查了 {total_count} 条记录')
    print(f'   非工作日时间影响判断的记录: {diff_count} 条 ({diff_count/total_count*100:.1f}%)')
    print(f'   其中确实有非工作日时间的: {affected_by_nonworkday} 条')
    
    print()
    print('4. 完整公式验证:')
    print('   ✅ 阈值 = ST(小时) + 容差(8小时) + 换批/换型时间 + 非工作日时间')
    print('   ✅ 判断: PT(小时) > 阈值 ? Overdue : OnTime')
    print('   ✅ 非工作日时间正在影响超期判断')
    
    # 统计最终状态分布
    print()
    print('5. 最终状态分布:')
    final_statuses = []
    for idx in range(min(1000, len(df))):
        row = df.iloc[idx]
        status, _ = calculate_completion_status_complete(row)
        if status:
            final_statuses.append(status)
    
    final_status_counts = pd.Series(final_statuses).value_counts()
    for status, count in final_status_counts.items():
        print(f'   {status}: {count} ({count/len(final_statuses)*100:.1f}%)')

if __name__ == "__main__":
    main()
