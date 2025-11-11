"""
生成中国节假日日历表
包含2024-2026年的法定节假日、调休安排和工作日标记
"""

import pandas as pd
from datetime import datetime, timedelta
import os

# 定义法定节假日（包含调休）
# 格式：{年份: {节假日名称: [日期列表], '调休工作日': [日期列表]}}
HOLIDAYS = {
    2024: {
        '元旦': ['2024-01-01'],
        '春节': ['2024-02-10', '2024-02-11', '2024-02-12', '2024-02-13', '2024-02-14', '2024-02-15', '2024-02-16', '2024-02-17'],
        '清明节': ['2024-04-04', '2024-04-05', '2024-04-06'],
        '劳动节': ['2024-05-01', '2024-05-02', '2024-05-03', '2024-05-04', '2024-05-05'],
        '端午节': ['2024-06-10'],
        '中秋节': ['2024-09-15', '2024-09-16', '2024-09-17'],
        '国庆节': ['2024-10-01', '2024-10-02', '2024-10-03', '2024-10-04', '2024-10-05', '2024-10-06', '2024-10-07'],
        '调休工作日': ['2024-02-04', '2024-02-18', '2024-04-07', '2024-04-28', '2024-05-11', '2024-09-14', '2024-09-29', '2024-10-12']
    },
    2025: {
        '元旦': ['2025-01-01'],
        '春节': ['2025-01-28', '2025-01-29', '2025-01-30', '2025-01-31', '2025-02-01', '2025-02-02', '2025-02-03', '2025-02-04'],
        '清明节': ['2025-04-04', '2025-04-05', '2025-04-06'],
        '劳动节': ['2025-05-01', '2025-05-02', '2025-05-03', '2025-05-04', '2025-05-05'],
        '端午节': ['2025-05-31'],
        '中秋节': ['2025-10-06'],
        '国庆节': ['2025-10-01', '2025-10-02', '2025-10-03', '2025-10-04', '2025-10-05', '2025-10-06', '2025-10-07', '2025-10-08'],
        '调休工作日': ['2025-01-26', '2025-02-08', '2025-04-27', '2025-05-09', '2025-10-11']
    },
    2026: {
        '元旦': ['2026-01-01', '2026-01-02', '2026-01-03'],
        '春节': ['2026-02-16', '2026-02-17', '2026-02-18', '2026-02-19', '2026-02-20', '2026-02-21', '2026-02-22'],
        '清明节': ['2026-04-04', '2026-04-05', '2026-04-06'],
        '劳动节': ['2026-05-01', '2026-05-02', '2026-05-03', '2026-05-04', '2026-05-05'],
        '端午节': ['2026-06-19', '2026-06-20', '2026-06-21'],
        '中秋节': ['2026-09-25', '2026-09-26', '2026-09-27'],
        '国庆节': ['2026-10-01', '2026-10-02', '2026-10-03', '2026-10-04', '2026-10-05', '2026-10-06', '2026-10-07', '2026-10-08'],
        '调休工作日': ['2026-02-15', '2026-02-28', '2026-04-26', '2026-05-09', '2026-09-28', '2026-10-10']
    }
}

# 星期中文映射
WEEKDAY_CN = {
    0: '星期一',
    1: '星期二',
    2: '星期三',
    3: '星期四',
    4: '星期五',
    5: '星期六',
    6: '星期日'
}


def generate_calendar(start_date: str = '2024-01-01', end_date: str = '2026-12-30') -> pd.DataFrame:
    """
    生成日历表
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    
    Returns:
        DataFrame包含：日期、星期、是否节假日、是否工作日、节假日名称
    """
    # 生成日期范围
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # 创建DataFrame
    df = pd.DataFrame({
        '日期': date_range,
        '年': date_range.year,
        '月': date_range.month,
        '日': date_range.day
    })
    
    # 添加星期
    df['星期'] = df['日期'].dt.weekday.map(WEEKDAY_CN)
    df['星期数'] = df['日期'].dt.weekday  # 0=Monday, 6=Sunday
    
    # 初始化列
    df['是否节假日'] = False
    df['是否工作日'] = False
    df['节假日名称'] = ''
    df['是否调休工作日'] = False
    
    # 收集所有节假日日期和调休工作日
    holiday_dates = set()
    workday_dates = set()  # 调休工作日（周末但需要上班）
    holiday_names_map = {}
    
    for year, year_holidays in HOLIDAYS.items():
        for holiday_name, dates in year_holidays.items():
            if holiday_name == '调休工作日':
                # 调休工作日：周末但需要上班
                for date_str in dates:
                    date_obj = pd.to_datetime(date_str).date()
                    workday_dates.add(date_obj)
            else:
                # 法定节假日
                for date_str in dates:
                    date_obj = pd.to_datetime(date_str).date()
                    holiday_dates.add(date_obj)
                    holiday_names_map[date_obj] = holiday_name
    
    # 标记节假日和调休工作日
    df['日期_仅日期'] = df['日期'].dt.date
    
    # 标记节假日
    df.loc[df['日期_仅日期'].isin(holiday_dates), '是否节假日'] = True
    df.loc[df['日期_仅日期'].isin(holiday_dates), '节假日名称'] = df.loc[df['日期_仅日期'].isin(holiday_dates), '日期_仅日期'].map(holiday_names_map)
    
    # 标记调休工作日
    df.loc[df['日期_仅日期'].isin(workday_dates), '是否调休工作日'] = True
    
    # 判断工作日
    # 工作日 = (周一到周五 且 不是节假日) 或 调休工作日
    df['是否工作日'] = (
        (df['星期数'] < 5) & (~df['是否节假日'])
    ) | df['是否调休工作日']
    
    # 清理临时列
    df = df.drop(columns=['日期_仅日期', '星期数'])
    
    # 重新排列列顺序
    df = df[['日期', '年', '月', '日', '星期', '是否节假日', '是否工作日', '是否调休工作日', '节假日名称']]
    
    return df


def main():
    """主函数"""
    print("正在生成中国节假日日历表...")
    
    # 生成日历表
    calendar_df = generate_calendar('2024-01-01', '2026-12-30')
    
    # 输出路径
    output_dir = r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish"
    output_file = os.path.join(output_dir, "日历工作日表.csv")
    
    # 确保目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存为CSV
    calendar_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"日历表已生成: {output_file}")
    print(f"总天数: {len(calendar_df)}")
    print(f"工作日数: {calendar_df['是否工作日'].sum()}")
    print(f"节假日数: {calendar_df['是否节假日'].sum()}")
    print(f"调休工作日数: {calendar_df['是否调休工作日'].sum()}")
    
    # 显示前几行
    print("\n前10行数据预览:")
    print(calendar_df.head(10).to_string(index=False))
    
    # 显示节假日统计
    print("\n节假日统计:")
    holiday_stats = calendar_df[calendar_df['是否节假日']].groupby('年')['节假日名称'].value_counts()
    print(holiday_stats)


if __name__ == "__main__":
    main()

