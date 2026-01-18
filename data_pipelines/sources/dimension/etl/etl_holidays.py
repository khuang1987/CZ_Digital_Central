"""
更新中国法定假期到日历表
使用方法: python etl_holidays.py [--year 2025]
"""
import json
import argparse
from pathlib import Path
import logging
import pandas as pd
import pyodbc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).parent.parent / 'config' / 'china_holidays.json'


def get_db_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=mddap_v2;"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def _get_year_range(year: int):
    start = pd.Timestamp(year=year, month=1, day=1)
    end = pd.Timestamp(year=year + 1, month=1, day=1)
    return start.date(), end.date()


def load_holiday_config():
    """加载假期配置"""
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def update_holidays(year=None):
    """更新假期数据到日历表"""
    config = load_holiday_config()
    conn = get_db_connection()
    cursor = conn.cursor()
    
    years_to_update = [year] if year else list(config['holidays'].keys())
    
    for yr in years_to_update:
        yr_str = str(yr)
        if yr_str not in config['holidays']:
            logger.warning(f'年份 {yr_str} 未配置假期数据')
            continue
        
        logger.info(f'更新 {yr_str} 年假期数据...')
        
        # 1. 重置该年所有日期为默认状态（周末休息，工作日上班）
        yr_start, yr_end = _get_year_range(int(yr_str))
        cursor.execute(
            """
            UPDATE dbo.dim_calendar
            SET is_workday = CASE WHEN weekday IN ('Saturday', 'Sunday') THEN 0 ELSE 1 END,
                holiday_name = NULL
            WHERE [date] >= ? AND [date] < ?
            """,
            (yr_start, yr_end),
        )
        
        # 2. 设置法定假期（休息日）
        holidays = config['holidays'].get(yr_str, {})
        for holiday_name, dates in holidays.items():
            for date in dates:
                cursor.execute(
                    """
                    UPDATE dbo.dim_calendar
                    SET is_workday = 0, holiday_name = ?
                    WHERE [date] = ?
                    """,
                    (holiday_name, date),
                )
        
        # 3. 设置调休工作日（周末上班）
        workdays = config['workdays_on_weekend'].get(yr_str, [])
        for date in workdays:
            cursor.execute(
                """
                UPDATE dbo.dim_calendar
                SET is_workday = 1, holiday_name = 'Workday Swap'
                WHERE [date] = ?
                """,
                (date,),
            )
        
        # 统计
        cursor.execute(
            "SELECT COUNT(*) FROM dbo.dim_calendar WHERE [date] >= ? AND [date] < ? AND is_workday = 0",
            (yr_start, yr_end),
        )
        rest_days = cursor.fetchone()[0]
        cursor.execute(
            "SELECT COUNT(*) FROM dbo.dim_calendar WHERE [date] >= ? AND [date] < ? AND is_workday = 1",
            (yr_start, yr_end),
        )
        work_days = cursor.fetchone()[0]
        logger.info(f'  {yr_str}: Workdays {work_days}, Rest days {rest_days}')
    
    conn.commit()

    conn.close()
    logger.info('假期数据更新完成!')


def show_holidays(year):
    """显示指定年份的假期"""
    conn = get_db_connection()
    cursor = conn.cursor()

    yr_start, yr_end = _get_year_range(int(year))
    
    cursor.execute(
        """
        SELECT [date], weekday, holiday_name, is_workday
        FROM dbo.dim_calendar
        WHERE [date] >= ? AND [date] < ? AND holiday_name IS NOT NULL
        ORDER BY [date]
        """,
        (yr_start, yr_end),
    )
    
    print(f'\n{year} 年假期/调休:')
    for row in cursor.fetchall():
        status = '休息' if row[3] == 0 else '上班'
        print(f'  {row[0]} {row[1]} - {row[2]} ({status})')
    
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='更新中国法定假期')
    parser.add_argument('--year', type=int, help='指定更新年份')
    parser.add_argument('--show', type=int, help='显示指定年份假期')
    args = parser.parse_args()
    
    if args.show:
        show_holidays(args.show)
    else:
        update_holidays(args.year)
        if args.year:
            show_holidays(args.year)


if __name__ == '__main__':
    main()
