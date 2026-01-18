"""
生成财历日历表 (Medtronic 4-5-4 财历)
规则：
- 财年从每年4月最后一个周六开始
- 财历周从周六开始
- 每季度采用 4-5-4 周制 (4周+5周+4周=13周)
- Q1: M01-M03 (May-Jul), Q2: M04-M06 (Aug-Oct), Q3: M07-M09 (Nov-Jan), Q4: M10-M12 (Feb-Apr)
"""
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)



# 财历月名称映射
MONTH_NAMES = ['MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', 'JAN', 'FEB', 'MAR', 'APR']
MONTH_NAMES_SHORT = ['May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr']

# 4-5-4 周制：每季度的月份周数
WEEKS_PER_MONTH = [4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5, 4]  # 12个月

# 星期名称 (中英文)
WEEKDAY_NAMES_CN = ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期日']
WEEKDAY_NAMES_EN = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


def get_fiscal_year_start(calendar_year):
    """获取财年开始日期（4月最后一个周六）"""
    # 从4月30日往前找最近的周六
    apr_30 = datetime(calendar_year, 4, 30)
    days_since_saturday = (apr_30.weekday() + 2) % 7  # 周六是5，调整计算
    last_saturday = apr_30 - timedelta(days=days_since_saturday)
    
    # 如果结果在5月，往前推一周
    if last_saturday.month == 5:
        last_saturday -= timedelta(days=7)
    
    return last_saturday


def generate_fiscal_calendar(start_fy=21, end_fy=30):
    """生成财历日历数据"""
    records = []
    
    for fy_num in range(start_fy, end_fy + 1):
        fy_name = f'FY{fy_num}'
        calendar_year = 2000 + fy_num - 1  # FY21 从 2020年4月开始
        
        # 获取财年开始日期
        fy_start = get_fiscal_year_start(calendar_year)
        logger.info(f'{fy_name} 开始日期: {fy_start.strftime("%Y-%m-%d")}')
        
        current_date = fy_start
        fiscal_week = 0
        
        for month_idx in range(12):
            month_num = month_idx + 1  # M01-M12
            month_name = MONTH_NAMES[month_idx]
            month_name_short = MONTH_NAMES_SHORT[month_idx]
            fiscal_month_order = month_idx + 1
            quarter = (month_idx // 3) + 1  # Q1-Q4
            weeks_in_month = WEEKS_PER_MONTH[month_idx]
            
            fiscal_month = f'{fy_name} M{month_num:02d} {month_name}'
            fiscal_quarter = f'Q{quarter}'
            
            # 生成该月的所有日期
            for week_in_month in range(weeks_in_month):
                fiscal_week += 1
                fiscal_week_str = f'W{fiscal_week}'
                
                for day_in_week in range(7):
                    # 计算日历周 (ISO周)
                    calendar_week = current_date.isocalendar()[1]
                    weekday_en = WEEKDAY_NAMES_EN[current_date.weekday()]
                    weekday_cn = WEEKDAY_NAMES_CN[current_date.weekday()]
                    
                    # 默认工作日判断（周六日为休息日）
                    is_workday = 0 if current_date.weekday() >= 5 else 1
                    
                    record = {
                        'date': current_date.strftime('%Y-%m-%d'),
                        'weekday': weekday_en,
                        'weekday_cn': weekday_cn,
                        'calendar_week': calendar_week,
                        'fiscal_week': fiscal_week,
                        'fiscal_week_label': fiscal_week_str,
                        'fiscal_month': fiscal_month,
                        'fiscal_quarter': fiscal_quarter,
                        'fiscal_year': fy_name,
                        'fiscal_month_short': month_name_short,
                        'fiscal_month_order': fiscal_month_order,
                        'is_workday': is_workday,
                        'holiday_name': None
                    }
                    records.append(record)
                    current_date += timedelta(days=1)
    
    return pd.DataFrame(records)


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )


def ensure_sqlserver_tables(db: SQLServerOnlyManager) -> None:
    with db.get_connection() as conn:
        cur = conn.cursor()

        cur.execute(
            """
            IF OBJECT_ID('dbo.dim_calendar', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.dim_calendar (
                    [date] date NOT NULL PRIMARY KEY,
                    [weekday] nvarchar(20) NULL,
                    [weekday_cn] nvarchar(20) NULL,
                    [calendar_week] int NULL,
                    [fiscal_week] int NULL,
                    [fiscal_week_label] nvarchar(20) NULL,
                    [fiscal_month] nvarchar(50) NULL,
                    [fiscal_quarter] nvarchar(20) NULL,
                    [fiscal_year] nvarchar(20) NULL,
                    [fiscal_month_short] nvarchar(20) NULL,
                    [fiscal_month_order] int NULL,
                    [is_workday] int NULL,
                    [holiday_name] nvarchar(100) NULL,
                    [created_at] datetime NOT NULL DEFAULT GETDATE()
                );
            END
            """
        )

        # raw_calendar may already exist; ensure minimum columns exist
        cur.execute(
            """
            IF OBJECT_ID('dbo.raw_calendar', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.raw_calendar (
                    id int IDENTITY(1,1) PRIMARY KEY,
                    CalendarDate date NOT NULL,
                    IsWorkday int NULL,
                    [Year] int NULL,
                    [Month] int NULL,
                    WeekOfYear int NULL,
                    DayOfWeek int NULL,
                    created_at datetime NOT NULL DEFAULT GETDATE()
                );
                CREATE UNIQUE INDEX ux_raw_calendar_date ON dbo.raw_calendar(CalendarDate);
            END
            """
        )

        conn.commit()


def refresh_dim_calendar(db: SQLServerOnlyManager, df: pd.DataFrame) -> int:
    df2 = df.copy()
    df2["date"] = pd.to_datetime(df2["date"], errors="coerce").dt.date

    cols = [
        "date",
        "weekday",
        "weekday_cn",
        "calendar_week",
        "fiscal_week",
        "fiscal_week_label",
        "fiscal_month",
        "fiscal_quarter",
        "fiscal_year",
        "fiscal_month_short",
        "fiscal_month_order",
        "is_workday",
        "holiday_name",
    ]
    df2 = df2[cols]

    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.dim_calendar")
        conn.commit()

    return db.bulk_insert(df2, "dim_calendar", if_exists="append")


def refresh_raw_calendar(db: SQLServerOnlyManager, df: pd.DataFrame) -> int:
    df2 = df.copy()
    dt = pd.to_datetime(df2["date"], errors="coerce")
    df_raw = pd.DataFrame(
        {
            "CalendarDate": dt.dt.date,
            "IsWorkday": pd.to_numeric(df2.get("is_workday", 1), errors="coerce"),
            "Year": dt.dt.year,
            "Month": dt.dt.month,
            "WeekOfYear": dt.dt.isocalendar().week.astype(int),
            "DayOfWeek": dt.dt.dayofweek + 1,
        }
    )

    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM dbo.raw_calendar")
        conn.commit()

    return db.bulk_insert(df_raw, "raw_calendar", if_exists="append")


def main():
    """主函数"""
    logger.info('开始生成财历日历表...')
    
    # 生成日历数据 (FY21-FY30)
    df = generate_fiscal_calendar(start_fy=21, end_fy=30)
    logger.info(f'生成 {len(df)} 条日历记录')
    
    db = get_db_manager()
    ensure_sqlserver_tables(db)
    inserted_dim = refresh_dim_calendar(db, df)
    inserted_raw = refresh_raw_calendar(db, df)

    logger.info(f"dim_calendar 已刷新: {inserted_dim} 行")
    logger.info(f"raw_calendar 已刷新: {inserted_raw} 行")
    logger.info('日历表生成完成!')


if __name__ == '__main__':
    main()
