"""
计划工时数据 ETL 脚本
从 Excel 文件导入计划工时数据到数据库
用于与实际工时进行对比分析
"""

import os
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

def get_db_manager() -> SQLServerOnlyManager:
    """获取 SQL Server 数据库管理器（只写 SQL Server）"""
    return SQLServerOnlyManager(
        sql_server=os.getenv("MDDAP_SQL_SERVER", r"localhost\SQLEXPRESS"),
        sql_db=os.getenv("MDDAP_SQL_DATABASE", "mddap_v2"),
        driver=os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
    )

def create_table(db: SQLServerOnlyManager):
    """创建计划工时表 (SQL Server)"""
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.planned_labor_hours', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.planned_labor_hours (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    plan_date DATE NOT NULL,
                    cz_planned_hours FLOAT NULL,
                    kh_planned_hours FLOAT NULL,
                    is_cz_workday BIT NOT NULL CONSTRAINT DF_planned_labor_hours_is_cz_workday DEFAULT(0),
                    is_kh_workday BIT NOT NULL CONSTRAINT DF_planned_labor_hours_is_kh_workday DEFAULT(0),
                    source_file NVARCHAR(260) NULL,
                    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
                    updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
                );

                CREATE UNIQUE INDEX ux_planned_labor_hours_plan_date ON dbo.planned_labor_hours(plan_date);
            END
            """
        )
        conn.commit()

def clean_data(df):
    """清洗数据"""
    # 查找日期列
    date_col = None
    cz_col = None
    kh_col = None
    
    for col in df.columns:
        col_str = str(col)
        if '日期' in col_str or 'date' in col_str:
            date_col = col
        elif '常州' in col_str:
            cz_col = col
        elif '康辉' in col_str:
            kh_col = col
    
    if not all([date_col, cz_col, kh_col]):
        raise ValueError(f"找不到必要的列。找到的列: {df.columns.tolist()}")
    
    # 重命名列
    df = df.rename(columns={
        date_col: 'plan_date',
        cz_col: 'cz_planned_hours',
        kh_col: 'kh_planned_hours'
    })
    
    # 只保留需要的列
    df = df[['plan_date', 'cz_planned_hours', 'kh_planned_hours']].copy()
    
    # 清洗日期
    df['plan_date'] = pd.to_datetime(df['plan_date'], errors='coerce').dt.date
    df = df.dropna(subset=['plan_date'])
    
    # 清洗工时数据
    df['cz_planned_hours'] = pd.to_numeric(df['cz_planned_hours'], errors='coerce')
    df['kh_planned_hours'] = pd.to_numeric(df['kh_planned_hours'], errors='coerce')
    
    # 删除空值行
    df = df.dropna()
    
    return df

def import_file(file_path, mode='all'):
    """导入单个文件"""
    db = get_db_manager()
    
    try:
        # 创建表
        create_table(db)
        
        # 读取 Excel 文件
        df = pd.read_excel(file_path, sheet_name=0)
        
        # 清洗数据
        df_clean = clean_data(df)
        
        if df_clean.empty:
            print(f"警告: 文件 {file_path} 没有有效数据")
            return
        
        # 添加源文件信息和工作日标识
        df_clean['source_file'] = os.path.basename(file_path)
        df_clean['is_cz_workday'] = (df_clean['cz_planned_hours'] > 0).astype(int)
        df_clean['is_kh_workday'] = (df_clean['kh_planned_hours'] > 0).astype(int)

        if mode == 'all':
            create_table(db)

            # Delete existing rows for the dates we are about to load
            dates = [d for d in df_clean['plan_date'].tolist() if d is not None]
            uniq_dates = list(dict.fromkeys(dates).keys())
            if uniq_dates:
                with db.get_connection() as conn:
                    cur = conn.cursor()
                    for i in range(0, len(uniq_dates), 800):
                        batch = uniq_dates[i : i + 800]
                        placeholders = ",".join(["?"] * len(batch))
                        cur.execute(
                            f"DELETE FROM dbo.planned_labor_hours WHERE plan_date IN ({placeholders})",
                            tuple(batch),
                        )
                    conn.commit()

            inserted = db.bulk_insert(df_clean, 'planned_labor_hours', if_exists='append')
            print(f"成功导入: 插入 {inserted}")
        
    except Exception as e:
        print(f"导入失败: {str(e)}")

def import_all_files():
    """导入所有文件"""
    excel_file = r"C:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记\01-常州园区每日计划工时.xlsx"
    
    if os.path.exists(excel_file):
        print(f"导入文件: {excel_file}")
        import_file(excel_file, mode='all')
    else:
        print(f"文件不存在: {excel_file}")

def query_data():
    """查询已导入的数据 (SQL Server)"""
    db = get_db_manager()
    with db.get_connection() as conn:
        return pd.read_sql_query(
            """
            SELECT
                plan_date,
                cz_planned_hours,
                kh_planned_hours,
                source_file,
                created_at,
                updated_at
            FROM dbo.planned_labor_hours
            ORDER BY plan_date DESC
            """,
            conn,
        )

if __name__ == "__main__":
    print("=" * 50)
    print("计划工时数据导入工具 (SQL Server Only)")
    print("=" * 50)
    
    # 导入数据
    import_all_files()
    
    # 显示结果
    print("\n最近导入的数据:")
    try:
        df = query_data()
        print(df.head(10))
        print(f"\n总计: {len(df)} 条记录")
    except Exception as e:
        print(f"查询失败: {e}")

