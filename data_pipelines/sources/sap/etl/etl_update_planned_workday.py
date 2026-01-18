"""
更新计划工时表，添加工作日标识列
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=os.getenv("MDDAP_SQL_SERVER", r"localhost\SQLEXPRESS"),
        sql_db=os.getenv("MDDAP_SQL_DATABASE", "mddap_v2"),
        driver=os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
    )


def _ensure_table_and_columns(db: SQLServerOnlyManager) -> None:
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

            IF COL_LENGTH('dbo.planned_labor_hours', 'is_cz_workday') IS NULL
            BEGIN
                ALTER TABLE dbo.planned_labor_hours ADD is_cz_workday BIT NOT NULL CONSTRAINT DF_planned_labor_hours_is_cz_workday2 DEFAULT(0);
            END

            IF COL_LENGTH('dbo.planned_labor_hours', 'is_kh_workday') IS NULL
            BEGIN
                ALTER TABLE dbo.planned_labor_hours ADD is_kh_workday BIT NOT NULL CONSTRAINT DF_planned_labor_hours_is_kh_workday2 DEFAULT(0);
            END
            """
        )
        conn.commit()

def update_table_structure():
    """更新表结构，添加工作日标识列"""
    db = get_db_manager()

    try:
        _ensure_table_and_columns(db)
        print("表结构检查/更新完成")
    except Exception as e:
        print(f"更新表结构失败: {str(e)}")

def update_workday_flags():
    """更新工作日标识"""
    db = get_db_manager()

    try:
        _ensure_table_and_columns(db)
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE dbo.planned_labor_hours
                SET is_cz_workday = CASE WHEN cz_planned_hours > 0 THEN 1 ELSE 0 END
                """
            )
            cur.execute(
                """
                UPDATE dbo.planned_labor_hours
                SET is_kh_workday = CASE WHEN kh_planned_hours > 0 THEN 1 ELSE 0 END
                """
            )
            conn.commit()

        print("更新工作日标识完成")

    except Exception as e:
        print(f"更新工作日标识失败: {str(e)}")

if __name__ == "__main__":
    print("=" * 50)
    print("更新计划工时表工作日标识 (SQL Server Only)")
    print("=" * 50)
    
    # 更新表结构
    update_table_structure()
    
    # 更新工作日标识
    update_workday_flags()
    
    print("\n完成！")
