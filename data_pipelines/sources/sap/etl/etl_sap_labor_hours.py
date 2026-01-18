"""
SAP 人工工时数据 ETL 脚本
从 Excel 文件导入 SAP 人工工时数据到数据库
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 路径配置
PROJECT_ROOT = Path(__file__).resolve().parents[4]

project_root_str = str(PROJECT_ROOT)
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

SOURCE_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\40-SAP工时')

# 历史文件列映射（24列格式）
COLUMN_MAPPING_LEGACY = {
    0: 'Plant',
    1: 'WorkCenter',
    2: 'WorkCenterDesc',
    3: 'CostCenter',
    4: 'CostCenterDesc',
    5: 'Material',
    6: 'MaterialDesc',
    7: 'MaterialType',
    8: 'MRPController',
    9: 'MRPControllerDesc',
    10: 'ProductionScheduler',
    11: 'ProductionSchedulerDesc',
    12: 'OrderNumber',
    13: 'OrderType',
    14: 'OrderTypeDesc',
    15: 'Operation',
    16: 'OperationDesc',
    17: 'PostingDate',
    18: 'EarnedLaborUnit',
    19: 'MachineTime',
    20: 'EarnedLaborTime',
    21: 'ActualQuantity',
    22: 'ActualScrapQty',
    23: 'TargetQuantity'
}

# 新格式文件列映射（27列格式，包含时间字段）
COLUMN_MAPPING_NEW = {
    0: 'Plant',
    1: 'WorkCenter',
    2: 'WorkCenterDesc',
    3: 'CostCenter',
    4: 'CostCenterDesc',
    5: 'Material',
    6: 'MaterialDesc',
    7: 'MaterialType',
    8: 'MRPController',
    9: 'MRPControllerDesc',
    10: 'ProductionScheduler',
    11: 'ProductionSchedulerDesc',
    12: 'OrderNumber',
    13: 'OrderType',
    14: 'OrderTypeDesc',
    15: 'Operation',
    16: 'OperationDesc',
    17: 'PostingDate',
    18: 'ActualStartTime',
    19: 'ActualFinishTime',
    20: 'ActualFinishDate',
    21: 'EarnedLaborUnit',
    22: 'MachineTime',
    23: 'EarnedLaborTime',
    24: 'ActualQuantity',
    25: 'ActualScrapQty',
    26: 'TargetQuantity'
}

# 数据库所有列
ALL_COLUMNS = [
    'Plant', 'WorkCenter', 'WorkCenterDesc', 'CostCenter', 'CostCenterDesc',
    'Material', 'MaterialDesc', 'MaterialType', 'MRPController', 'MRPControllerDesc',
    'ProductionScheduler', 'ProductionSchedulerDesc', 'OrderNumber', 'OrderType',
    'OrderTypeDesc', 'Operation', 'OperationDesc', 'PostingDate',
    'ActualStartTime', 'ActualFinishTime', 'ActualFinishDate',
    'EarnedLaborUnit', 'MachineTime', 'EarnedLaborTime',
    'ActualQuantity', 'ActualScrapQty', 'TargetQuantity', 'source_file', 'record_hash'
]


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )


def create_table(db: SQLServerOnlyManager, drop_existing: bool = False):
    """Ensure SQL Server table dbo.raw_sap_labor_hours exists."""
    with db.get_connection() as conn:
        cursor = conn.cursor()

        if drop_existing:
            cursor.execute("IF OBJECT_ID('dbo.raw_sap_labor_hours', 'U') IS NOT NULL TRUNCATE TABLE dbo.raw_sap_labor_hours;")
            conn.commit()

        cursor.execute(
            """
            IF OBJECT_ID('dbo.raw_sap_labor_hours', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.raw_sap_labor_hours (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    Plant NVARCHAR(50) NULL,
                    WorkCenter NVARCHAR(50) NULL,
                    WorkCenterDesc NVARCHAR(255) NULL,
                    CostCenter NVARCHAR(50) NULL,
                    CostCenterDesc NVARCHAR(255) NULL,
                    Material NVARCHAR(80) NULL,
                    MaterialDesc NVARCHAR(255) NULL,
                    MaterialType NVARCHAR(50) NULL,
                    MRPController NVARCHAR(50) NULL,
                    MRPControllerDesc NVARCHAR(255) NULL,
                    ProductionScheduler NVARCHAR(50) NULL,
                    ProductionSchedulerDesc NVARCHAR(255) NULL,
                    OrderNumber NVARCHAR(50) NULL,
                    OrderType NVARCHAR(50) NULL,
                    OrderTypeDesc NVARCHAR(255) NULL,
                    Operation NVARCHAR(50) NULL,
                    OperationDesc NVARCHAR(255) NULL,
                    PostingDate NVARCHAR(30) NULL,
                    ActualStartTime NVARCHAR(50) NULL,
                    ActualFinishTime NVARCHAR(50) NULL,
                    ActualFinishDate NVARCHAR(30) NULL,
                    EarnedLaborUnit NVARCHAR(20) NULL,
                    MachineTime FLOAT NULL,
                    EarnedLaborTime FLOAT NULL,
                    ActualQuantity FLOAT NULL,
                    ActualScrapQty FLOAT NULL,
                    TargetQuantity FLOAT NULL,
                    source_file NVARCHAR(260) NULL,
                    record_hash NVARCHAR(255) NULL,
                    created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
                    updated_at DATETIME2 NOT NULL DEFAULT GETDATE()
                );
            END
            """
        )
        conn.commit()

        # Add missing columns if the existing table is older
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'raw_sap_labor_hours'
            """
        )
        existing = {r[0] for r in cursor.fetchall()}
        desired = {
            'source_file': "NVARCHAR(260) NULL",
            'record_hash': "NVARCHAR(255) NULL",
        }
        for col, ddl in desired.items():
            if col not in existing:
                cursor.execute(f"ALTER TABLE dbo.raw_sap_labor_hours ADD {col} {ddl};")
        conn.commit()

    logger.info("Database table dbo.raw_sap_labor_hours ready (SQL Server)")


def read_excel_file(file_path):
    """读取 Excel 文件并标准化列名"""
    logger.info(f"读取文件: {file_path}")

    # 读取数据，跳过前7行
    df = pd.read_excel(file_path, skiprows=7)

    # 根据列数选择映射
    num_cols = len(df.columns)
    logger.info(f"文件列数: {num_cols}")

    if num_cols >= 27:
        # 新格式（27列）
        column_mapping = COLUMN_MAPPING_NEW
    else:
        # 旧格式（24列）
        column_mapping = COLUMN_MAPPING_LEGACY

    # 重命名列
    new_columns = []
    for i in range(len(df.columns)):
        if i in column_mapping:
            new_columns.append(column_mapping[i])
        else:
            new_columns.append(f'Unknown_{i}')

    df.columns = new_columns

    # 只保留需要的列 (暂时排除 record_hash，稍后生成)
    needed_cols = [c for c in ALL_COLUMNS if c != 'record_hash' and c != 'source_file']
    existing_cols = [col for col in needed_cols if col in df.columns]
    df = df[existing_cols].copy()

    # 添加缺失的列
    for col in needed_cols:
        if col not in df.columns:
            df[col] = None

    # 添加源文件
    df['source_file'] = os.path.basename(file_path)

    # 数据清洗
    df = clean_data(df)

    logger.info(f"读取完成，共 {len(df)} 条记录")
    return df


def clean_data(df):
    """数据清洗"""
    # 转换日期格式
    if 'PostingDate' in df.columns:
        df['PostingDate'] = pd.to_datetime(df['PostingDate'], errors='coerce').dt.strftime('%Y-%m-%d')

    if 'ActualFinishDate' in df.columns:
        df['ActualFinishDate'] = pd.to_datetime(df['ActualFinishDate'], errors='coerce').dt.strftime('%Y-%m-%d')

    # 转换时间字段为字符串，统一空值为空字符串（用于去重）
    if 'ActualStartTime' in df.columns:
        df['ActualStartTime'] = df['ActualStartTime'].astype(str)
        df['ActualStartTime'] = df['ActualStartTime'].replace(['NaT', 'nan', 'None', 'NaN', 'null', 'NULL'], '')
        df.loc[df['ActualStartTime'].isna(), 'ActualStartTime'] = ''

    if 'ActualFinishTime' in df.columns:
        df['ActualFinishTime'] = df['ActualFinishTime'].astype(str)
        df['ActualFinishTime'] = df['ActualFinishTime'].replace(['NaT', 'nan', 'None', 'NaN', 'null', 'NULL'], '')
        df.loc[df['ActualFinishTime'].isna(), 'ActualFinishTime'] = ''

    # 转换数值类型
    numeric_cols = ['MachineTime', 'EarnedLaborTime', 'ActualQuantity', 'ActualScrapQty', 'TargetQuantity']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 统一工时单位为小时（秒转换为小时）
    if 'EarnedLaborUnit' in df.columns:
        mask = df['EarnedLaborUnit'] == 's'
        if mask.any():
            df.loc[mask, 'MachineTime'] = df.loc[mask, 'MachineTime'] / 3600.0
            df.loc[mask, 'EarnedLaborTime'] = df.loc[mask, 'EarnedLaborTime'] / 3600.0
            df.loc[mask, 'EarnedLaborUnit'] = 'Hour'
            logger.info(f"已将 {mask.sum()} 条记录的工时单位从秒转换为小时")

    # 去除空订单号
    df = df[df['OrderNumber'].notna() & (df['OrderNumber'] != '')]

    # 标准化 OrderNumber 和 Operation 格式（去掉 .0 后缀）
    if 'OrderNumber' in df.columns:
        df['OrderNumber'] = df['OrderNumber'].astype(str).str.replace(r'\.0$', '', regex=True)
    if 'Operation' in df.columns:
        df['Operation'] = df['Operation'].astype(str).str.replace(r'\.0$', '', regex=True)

    # 生成 record_hash (OrderNumber|Operation|PostingDate|ActualStartTime)
    # 确保字段存在
    key_cols = ['OrderNumber', 'Operation', 'PostingDate', 'ActualStartTime']
    for col in key_cols:
        if col not in df.columns:
            df[col] = ''

    df['record_hash'] = (
        df['OrderNumber'].fillna('').astype(str) + '|' +
        df['Operation'].fillna('').astype(str) + '|' +
        df['PostingDate'].fillna('').astype(str) + '|' +
        df['ActualStartTime'].fillna('').astype(str)
    )

    # 内部去重（基于 record_hash）
    before_count = len(df)
    df = df.drop_duplicates(subset=['record_hash'], keep='last')
    after_count = len(df)
    if before_count > after_count:
        logger.info(f"内部去重: 移除 {before_count - after_count} 条重复记录")

    return df


def import_data(db, df: pd.DataFrame):
    """导入数据到数据库"""
    if df.empty:
        return 0

    inserted = db.merge_insert_by_hash(df, 'raw_sap_labor_hours', hash_column='record_hash')
    logger.info(f"导入完成: 新增 {inserted}")
    return inserted


def get_existing_count(db: SQLServerOnlyManager):
    """获取现有记录数 (SQL Server)"""
    return db.get_table_count('raw_sap_labor_hours', schema='dbo')


def import_all_files(rebuild=False):
    """导入所有文件"""
    db = get_db_manager()

    # 创建表
    create_table(db, drop_existing=rebuild)
    
    # 获取初始记录数
    initial_count = get_existing_count(db)
    logger.info(f"初始记录数: {initial_count}")
    
    # 文件列表
    files = [
        'EH_FY23.xlsx',
        'EH_FY24.xlsx',
        'EH_FY25.xlsx',
        'EH_FY26.xlsx',
        'YPP_M03_Q5003_00000.xlsx'
    ]
    
    # Resolve full paths
    file_paths = []
    for filename in files:
        p = SOURCE_PATH / filename
        if p.exists():
            file_paths.append(str(p))
        else:
            logger.warning(f"文件不存在: {p}")
            
    if rebuild:
        # If rebuilding, process all existing files
        to_process = file_paths
    else:
        # Filter unchanged files
        to_process = db.filter_changed_files("sap_labor_hours", file_paths)
        
    if not to_process:
        logger.info("所有文件均未发生变化，无需更新")
        return 0

    total_imported = 0
    
    for file_path in to_process:
        try:
            df = read_excel_file(file_path)
            imported = import_data(db, df)
            total_imported += imported
            
            # Mark as processed
            db.mark_file_processed("sap_labor_hours", file_path)
            
            logger.info(f"文件 {os.path.basename(file_path)} 处理完成")
        except Exception as e:
            logger.error(f"文件 {os.path.basename(file_path)} 导入失败: {e}")
    
    # 获取最终记录数
    final_count = get_existing_count(db)
    logger.info(f"导入完成！初始: {initial_count}, 最终: {final_count}, 新增: {final_count - initial_count}")
    
    return total_imported


def import_ypp_file():
    """仅导入 YPP 文件（增量更新）"""
    db = get_db_manager()
    create_table(db)
    
    # 查找最新的 YPP 文件
    ypp_files = list(SOURCE_PATH.glob('YPP*.xlsx'))
    if not ypp_files:
        logger.warning("未找到 YPP 文件")
        return 0
    
    # 选择最新的文件
    latest_ypp = max(ypp_files, key=lambda x: x.stat().st_mtime)
    latest_ypp_str = str(latest_ypp)
    
    # Check if changed
    changed = db.filter_changed_files("sap_labor_hours", [latest_ypp_str])
    if not changed:
        logger.info(f"最新 YPP 文件未发生变化，跳过: {latest_ypp.name}")
        return 0

    logger.info(f"找到最新 YPP 文件: {latest_ypp}")
    
    df = read_excel_file(latest_ypp)
    imported = import_data(db, df)
    
    # Mark as processed
    db.mark_file_processed("sap_labor_hours", latest_ypp_str)
    
    return imported


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='导入 SAP 人工工时数据')
    parser.add_argument('--ypp', action='store_true', help='仅导入 YPP 文件（增量更新）')
    parser.add_argument('--rebuild', action='store_true', help='重建表并重新导入所有数据')
    args = parser.parse_args()
    
    if args.ypp:
        # 仅导入 YPP 文件
        logger.info("开始增量导入 YPP 文件...")
        import_ypp_file()
    elif args.rebuild:
        # 重建表并导入所有文件
        logger.info("开始重建表并导入所有文件...")
        import_all_files(rebuild=True)
    else:
        # 导入所有文件（不删除旧数据）
        logger.info("开始导入所有文件...")
        import_all_files(rebuild=False)
