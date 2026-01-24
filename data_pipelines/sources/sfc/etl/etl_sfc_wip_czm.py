"""
SFC WIP 批次流转报表 ETL 脚本 (CZM)
从 Excel 文件导入 SFC WIP 数据到数据库
数据来源: SFC 系统导出的批次流转报表
注意: 仅包含 CZM 数据，不包含 CKH
"""

import pandas as pd
import os
import logging
import sys
from datetime import datetime
from pathlib import Path
import glob

PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 路径配置
SOURCE_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\70-SFC导出数据\批次流转报表')

# 列映射 (中文 -> 英文)
COLUMN_MAPPING = {
    '产品类型': 'product_type',
    '生产流转卡号': 'flow_card_no',
    '物料编码': 'material_code',
    '产品号': 'product_no',
    '产品名称': 'product_name',
    '批次': 'batch_no',
    '机台号': 'machine_no',
    '开批时间': 'batch_start_time',
    '当前工序号': 'current_operation_no',
    '当前工序名称': 'current_operation_name',
    '工序状态': 'operation_status',
    '计划数量': 'planned_qty',
    '合格数量 (上道工序合格数量)': 'qualified_qty',
    '上道结束时间': 'prev_end_time',
    '当前开工时间': 'current_start_time',
    'Check In': 'check_in',
    'Check In时间': 'check_in_time',
    '等待时间 (小时)': 'wait_hours',
    '标准LT': 'standard_lt',
    '冻结': 'frozen'
}


def get_db_manager():
    return SQLServerOnlyManager(
        sql_server=os.getenv("MDDAP_SQL_SERVER", r"localhost\SQLEXPRESS"),
        sql_db=os.getenv("MDDAP_SQL_DATABASE", "mddap_v2"),
        driver=os.getenv("MDDAP_SQL_DRIVER", "ODBC Driver 17 for SQL Server"),
    )


def create_table(db, rebuild: bool = False):
    """创建 WIP 表"""
    with db.get_connection() as conn:
        cursor = conn.cursor()

        if rebuild:
            cursor.execute("IF OBJECT_ID('dbo.raw_sfc_wip_czm', 'U') IS NOT NULL DROP TABLE dbo.raw_sfc_wip_czm;")

        cursor.execute(
            """
            IF OBJECT_ID('dbo.raw_sfc_wip_czm', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.raw_sfc_wip_czm (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    product_type NVARCHAR(255) NULL,
                    flow_card_no NVARCHAR(255) NULL,
                    material_code NVARCHAR(255) NULL,
                    product_no NVARCHAR(255) NULL,
                    product_name NVARCHAR(255) NULL,
                    batch_no NVARCHAR(255) NULL,
                    machine_no NVARCHAR(255) NULL,
                    batch_start_time NVARCHAR(255) NULL,
                    current_operation_no NVARCHAR(255) NULL,
                    current_operation_name NVARCHAR(255) NULL,
                    operation_status NVARCHAR(255) NULL,
                    planned_qty BIGINT NULL,
                    qualified_qty FLOAT NULL,
                    prev_end_time NVARCHAR(255) NULL,
                    current_start_time NVARCHAR(255) NULL,
                    check_in NVARCHAR(255) NULL,
                    check_in_time NVARCHAR(255) NULL,
                    wait_hours FLOAT NULL,
                    standard_lt FLOAT NULL,
                    frozen FLOAT NULL,
                    snapshot_date DATE NULL,
                    source_file NVARCHAR(260) NULL,
                    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
                );

                CREATE INDEX idx_wip_czm_batch ON dbo.raw_sfc_wip_czm(batch_no);
                CREATE INDEX idx_wip_czm_material ON dbo.raw_sfc_wip_czm(material_code);
                CREATE INDEX idx_wip_czm_snapshot ON dbo.raw_sfc_wip_czm(snapshot_date);
                CREATE INDEX idx_wip_czm_flow_card ON dbo.raw_sfc_wip_czm(flow_card_no);
            END
            """
        )

        conn.commit()

    logger.info('WIP 表创建完成')


def parse_snapshot_date(filename):
    """从文件名解析快照日期"""
    # WIP-20251214080001.xlsx -> 2025-12-14
    try:
        basename = os.path.basename(filename)
        date_str = basename.split('-')[1][:8]
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    except:
        return None


def clean_data(df, source_file):
    """清洗数据"""
    # 重命名列
    df = df.rename(columns=COLUMN_MAPPING)
    
    # 添加快照日期和源文件
    snapshot_date = pd.to_datetime(parse_snapshot_date(source_file), errors='coerce')
    df['snapshot_date'] = snapshot_date.date() if pd.notnull(snapshot_date) else None
    df['source_file'] = os.path.basename(source_file)
    
    # 转换数据类型
    df['flow_card_no'] = df['flow_card_no'].astype(str)
    df['material_code'] = df['material_code'].astype(str).str.strip()
    df['batch_no'] = df['batch_no'].astype(str).str.strip()
    
    # 处理数值字段
    numeric_cols = ['planned_qty', 'qualified_qty', 'wait_hours', 'standard_lt', 'frozen']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df


def get_processed_files(db):
    """获取已处理的文件列表"""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT source_file FROM dbo.raw_sfc_wip_czm WHERE source_file IS NOT NULL")
            return {str(r[0]) for r in cur.fetchall() if r and r[0] is not None}
    except Exception:
        return set()


def import_file(db, filepath):
    """导入单个文件"""
    try:
        df = pd.read_excel(filepath)
        df = clean_data(df, filepath)
        
        # 使用 bulk_insert (append 模式)
        inserted = db.bulk_insert(df, 'raw_sfc_wip_czm', if_exists='append')
        
        return inserted
    except Exception as e:
        logger.error(f'导入文件失败 {filepath}: {str(e)}')
        return 0


def import_latest_only(db):
    """仅导入最新的 WIP 文件（用于日常快照）"""
    files = sorted(glob.glob(str(SOURCE_PATH / 'WIP-*.xlsx')))
    if not files:
        logger.warning('未找到 WIP 文件')
        return 0
    
    latest_file = files[-1]
    logger.info(f'导入最新文件: {os.path.basename(latest_file)}')
    
    # 检查是否已导入
    processed = get_processed_files(db)
    if os.path.basename(latest_file) in processed:
        logger.info('最新文件已导入，跳过')
        return 0
    
    count = import_file(db, latest_file)
    logger.info(f'导入 {count} 条记录')
    return count


def import_all_files(db, limit=None):
    """导入所有 WIP 文件"""
    files = sorted(glob.glob(str(SOURCE_PATH / 'WIP-*.xlsx')))
    if not files:
        logger.warning('未找到 WIP 文件')
        return 0
    
    processed = get_processed_files(db)
    total_count = 0
    
    files_to_process = [f for f in files if os.path.basename(f) not in processed]
    if limit:
        files_to_process = files_to_process[:limit]
    
    logger.info(f'待处理文件: {len(files_to_process)} / {len(files)}')
    
    for i, filepath in enumerate(files_to_process):
        logger.info(f'[{i+1}/{len(files_to_process)}] 处理: {os.path.basename(filepath)}')
        count = import_file(db, filepath)
        total_count += count
    
    return total_count


def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description='导入 SFC WIP 数据 (CZM)')
    parser.add_argument('--mode', choices=['latest', 'all', 'init'], default='latest',
                       help='导入模式: latest=仅最新, all=全部增量, init=重建表并导入全部')
    parser.add_argument('--limit', type=int, help='限制导入文件数量')
    args = parser.parse_args()
    
    logger.info(f'开始导入 SFC WIP 数据 (CZM), 模式: {args.mode}')
    
    db = get_db_manager()

    if args.mode == 'init':
        create_table(db, rebuild=True)
        total = import_all_files(db, args.limit)
    elif args.mode == 'all':
        create_table(db, rebuild=False)
        total = import_all_files(db, args.limit)
    else:
        create_table(db, rebuild=False)
        total = import_latest_only(db)
    
    # 统计
    total_records = db.get_table_count('raw_sfc_wip_czm')
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(DISTINCT snapshot_date) as cnt FROM dbo.raw_sfc_wip_czm')
            r = cur.fetchone()
            total_snapshots = int(r[0]) if r and r[0] is not None else 0

            cur.execute('SELECT MIN(snapshot_date) as min_d, MAX(snapshot_date) as max_d FROM dbo.raw_sfc_wip_czm')
            r2 = cur.fetchone()
            date_range = (r2[0], r2[1]) if r2 else (None, None)
    except Exception:
        total_snapshots = 0
        date_range = (None, None)
    
    print(f'\n导入完成:')
    print(f'  本次导入: {total} 条')
    print(f'  总记录数: {total_records:,}')
    print(f'  快照天数: {total_snapshots}')
    print(f'  日期范围: {date_range[0]} ~ {date_range[1]}')
    
    logger.info('SFC WIP 数据导入完成!')


if __name__ == '__main__':
    main()
