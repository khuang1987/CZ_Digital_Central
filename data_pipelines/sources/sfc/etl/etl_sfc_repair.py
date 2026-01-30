"""
SFC 维修记录 ETL 脚本
从 Excel 文件导入设备维修记录数据到数据库
数据来源: SFC 系统导出的维修记录
"""

import pandas as pd
import os
import logging
from datetime import datetime
from pathlib import Path
import glob
import sys

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
SOURCE_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\70-SFC导出数据\维修记录')

# 列映射 (原始列名 -> 英文字段名)
COLUMN_MAPPING = {
    'No.\n序号': 'seq_no',
    'Plant\n工厂': 'plant',
    'Orde\n订单': 'order_no',
    'Technical identification number\n技术标识号': 'tech_id',
    'Technical object description\n技术对象描述': 'tech_object_desc',
    'Actual order date Month/day\n实际下达日期': 'actual_order_date',
    'Basic start time\n基本开始时间': 'basic_start_time',
    'Reference date Month/day\n参考日期': 'reference_date',
    'Reference time\n参考时间': 'reference_time',
    'Failure description\n故障描述': 'failure_desc',
    'Repair personnel\n维修人员': 'repair_personnel',
    'Repair Start Time\n修理开始时间月/日': 'repair_start_time',
    'Repair finish Time\n修理完成时间月/日': 'repair_finish_time',
    'Fault Cause \n故障原因': 'fault_cause',
    'Repair Contents\n维修内容': 'repair_contents',
    'Replacement Accessories\n配件': 'replacement_parts',
    'Down Status\n停机状态': 'down_status',
    'Downtime\n停机时长\n（H）': 'downtime_hours',
    'Repair Duration\n维修工时\n（H）': 'repair_duration_hours',
    'maintenance mode\n维修方式': 'maintenance_mode',
    'Fault symptom differentiation\n现象区分': 'fault_symptom',
    'Verification number\n验证编号': 'verification_no',
    'Application Department Confirmation/personnel\n申请部门确认人': 'confirm_personnel',
    'Application Department Confirmation/Date\n申请部门确认/日期': 'confirm_date',
    'Application Department Confirmation/Date\n申请部门确认/时间': 'confirm_time'
}


def get_db_manager():
    return SQLServerOnlyManager()


def create_table(db):
    """创建维修记录表"""
    with db.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            IF OBJECT_ID('dbo.raw_sfc_repair', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.raw_sfc_repair (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    seq_no INT NULL,
                    plant INT NULL,
                    order_no INT NULL,
                    tech_id NVARCHAR(MAX) NULL,
                    tech_object_desc NVARCHAR(MAX) NULL,
                    actual_order_date NVARCHAR(MAX) NULL,
                    basic_start_time NVARCHAR(MAX) NULL,
                    reference_date NVARCHAR(MAX) NULL,
                    reference_time NVARCHAR(MAX) NULL,
                    failure_desc NVARCHAR(MAX) NULL,
                    repair_personnel NVARCHAR(MAX) NULL,
                    repair_start_time NVARCHAR(MAX) NULL,
                    repair_finish_time NVARCHAR(MAX) NULL,
                    fault_cause NVARCHAR(MAX) NULL,
                    repair_contents NVARCHAR(MAX) NULL,
                    replacement_parts NVARCHAR(MAX) NULL,
                    down_status NVARCHAR(MAX) NULL,
                    downtime_hours FLOAT NULL,
                    repair_duration_hours FLOAT NULL,
                    maintenance_mode NVARCHAR(MAX) NULL,
                    fault_symptom NVARCHAR(MAX) NULL,
                    verification_no NVARCHAR(MAX) NULL,
                    confirm_personnel NVARCHAR(MAX) NULL,
                    confirm_date NVARCHAR(MAX) NULL,
                    confirm_time NVARCHAR(MAX) NULL,
                    updated_at DATETIME2 NULL,
                    source_file NVARCHAR(MAX) NULL,
                    created_at DATETIME2 NULL
                );
            END
            """
        )
        conn.commit()

    logger.info('维修记录表检查完成 (SQL Server)')


def parse_snapshot_date(filename):
    """从文件名解析快照日期"""
    # Dev-20251214080001.xlsx -> 2025-12-14
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

    # 添加源文件
    df['source_file'] = os.path.basename(source_file)

    # 处理数值字段
    numeric_cols = ['seq_no', 'plant', 'order_no', 'downtime_hours', 'repair_duration_hours']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def get_processed_files(db):
    """获取已处理的文件列表"""
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT source_file FROM dbo.raw_sfc_repair WHERE source_file IS NOT NULL")
            return {r[0] for r in cursor.fetchall() if r and r[0]}
    except:
        return set()


def _delete_existing_orders(db, order_nos: list[int]) -> int:
    if not order_nos:
        return 0

    deleted = 0
    with db.get_connection() as conn:
        cursor = conn.cursor()
        chunk_size = 800
        for i in range(0, len(order_nos), chunk_size):
            chunk = order_nos[i:i + chunk_size]
            placeholders = ",".join(["?" for _ in chunk])
            cursor.execute(
                f"DELETE FROM dbo.raw_sfc_repair WHERE order_no IN ({placeholders})",
                chunk,
            )
            try:
                deleted += int(cursor.rowcount or 0)
            except Exception:
                pass
        conn.commit()
    return deleted


def import_file(db, filepath):
    """导入单个文件（增量更新模式，按 order_no 去重）"""
    try:
        df = pd.read_excel(filepath)
        df = clean_data(df, filepath)

        if df.empty:
            return 0

        order_nos = (
            df.get('order_no', pd.Series([], dtype='float64'))
            .dropna()
            .astype(int)
            .tolist()
        )
        _delete_existing_orders(db, order_nos)

        db.bulk_insert(df, 'raw_sfc_repair')
        return len(df)
    except Exception as e:
        logger.error(f'导入文件失败 {filepath}: {str(e)}')
        return 0


def import_latest_only(db, force=False):
    """导入最新的维修记录文件（增量更新模式）"""
    files = sorted(glob.glob(str(SOURCE_PATH / 'Dev-*.xlsx')))
    if not files:
        logger.warning('未找到维修记录文件')
        return 0

    latest_file = files[-1]
    
    # Check if latest file has changed
    if not force:
        changed = db.filter_changed_files("sfc_repair", [latest_file])
        if not changed:
            logger.info(f"最新文件未发生变化，跳过: {os.path.basename(latest_file)}")
            return 0
    else:
        logger.info(f"强制刷新模式: {os.path.basename(latest_file)}")

    logger.info(f'导入最新文件: {os.path.basename(latest_file)}')

    # 增量更新
    count = import_file(db, latest_file)
    
    # Mark as processed if successful (even if count=0, it means processed but maybe empty)
    db.mark_file_processed("sfc_repair", latest_file)

    logger.info(f'处理完成，写入 {count} 条')
    return count


def import_latest_file(db, force=False):
    """导入最新文件（用于 init 模式）"""
    # Just an alias wrapper or specific logic if distinct from import_latest_only
    # In V1 logic, it called import_file which was insert or replace.
    return import_latest_only(db, force=force)


def import_all_files(db, limit=None):
    """导入所有维修记录文件（增量模式，按文件去重 + 数据去重）"""
    files = sorted(glob.glob(str(SOURCE_PATH / 'Dev-*.xlsx')))
    if not files:
        logger.warning('未找到维修记录文件')
        return 0

    # Use robust state tracking instead of just checking source_file existence
    # processed = get_processed_files(db) 
    # files_to_process = [f for f in files if os.path.basename(f) not in processed]
    
    files_to_process = db.filter_changed_files("sfc_repair", files)

    if not files_to_process:
        logger.info("所有文件均未发生变化，无需更新")
        return 0

    if limit:
        files_to_process = files_to_process[:limit]
    
    logger.info(f'待处理文件: {len(files_to_process)} / {len(files)}')
    
    total_new = 0
    for i, filepath in enumerate(files_to_process):
        logger.info(f'[{i+1}/{len(files_to_process)}] 处理: {os.path.basename(filepath)}')
        written = import_file(db, filepath)
        total_new += written
        
        # Mark as processed
        db.mark_file_processed("sfc_repair", filepath)
        
        if (i + 1) % 50 == 0:
            logger.info(f'已处理 {i+1} 个文件，累计新增 {total_new} 条')
    
    return total_new


def main():
    """主函数"""
    import argparse
    parser = argparse.ArgumentParser(description='导入 SFC 维修记录数据')
    parser.add_argument('--mode', choices=['latest', 'all', 'init'], default='latest',
                       help='导入模式: latest=仅最新, all=全部增量, init=重建表并导入全部')
    parser.add_argument('--limit', type=int, help='限制导入文件数量')
    parser.add_argument('--force', action='store_true', help='强制刷新')
    args = parser.parse_args()
    
    logger.info(f'开始导入 SFC 维修记录数据, 模式: {args.mode}')
    
    db = get_db_manager()
    
    if args.mode == 'init':
        create_table(db)
        total = import_all_files(db, args.limit)
    elif args.mode == 'all':
        total = import_all_files(db, args.limit)
    else:
        # latest 模式
        total = import_latest_only(db, force=args.force)
    
    # 统计
    total_records = db.get_table_count('raw_sfc_repair')
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(actual_order_date) as min_d, MAX(actual_order_date) as max_d FROM dbo.raw_sfc_repair")
            row = cursor.fetchone()
            date_range = (row[0], row[1]) if row else (None, None)
    except:
        date_range = (None, None)
    
    print(f'\n导入完成:')
    print(f'  本次新增: {total} 条')
    print(f'  总记录数: {total_records:,}')
    print(f'  订单日期范围: {date_range[0]} ~ {date_range[1]}')
    
    logger.info('SFC 维修记录数据导入完成!')


if __name__ == '__main__':
    main()
