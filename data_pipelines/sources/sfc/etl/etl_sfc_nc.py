"""
SFC 不合格异常数据 ETL 脚本
数据源: SFC 导出的不合格异常记录
表名: raw_sfc_nc
更新模式: 增量导入，按唯一键去重
"""

import os
import sys
import time
import glob
import logging
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = str(Path(__file__).resolve().parents[4])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from shared_infrastructure.utils.etl_utils import read_sharepoint_excel
from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

# 配置
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = Path(__file__).resolve().parents[4]
SOURCE_PATH = Path(r'C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\70-SFC导出数据\不合格异常')

LOG_DIR = os.path.join(project_root, "shared_infrastructure", "logs", "sfc")

os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"etl_sfc_nc_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# 列映射 (中文 -> 英文)
COLUMN_MAPPING = {
    '检验类型': 'inspection_type',
    '批次号': 'batch_no',
    '产品编号': 'product_no',
    '产品类型': 'product_type',
    '机床编号': 'machine_no',
    '工序编号': 'operation_no',
    '工序名称': 'operation_name',
    '工序状态': 'operation_status',
    '产品序号': 'product_sn',
    '特征编码': 'feature_code',
    '特征名称': 'feature_name',
    '检验方法': 'inspection_method',
    '异常原因': 'nc_reason',
    '行动措施': 'action_taken',
    '异常数量': 'nc_qty',
    '记录人': 'recorder',
    '记录时间': 'record_time',
    '不合格现象描述': 'nc_description'
}


def get_db_manager() -> SQLServerOnlyManager:
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )


def _ensure_raw_sfc_nc_schema(db: SQLServerOnlyManager) -> None:
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            IF OBJECT_ID('dbo.raw_sfc_nc', 'U') IS NULL
            BEGIN
                CREATE TABLE dbo.raw_sfc_nc (
                    id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
                    inspection_type NVARCHAR(200) NULL,
                    batch_no NVARCHAR(200) NULL,
                    product_no NVARCHAR(200) NULL,
                    product_type NVARCHAR(200) NULL,
                    machine_no NVARCHAR(200) NULL,
                    operation_no NVARCHAR(200) NULL,
                    operation_name NVARCHAR(200) NULL,
                    operation_status NVARCHAR(200) NULL,
                    product_sn NVARCHAR(200) NULL,
                    feature_code NVARCHAR(200) NULL,
                    feature_name NVARCHAR(200) NULL,
                    inspection_method NVARCHAR(200) NULL,
                    nc_reason NVARCHAR(500) NULL,
                    action_taken NVARCHAR(500) NULL,
                    nc_qty INT NULL,
                    recorder NVARCHAR(200) NULL,
                    record_time NVARCHAR(100) NULL,
                    nc_description NVARCHAR(MAX) NULL,
                    source_file NVARCHAR(500) NULL,
                    record_hash NVARCHAR(512) NULL,
                    created_at DATETIME2 NULL
                );
            END
            """
        )
        conn.commit()

        cur.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'raw_sfc_nc'
            """
        )
        cols = {r[0] for r in cur.fetchall()}

        if 'record_hash' not in cols:
            cur.execute("ALTER TABLE dbo.raw_sfc_nc ADD record_hash NVARCHAR(512) NULL")
            conn.commit()

        cur.execute(
            """
            UPDATE dbo.raw_sfc_nc
            SET record_hash = CONCAT(
                COALESCE(batch_no, ''), '|',
                COALESCE(product_sn, ''), '|',
                COALESCE(feature_code, ''), '|',
                COALESCE(record_time, '')
            )
            WHERE record_hash IS NULL OR record_hash = ''
            """
        )
        conn.commit()

        order_col = None
        for candidate in ['id', 'created_at', 'record_time']:
            if candidate in cols:
                order_col = candidate
                break
        order_clause = f"ORDER BY [{order_col}]" if order_col else "ORDER BY (SELECT 1)"

        cur.execute(
            f"""
            ;WITH d AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY record_hash {order_clause}) AS rn
                FROM dbo.raw_sfc_nc
                WHERE record_hash IS NOT NULL
            )
            DELETE FROM d WHERE rn > 1;
            """
        )
        conn.commit()

        cur.execute(
            """
            IF NOT EXISTS (
                SELECT 1
                FROM sys.indexes
                WHERE name = 'ux_raw_sfc_nc_record_hash'
                  AND object_id = OBJECT_ID('dbo.raw_sfc_nc')
            )
            BEGIN
                CREATE UNIQUE INDEX ux_raw_sfc_nc_record_hash
                ON dbo.raw_sfc_nc (record_hash);
            END
            """
        )
        conn.commit()


def clean_data(df, source_file):
    """清洗数据"""
    # 重命名列
    df = df.rename(columns=COLUMN_MAPPING)
    
    df['source_file'] = source_file
    
    # 处理数值字段
    if 'nc_qty' in df.columns:
        df['nc_qty'] = pd.to_numeric(df['nc_qty'], errors='coerce')
    
    if 'record_time' in df.columns:
        df['record_time'] = df['record_time'].astype(str)
        
    # 生成 record_hash (batch_no|product_sn|feature_code|record_time)
    # 确保字段存在
    key_cols = ['batch_no', 'product_sn', 'feature_code', 'record_time']
    for col in key_cols:
        if col not in df.columns:
            df[col] = ''
            
    df['record_hash'] = (
        df['batch_no'].fillna('').astype(str) + '|' +
        df['product_sn'].fillna('').astype(str) + '|' +
        df['feature_code'].fillna('').astype(str) + '|' +
        df['record_time'].fillna('').astype(str)
    )
    
    if 'nc_qty' in df.columns:
        df['nc_qty'] = pd.to_numeric(df['nc_qty'], errors='coerce').astype('Int64')

    df['created_at'] = datetime.now()
    target_cols = [
        'inspection_type', 'batch_no', 'product_no', 'product_type', 'machine_no',
        'operation_no', 'operation_name', 'operation_status', 'product_sn',
        'feature_code', 'feature_name', 'inspection_method', 'nc_reason',
        'action_taken', 'nc_qty', 'recorder', 'record_time', 'nc_description',
        'source_file', 'record_hash', 'created_at'
    ]
    existing = [c for c in target_cols if c in df.columns]
    return df[existing]


def _read_nc_files(
    mode: str = 'all',
    max_new_files: int = 20,
) -> List[str]:
    pattern = str(SOURCE_PATH / 'NC-*.xlsx')
    file_paths = glob.glob(pattern)
    if not file_paths:
        logger.warning('未找到不合格异常文件')
        return []

    db = get_db_manager()
    changed_files = db.filter_changed_files('sfc_nc', file_paths)
    changed_files = sorted(changed_files, key=lambda p: os.path.getmtime(p), reverse=True)

    if mode == 'latest':
        return changed_files[:1]

    if max_new_files and max_new_files > 0:
        changed_files = changed_files[:max_new_files]
        logger.info(f"本次最多处理 {len(changed_files)} 个文件（max_new_files={max_new_files}）")

    return changed_files


def _save_to_database(df: pd.DataFrame) -> Dict[str, int]:
    if df is None or df.empty:
        return {"inserted": 0, "skipped": 0}

    db = get_db_manager()

    original = len(df)
    df = df.drop_duplicates(subset=['record_hash'], keep='first')
    internal_dups = original - len(df)
    if internal_dups > 0:
        logger.info(f"内部去重: 移除 {internal_dups} 条重复记录")

    inserted = db.merge_insert_by_hash(df, 'raw_sfc_nc', hash_column='record_hash')
    db_dups = len(df) - inserted
    if db_dups > 0:
        logger.info(f"数据库去重: 跳过 {db_dups} 条已存在记录")

    return {"inserted": inserted, "skipped": internal_dups + db_dups}


def main():
    import argparse

    parser = argparse.ArgumentParser(description='导入 SFC 不合格异常数据')
    parser.add_argument('--mode', choices=['latest', 'all'], default='all')
    parser.add_argument('--max-new-files', type=int, default=20)
    parser.add_argument('--max-rows-per-file', type=int, default=0)
    args = parser.parse_args()

    logger.info('=' * 60)
    logger.info('SFC 不合格异常数据 ETL 启动')
    logger.info('=' * 60)

    t0 = time.time()
    db = get_db_manager()
    _ensure_raw_sfc_nc_schema(db)

    files_to_process = _read_nc_files(mode=args.mode, max_new_files=args.max_new_files)
    if not files_to_process:
        logger.info('没有新数据需要处理')
        return

    total_read = 0
    total_inserted = 0
    total_skipped = 0
    failed_files: List[str] = []

    for file_path in files_to_process:
        try:
            logger.info(f"读取: {os.path.basename(file_path)}")
            rows_limit: Optional[int] = args.max_rows_per_file if args.max_rows_per_file and args.max_rows_per_file > 0 else None
            df_file = read_sharepoint_excel(file_path, max_rows=rows_limit)
            df_file = clean_data(df_file, file_path)

            total_read += len(df_file)
            stats = _save_to_database(df_file)
            total_inserted += stats['inserted']
            total_skipped += stats['skipped']

            db.mark_file_processed('sfc_nc', file_path)
            logger.info(
                f"文件写入完成: {os.path.basename(file_path)} 插入 {stats['inserted']} 条, 跳过 {stats['skipped']} 条"
            )
        except Exception as e:
            logger.error(f"文件处理失败 {os.path.basename(file_path)}: {e}")
            failed_files.append(file_path)

    logger.info(f"写入完成: 插入 {total_inserted} 条, 跳过 {total_skipped} 条")
    if failed_files:
        raise RuntimeError(f"{len(failed_files)} files failed: {[os.path.basename(p) for p in failed_files[:5]]}")

    count = db.get_table_count('raw_sfc_nc')
    logger.info(f"数据库 raw_sfc_nc 表当前记录数: {count}")
    logger.info(f"处理完成，耗时: {time.time() - t0:.2f} 秒")


if __name__ == '__main__':
    main()
