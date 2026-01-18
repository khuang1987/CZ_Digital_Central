"""
SFC 批次报工原始数据 ETL 脚本 (V2 架构)
功能：从 Excel 读取 SFC 批次报工数据，简单清洗后存入数据库原始表
"""

import os
import sys
import time
import logging
import glob
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = str(Path(__file__).resolve().parents[4])

if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd

from shared_infrastructure.utils.etl_utils import (
    load_config,
    read_sharepoint_excel,
)
from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

# 配置
CONFIG_PATH = os.path.join(current_dir, "..", "config", "config_sfc_batch_report.yaml")
LOG_DIR = os.path.join(project_root, "shared_infrastructure", "logs", "sfc")

# 设置日志
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"etl_sfc_batch_output_raw_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def get_db_manager() -> SQLServerOnlyManager:
    """获取 SQL Server 数据库管理器（只写 SQL Server）"""
    return SQLServerOnlyManager(
        sql_server=r"localhost\SQLEXPRESS",
        sql_db="mddap_v2",
        driver="ODBC Driver 17 for SQL Server",
    )


def read_sfc_files(
    cfg: Dict[str, Any],
    test_mode: bool = False,
    max_files: int = 3,
    max_rows: int = 1000,
    max_new_files: int = 20,
    max_rows_per_file: int = 0,
) -> List[str]:
    """
    读取 SFC Excel 文件
    
    Args:
        cfg: 配置字典
        test_mode: 测试模式，只处理部分文件
        max_files: 测试模式下最大文件数
    """
    sfc_path = cfg.get("source", {}).get("sfc_path", "")
    if not sfc_path:
        logging.error("未找到 sfc_path 配置")
        return []
    
    db = get_db_manager()
    file_paths = glob.glob(sfc_path)
    # Normalize paths to absolute and standard format
    file_paths = [os.path.normpath(os.path.abspath(p)) for p in file_paths]
    
    if not file_paths:
        logging.warning(f"未找到匹配的文件: {sfc_path}")
        return []
    
    # 测试模式：只取最新的 max_files 个文件
    if test_mode:
        file_paths = sorted(file_paths, reverse=True)[:max_files]
        logging.info(f"测试模式：只处理最新 {len(file_paths)} 个文件，每文件最多 {max_rows} 行")
    
    # 过滤未变化的文件
    changed_files = db.filter_changed_files("sfc_batch_output_raw", file_paths)

    # 始终按最新到最旧处理（即使 max_new_files=0 也保持顺序）
    changed_files = sorted(changed_files, key=lambda p: os.path.getmtime(p), reverse=True)

    # 非测试模式：限制每次最多处理 N 个“未导入/有变化”的文件
    if (not test_mode) and max_new_files and max_new_files > 0:
        changed_files = changed_files[:max_new_files]
        logging.info(f"本次最多处理 {len(changed_files)} 个文件（max_new_files={max_new_files}）")
    
    if not changed_files:
        logging.info("所有文件未变化，跳过")
        return []
    
    logging.info(f"{len(changed_files)} 个文件待处理")
    return changed_files


def clean_sfc_data(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """简单清洗 SFC 数据"""
    if df.empty:
        return df
    
    result = df.copy()
    
    # 字段映射（统一命名规范）
    column_mapping = {
        # 从配置文件映射
        '产品号': 'CFN',
        '批次': 'BatchNumber',
        '工序号': 'Operation',
        '工序名称': 'OperationDesc',
        'Check In 时间': 'TrackInTime',  # 统一命名为TrackInTime
        '机台号': 'Machine',
        '报工时间': 'TrackOutTime',
        'Check In': 'TrackInOperator',  # 开工人
        '上道工序报工时间': 'EnterStepTime',
        '报工人': 'TrackOutOperator',  # 报工人
        '合格数量': 'TrackOutQty',
        '报废数量': 'ScrapQty',
        # 英文列名标准化
        'CheckInTime': 'TrackInTime',
        'CheckinTime': 'TrackInTime',
        'machine': 'Machine',
        'TrackOutQuantity': 'TrackOutQty',
        'ScrapQuantity': 'ScrapQty',
        'CheckIn_User': 'TrackInOperator',
        'CheckinUser': 'TrackInOperator',
        'TrackOut_User': 'TrackOutOperator',
        'TrackOutUser': 'TrackOutOperator',
        'Operation description': 'OperationDesc',
    }
    
    rename_dict = {k: v for k, v in column_mapping.items() if k in result.columns}
    if rename_dict:
        result = result.rename(columns=rename_dict)

    if 'BatchNumber' in result.columns:
        bn = result['BatchNumber'].astype('string').str.strip()
        keep_mask = ~bn.str.contains(r"-0\d+$", na=False, regex=True)
        result = result.loc[keep_mask].copy()
        result['BatchNumber'] = bn.loc[keep_mask]
    
    # 时间字段转换
    time_columns = ['TrackInTime', 'TrackOutTime', 'EnterStepTime']
    for col in time_columns:
        if col in result.columns:
            result[col] = pd.to_datetime(result[col], errors='coerce')

    if 'Operation' in result.columns:
        op_num = pd.to_numeric(result['Operation'], errors='coerce')
        op_num = op_num.where(op_num.isna() | (op_num == op_num.round()))
        result['Operation'] = op_num.round().astype('Int64')
    
    # 生成 record_hash
    key_fields = ['BatchNumber', 'Operation', 'TrackOutTime']
    available_keys = [k for k in key_fields if k in result.columns]
    
    if available_keys:
        hash_df = result[available_keys].copy()
        for c in hash_df.columns:
            hash_df[c] = hash_df[c].astype('string').fillna('')
        record_hash = hash_df.iloc[:, 0]
        for col in hash_df.columns[1:]:
            record_hash = record_hash + '|' + hash_df[col]
        result['record_hash'] = record_hash
    
    # 选择目标列（统一命名）
    target_columns = [
        'BatchNumber', 'Operation', 'OperationDesc', 'Machine', 'CFN',
        'TrackInTime', 'TrackOutTime', 'EnterStepTime',
        'TrackOutQty', 'ScrapQty',
        'TrackInOperator', 'TrackOutOperator',
        'source_file', 'record_hash'
    ]
    
    existing_columns = [c for c in target_columns if c in result.columns]
    result = result[existing_columns]
    
    logging.info(f"清洗完成: {len(result)} 行, {len(existing_columns)} 列")
    
    return result


def save_to_database(df: pd.DataFrame, table_name: str = "raw_sfc") -> Dict[str, int]:
    """保存数据到数据库"""
    if df.empty:
        return {"inserted": 0, "skipped": 0}
    
    db = get_db_manager()
    
    # 1. 先对 DataFrame 内部去重（保留第一条）
    original_count = len(df)
    df = df.drop_duplicates(subset=['record_hash'], keep='first')
    internal_dups = original_count - len(df)
    if internal_dups > 0:
        logging.info(f"内部去重: 移除 {internal_dups} 条重复记录")
    
    inserted = db.merge_insert_by_hash(df, table_name, hash_column="record_hash")

    db_dups = len(df) - inserted
    if db_dups > 0:
        logging.info(f"数据库去重: 跳过 {db_dups} 条已存在记录")

    skipped = internal_dups + db_dups
    return {"inserted": inserted, "skipped": skipped}


def main(
    test_mode: bool = False,
    max_files: int = 3,
    max_rows: int = 1000,
    max_new_files: int = 20,
    max_rows_per_file: int = 0,
):
    logging.info("=" * 60)
    logging.info("SFC 批次报工原始数据 ETL (V2) 启动")
    if test_mode:
        logging.info(f"*** 测试模式：最多 {max_files} 个文件，每文件最多 {max_rows} 行 ***")
    logging.info("=" * 60)
    
    t0 = time.time()
    
    try:
        cfg = load_config(CONFIG_PATH)
        if not cfg:
            logging.error("配置加载失败")
            return
        
        files_to_process = read_sfc_files(
            cfg,
            test_mode=test_mode,
            max_files=max_files,
            max_rows=max_rows,
            max_new_files=max_new_files,
            max_rows_per_file=max_rows_per_file,
        )

        if not files_to_process:
            logging.info("没有新数据需要处理")
            return

        total_read = 0
        total_inserted = 0
        total_skipped = 0
        db = get_db_manager()

        failed_files: List[str] = []

        for file_path in files_to_process:
            try:
                logging.info(f"读取: {os.path.basename(file_path)}")
                rows_limit = max_rows if test_mode else (max_rows_per_file if max_rows_per_file and max_rows_per_file > 0 else None)
                df_file = read_sharepoint_excel(file_path, max_rows=rows_limit)

                df_file["source_file"] = file_path
                total_read += len(df_file)

                df_clean = clean_sfc_data(df_file, cfg)
                stats = save_to_database(df_clean, "raw_sfc")

                total_inserted += stats["inserted"]
                total_skipped += stats["skipped"]

                db.mark_file_processed("sfc_batch_output_raw", file_path)
                logging.info(
                    f"文件写入完成: {os.path.basename(file_path)} 插入 {stats['inserted']} 条, 跳过 {stats['skipped']} 条"
                )
            except Exception as e:
                logging.error(f"文件处理失败 {os.path.basename(file_path)}: {e}")
                failed_files.append(file_path)

        logging.info(f"写入完成: 插入 {total_inserted} 条, 跳过 {total_skipped} 条")

        if failed_files:
            raise RuntimeError(f"{len(failed_files)} files failed: {[os.path.basename(p) for p in failed_files[:5]]}")
        
        db = get_db_manager()
        db.log_etl_run(
            etl_name='sfc_batch_output_raw',
            status='success',
            records_read=total_read,
            records_inserted=total_inserted,
            records_skipped=total_skipped
        )
        
        count = db.get_table_count("raw_sfc")
        logging.info(f"数据库 raw_sfc 表当前记录数: {count}")
        
    except Exception as e:
        logging.exception(f"ETL 失败: {e}")
        raise
    
    logging.info(f"处理完成，耗时: {time.time() - t0:.2f} 秒")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='SFC 批次报工原始数据 ETL')
    parser.add_argument('--test', action='store_true', help='测试模式，只处理部分文件')
    parser.add_argument('--max-files', type=int, default=3, help='测试模式下最大文件数')
    parser.add_argument('--max-rows', type=int, default=1000, help='测试模式下每文件最大行数')
    parser.add_argument('--max-new-files', type=int, default=20, help='非测试模式：每次最多处理的未导入/变化文件数（按最新优先）')
    parser.add_argument('--max-rows-per-file', type=int, default=0, help='非测试模式：每个文件最多读取行数（0=不限制，用于快速验证）')
    args = parser.parse_args()
    main(
        test_mode=args.test,
        max_files=args.max_files,
        max_rows=args.max_rows,
        max_new_files=args.max_new_files,
        max_rows_per_file=args.max_rows_per_file,
    )
