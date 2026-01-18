"""
SAP Routing 标准时间原始数据 ETL 脚本 (V2 架构)
功能：从 Excel 读取 SAP Routing 数据，简单清洗后存入数据库原始表
"""

import os
import sys
import time
import logging
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

import pandas as pd

from shared_infrastructure.utils.etl_utils import (
    load_config,
)
from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager

# 配置
current_dir = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = Path(__file__).resolve().parents[4]

project_root_str = str(PROJECT_ROOT)
if project_root_str not in sys.path:
    sys.path.insert(0, project_root_str)

CONFIG_PATH = os.path.join(current_dir, "..", "config", "config_sap_routing.yaml")
LOG_DIR = os.path.join(PROJECT_ROOT, "shared_infrastructure", "logs", "sap")

# 设置日志
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"etl_sap_routing_raw_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8'),
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


def read_routing_files(cfg: Dict[str, Any]) -> pd.DataFrame:
    """读取 SAP Routing Excel 文件，并匹配机加工清单中的 OEE 和调试时间"""
    source_cfg = cfg.get("source", {})
    excel_base_dir = source_cfg.get("excel_base_dir", "")
    factory_files = source_cfg.get("factory_files", [])
    
    if not excel_base_dir or not factory_files:
        logging.error("未找到 SAP Routing 配置")
        return pd.DataFrame()
    
    db = get_db_manager()
    all_dataframes = []
    processed_files = []
    
    for factory_cfg in factory_files:
        file_name = factory_cfg.get("file", "")
        factory_code = factory_cfg.get("factory_code", "")
        routing_sheet = factory_cfg.get("routing_sheet", "")
        machining_sheet = factory_cfg.get("machining_sheet", "")
        
        file_path = os.path.join(excel_base_dir, file_name)
        file_path = os.path.abspath(file_path)
        file_path = os.path.normpath(file_path)
        
        if not os.path.exists(file_path):
            logging.warning(f"文件不存在: {file_path}")
            continue
        
        # 检查文件是否变化
        # filter_changed_files 内部只返回需要处理的文件，我们这里手动做一下详细日志以便调试
        changed_files = db.filter_changed_files(f"sap_routing_raw_{factory_code}", [file_path])
        if not changed_files:
            logging.info(f"{factory_code}: 文件未变化，跳过 (Path: {file_path})")
            continue
        else:
             # 如果文件被判定为变化，尝试查询并打印原因（仅调试用）
            try:
                with db.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT file_mtime, file_size FROM dbo.etl_file_state WHERE etl_name=? AND file_path=?", (f"sap_routing_raw_{factory_code}", file_path))
                    row = cur.fetchone()
                    if row:
                        curr_mtime = os.path.getmtime(file_path)
                        curr_size = os.path.getsize(file_path)
                        logging.info(f"{factory_code}: 文件已变化. DB(mtime={row[0]}, size={row[1]}) vs FS(mtime={curr_mtime}, size={curr_size})")
                    else:
                        logging.info(f"{factory_code}: 发现新文件 (未在数据库中记录)")
            except Exception:
                pass

        try:
            logging.info(f"读取 {factory_code}: {file_name}")
            
            # 读取 Routing 表
            df_routing = None
            if routing_sheet:
                df_routing = pd.read_excel(file_path, sheet_name=routing_sheet)
                df_routing["factory_code"] = factory_code
                df_routing["source_file"] = file_path # Normalized Absolute path
                logging.info(f"  Routing 表: {len(df_routing)} 行")
            
            # 读取机加工清单表并匹配 OEE 和调试时间
            if machining_sheet and df_routing is not None:
                try:
                    df_machining = pd.read_excel(file_path, sheet_name=machining_sheet)
                    logging.info(f"  机加工清单: {len(df_machining)} 行")
                    
                    # 标准化列名以便匹配
                    machining_rename = {
                        'Material Number': 'ProductNumber_match',
                        'CFN': 'CFN_match',
                        'Group': 'Group_match',
                        'Operation/activity': 'Operation_match',
                        '调试时间': 'SetupTime_from_machining',
                        'OEE': 'OEE_from_machining'
                    }
                    df_machining = df_machining.rename(columns={k: v for k, v in machining_rename.items() if k in df_machining.columns})
                    
                    # 准备匹配字段
                    routing_rename_temp = {
                        'Material Number': 'ProductNumber_match',
                        'CFN': 'CFN_match',
                        'Group': 'Group_match',
                        'Operation/activity': 'Operation_match'
                    }
                    for old, new in routing_rename_temp.items():
                        if old in df_routing.columns and new not in df_routing.columns:
                            df_routing[new] = df_routing[old]
                    
                    # 通过 CFN + Group + Operation 匹配
                    match_cols = ['CFN_match', 'Group_match', 'Operation_match']
                    if all(col in df_routing.columns for col in match_cols) and all(col in df_machining.columns for col in match_cols):
                        df_machining_subset = df_machining[match_cols + ['SetupTime_from_machining', 'OEE_from_machining']].drop_duplicates(subset=match_cols)
                        
                        df_routing = df_routing.merge(
                            df_machining_subset,
                            on=match_cols,
                            how='left'
                        )
                        
                        # 将匹配到的值赋给目标字段
                        if 'SetupTime_from_machining' in df_routing.columns:
                            df_routing['SetupTime'] = df_routing['SetupTime_from_machining']
                        if 'OEE_from_machining' in df_routing.columns:
                            df_routing['OEE'] = df_routing['OEE_from_machining']
                        
                        matched_count = df_routing['SetupTime'].notna().sum()
                        logging.info(f"  匹配到 SetupTime/OEE: {matched_count} / {len(df_routing)} 行")
                        
                        # 清理临时列
                        temp_cols = ['ProductNumber_match', 'CFN_match', 'Group_match', 'Operation_match', 
                                    'SetupTime_from_machining', 'OEE_from_machining']
                        df_routing = df_routing.drop(columns=[c for c in temp_cols if c in df_routing.columns], errors='ignore')
                    
                except Exception as e:
                    logging.warning(f"  机加工清单匹配失败: {e}")
            
            if df_routing is not None:
                all_dataframes.append(df_routing)
            
            processed_files.append((f"sap_routing_raw_{factory_code}", file_path))
            
        except Exception as e:
            logging.error(f"读取失败 {file_path}: {e}")
    
    if all_dataframes:
        combined = pd.concat(all_dataframes, ignore_index=True)
        logging.info(f"合并完成: 共 {len(combined)} 行")
        return combined
    
    return pd.DataFrame()


def clean_routing_data(df: pd.DataFrame) -> pd.DataFrame:
    """简单清洗 SAP Routing 数据"""
    if df.empty:
        return df
    
    result = df.copy()
    
    # 替换 N/A 值
    result = result.replace(["N/A", "#N/A", "N/A ", "n/a", "#n/a"], None)
    result = result.replace("", None)
    
    # 标准化列名（统一命名规范）
    column_mapping = {
        # 英文原始列名
        'Material Number': 'ProductNumber',  # 物料号（M开头代码）
        'Operation/activity': 'Operation',
        'Operation description': 'OperationDesc',
        'Work Center': 'WorkCenter',
        # Machine 和 Labor 映射为 EH_machine 和 EH_labor（单件工时）
        'Machine': 'EH_machine',  # 单件机器工时（秒）
        'Labor': 'EH_labor',  # 单件人工工时（秒）
        # 中文列名
        '物料': 'ProductNumber',
        '物料号': 'ProductNumber',
        'Material': 'ProductNumber',
        '工厂': 'Plant',
        '工序': 'Operation',
        '工序号': 'Operation',
        'OperationNumber': 'Operation',
        '工作中心': 'WorkCenter',
        '工序描述': 'OperationDesc',
        'Operation Description': 'OperationDesc',
        'OperationDescription': 'OperationDesc',
        '标准时间': 'StandardTime',
        'Standard Time': 'StandardTime',
        '调试时间': 'SetupTime',
        'Setup Time': 'SetupTime',
        # V1 兼容字段
        'Base Quantity': 'Quantity',
        'Quantity': 'Quantity',
        '数量': 'Quantity',
        '基本数量': 'Quantity',
        'Group': 'Group',
        '工艺组': 'Group',
    }
    
    rename_dict = {k: v for k, v in column_mapping.items() if k in result.columns}
    if rename_dict:
        result = result.rename(columns=rename_dict)

    if 'Operation' in result.columns:
        op_num = pd.to_numeric(result['Operation'], errors='coerce')
        op_num = op_num.where(op_num.isna() | (op_num == op_num.round()))
        result['Operation'] = op_num.round().astype('Int64')

    if 'Plant' in result.columns:
        plant_num = pd.to_numeric(result['Plant'], errors='coerce')
        plant_num = plant_num.where(plant_num.isna() | (plant_num == plant_num.round()))
        result['Plant'] = plant_num.round().astype('Int64')

    if 'Group' in result.columns:
        group_num = pd.to_numeric(result['Group'], errors='coerce')
        group_num = group_num.where(group_num.isna() | (group_num == group_num.round()))
        result['Group'] = group_num.round().astype('Int64')
    
    # 计算 StandardTime (分钟)：优先使用 EH_machine，如果为 0 或空则使用 EH_labor
    # 公式: StandardTime = (选择的工时秒数) / 60，保留1位小数
    if 'EH_machine' in result.columns or 'EH_labor' in result.columns:
        eh_machine = result.get('EH_machine', 0).fillna(0)
        eh_labor = result.get('EH_labor', 0).fillna(0)
        
        # 优先使用 machine time，如果为 0 则使用 labor time
        result['StandardTime'] = eh_machine.where(eh_machine > 0, eh_labor) / 60.0
        result['StandardTime'] = result['StandardTime'].round(1)
        
        valid_count = (result['StandardTime'] > 0).sum()
        logging.info(f"计算 StandardTime: {valid_count} 条有效记录（> 0）")
    
    # OEE 处理：当 OEE 为 0 时，使用默认值 77%
    if 'OEE' in result.columns:
        result['OEE'] = pd.to_numeric(result['OEE'], errors='coerce')
        result['OEE'] = result['OEE'].where(result['OEE'] > 0, 0.77)
        logging.info(f"OEE 字段处理完成，0 值已替换为默认 77%")
    
    # SetupTime 保留1位小数
    if 'SetupTime' in result.columns:
        result['SetupTime'] = pd.to_numeric(result['SetupTime'], errors='coerce').round(1)
    
    # 生成 record_hash
    # V2 Update: Include Group and CFN to support multiple routing versions
    key_fields = ['ProductNumber', 'CFN', 'Group', 'Operation', 'factory_code']
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
        'ProductNumber', 'CFN', 'Plant', 'Operation',
        'WorkCenter', 'OperationDesc',
        'StandardTime', 'SetupTime', 'OEE',
        'EH_machine', 'EH_labor', 'Quantity', 'Group',  # 单件工时字段
        'factory_code',
        'source_file', 'record_hash'
    ]
    
    existing_columns = [c for c in target_columns if c in result.columns]
    result = result[existing_columns]
    
    logging.info(f"清洗完成: {len(result)} 行, {len(existing_columns)} 列")
    
    return result


def save_to_database(df: pd.DataFrame, table_name: str = "raw_sap_routing") -> Dict[str, int]:
    """保存数据到数据库"""
    if df.empty:
        return {"inserted": 0, "skipped": 0}
    
    db = get_db_manager()
    
    # 1. 先对 DataFrame 内部去重
    original_count = len(df)
    df = df.drop_duplicates(subset=['record_hash'], keep='first')
    internal_dups = original_count - len(df)
    if internal_dups > 0:
        logging.info(f"内部去重: 移除 {internal_dups} 条重复记录")
    
    # 2. 再与数据库已有记录去重
    existing_hashes = db.get_existing_hashes(
        table_name,
        "record_hash",
        hashes=df["record_hash"].dropna().astype(str).tolist() if "record_hash" in df.columns else None,
    )
    
    if existing_hashes:
        new_df = df[~df['record_hash'].isin(existing_hashes)]
        db_dups = len(df) - len(new_df)
        logging.info(f"数据库去重: 跳过 {db_dups} 条已存在记录")
    else:
        new_df = df
        db_dups = 0
    
    skipped = internal_dups + db_dups
    
    if new_df.empty:
        for etl_name, file_path in df[["factory_code", "source_file"]].dropna().drop_duplicates().itertuples(index=False, name=None):
            db.mark_file_processed(f"sap_routing_raw_{etl_name}", file_path)
        return {"inserted": 0, "skipped": skipped}
    
    inserted = db.bulk_insert(new_df, table_name, if_exists='append')

    for etl_name, file_path in df[["factory_code", "source_file"]].dropna().drop_duplicates().itertuples(index=False, name=None):
        db.mark_file_processed(f"sap_routing_raw_{etl_name}", file_path)
    
    return {"inserted": inserted, "skipped": skipped}


def main():
    logging.info("=" * 60)
    logging.info("SAP Routing 原始数据 ETL (V2) 启动")
    logging.info("=" * 60)
    
    t0 = time.time()
    
    try:
        cfg = load_config(CONFIG_PATH)
        if not cfg:
            logging.error("配置加载失败")
            return
        
        df = read_routing_files(cfg)
        
        if df.empty:
            logging.info("没有新数据需要处理")
            return
        
        df_clean = clean_routing_data(df)
        stats = save_to_database(df_clean, "raw_sap_routing")
        
        logging.info(f"写入完成: 插入 {stats['inserted']} 条, 跳过 {stats['skipped']} 条")
        
        db = get_db_manager()
        db.log_etl_run(
            etl_name='sap_routing_raw',
            status='success',
            records_read=len(df),
            records_inserted=stats['inserted'],
            records_skipped=stats['skipped']
        )
        
        count = db.get_table_count("raw_sap_routing")
        logging.info(f"数据库 raw_sap_routing 表当前记录数: {count}")
        
    except Exception as e:
        logging.exception(f"ETL 失败: {e}")
        raise
    
    logging.info(f"处理完成，耗时: {time.time() - t0:.2f} 秒")


if __name__ == "__main__":
    main()
