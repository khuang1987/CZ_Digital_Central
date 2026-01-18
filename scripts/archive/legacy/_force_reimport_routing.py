"""强制重新导入 routing 数据（跳过文件变化检查）"""
import sys
import os
from pathlib import Path

# 添加项目根目录到 Python 路径
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import time
import logging
from datetime import datetime
import pandas as pd

from shared_infrastructure.utils.etl_utils import load_config
from shared_infrastructure.utils.db_dual_utils import DualDatabaseManager

# 配置
CONFIG_PATH = PROJECT_ROOT / "data_pipelines" / "sources" / "sap" / "config" / "config_sap_routing.yaml"
DB_PATH = PROJECT_ROOT / "data_pipelines" / "database" / "mddap_v2.db"
LOG_DIR = PROJECT_ROOT / "logs" / "sap"

# 设置日志
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / f"force_reimport_routing_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def get_db_manager():
    return DualDatabaseManager(DB_PATH)

def read_and_match_routing(cfg):
    """读取并匹配 routing 数据"""
    source_cfg = cfg.get("source", {})
    excel_base_dir = source_cfg.get("excel_base_dir", "")
    factory_files = source_cfg.get("factory_files", [])
    
    all_dataframes = []
    
    for factory_cfg in factory_files:
        file_name = factory_cfg.get("file", "")
        factory_code = factory_cfg.get("factory_code", "")
        routing_sheet = factory_cfg.get("routing_sheet", "")
        machining_sheet = factory_cfg.get("machining_sheet", "")
        
        file_path = os.path.join(excel_base_dir, file_name)
        
        if not os.path.exists(file_path):
            logging.warning(f"文件不存在: {file_path}")
            continue
        
        logging.info(f"读取 {factory_code}: {file_name}")
        
        # 读取 Routing 表
        df_routing = pd.read_excel(file_path, sheet_name=routing_sheet)
        df_routing["factory_code"] = factory_code
        df_routing["source_file"] = file_path
        logging.info(f"  Routing 表: {len(df_routing)} 行")
        
        # 读取机加工清单并匹配
        if machining_sheet:
            try:
                df_machining = pd.read_excel(file_path, sheet_name=machining_sheet)
                logging.info(f"  机加工清单: {len(df_machining)} 行")
                
                # 标准化列名
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
                    if old in df_routing.columns:
                        df_routing[new] = df_routing[old]
                
                # 匹配
                match_cols = ['CFN_match', 'Group_match', 'Operation_match']
                df_machining_subset = df_machining[match_cols + ['SetupTime_from_machining', 'OEE_from_machining']].drop_duplicates(subset=match_cols)
                
                df_routing = df_routing.merge(df_machining_subset, on=match_cols, how='left')
                
                # 赋值
                df_routing['SetupTime'] = df_routing['SetupTime_from_machining']
                df_routing['OEE'] = df_routing['OEE_from_machining']
                
                matched_count = df_routing['SetupTime'].notna().sum()
                logging.info(f"  匹配到 SetupTime/OEE: {matched_count} / {len(df_routing)} 行")
                
                # 清理临时列
                temp_cols = ['ProductNumber_match', 'CFN_match', 'Group_match', 'Operation_match', 
                            'SetupTime_from_machining', 'OEE_from_machining']
                df_routing = df_routing.drop(columns=[c for c in temp_cols if c in df_routing.columns], errors='ignore')
                
            except Exception as e:
                logging.warning(f"  机加工清单匹配失败: {e}")
        
        all_dataframes.append(df_routing)
    
    if all_dataframes:
        combined = pd.concat(all_dataframes, ignore_index=True)
        logging.info(f"合并完成: 共 {len(combined)} 行")
        return combined
    
    return pd.DataFrame()

def clean_routing_data(df):
    """清洗 routing 数据"""
    if df.empty:
        return df
    
    result = df.copy()
    
    # 替换 N/A
    result = result.replace(["N/A", "#N/A", "N/A ", "n/a", "#n/a"], None)
    result = result.replace("", None)
    
    # 列名映射
    column_mapping = {
        'Material Number': 'ProductNumber',
        'Operation/activity': 'Operation',
        'Operation description': 'OperationDesc',
        'Work Center': 'WorkCenter',
        'Machine': 'EH_machine',
        'Labor': 'EH_labor',
        'Base Quantity': 'Quantity',
        '工厂': 'Plant',
    }
    
    rename_dict = {k: v for k, v in column_mapping.items() if k in result.columns}
    if rename_dict:
        result = result.rename(columns=rename_dict)
    
    # 计算 StandardTime (分钟)：优先使用 EH_machine，如果为 0 或空则使用 EH_labor
    if 'EH_machine' in result.columns or 'EH_labor' in result.columns:
        eh_machine = result.get('EH_machine', 0).fillna(0)
        eh_labor = result.get('EH_labor', 0).fillna(0)
        
        # 优先使用 machine time，如果为 0 则使用 labor time
        result['StandardTime'] = eh_machine.where(eh_machine > 0, eh_labor) / 60.0
        result['StandardTime'] = result['StandardTime'].round(1)
        
        valid_st = (result['StandardTime'] > 0).sum()
        logging.info(f"计算 StandardTime: {valid_st} 条有效记录（> 0）")
    
    # OEE 处理：当 OEE 为 0 时，使用默认值 77%
    if 'OEE' in result.columns:
        result['OEE'] = pd.to_numeric(result['OEE'], errors='coerce')
        result['OEE'] = result['OEE'].where(result['OEE'] > 0, 0.77)
        logging.info(f"OEE 字段处理完成，0 值已替换为默认 77%")
    
    # SetupTime 保留1位小数
    if 'SetupTime' in result.columns:
        result['SetupTime'] = pd.to_numeric(result['SetupTime'], errors='coerce').round(1)
    
    # 生成 record_hash
    key_fields = ['ProductNumber', 'Operation', 'factory_code']
    available_keys = [k for k in key_fields if k in result.columns]
    
    if available_keys:
        hash_df = result[available_keys].fillna('').astype(str)
        record_hash = hash_df.iloc[:, 0]
        for col in hash_df.columns[1:]:
            record_hash = record_hash + '|' + hash_df[col]
        result['record_hash'] = record_hash
    
    # 选择目标列
    target_columns = [
        'ProductNumber', 'CFN', 'Plant', 'Operation',
        'WorkCenter', 'OperationDesc',
        'StandardTime', 'SetupTime', 'OEE',
        'EH_machine', 'EH_labor', 'Quantity', 'Group',
        'factory_code',
        'source_file', 'record_hash'
    ]
    
    existing_columns = [c for c in target_columns if c in result.columns]
    result = result[existing_columns]
    
    logging.info(f"清洗完成: {len(result)} 行, {len(existing_columns)} 列")
    
    return result

def main():
    logging.info("=" * 60)
    logging.info("强制重新导入 SAP Routing 数据")
    logging.info("=" * 60)
    
    t0 = time.time()
    
    try:
        cfg = load_config(str(CONFIG_PATH))
        if not cfg:
            logging.error("配置加载失败")
            return
        
        df = read_and_match_routing(cfg)
        
        if df.empty:
            logging.info("没有数据")
            return
        
        df_clean = clean_routing_data(df)
        
        # 去重
        original_count = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['record_hash'], keep='first')
        internal_dups = original_count - len(df_clean)
        if internal_dups > 0:
            logging.info(f"内部去重: 移除 {internal_dups} 条")
        
        # 保存到数据库
        db = get_db_manager()
        inserted = db.bulk_insert(df_clean, "raw_sap_routing", if_exists='append')
        
        logging.info(f"写入完成: 插入 {inserted} 条")
        
        count = db.get_table_count("raw_sap_routing")
        logging.info(f"数据库 raw_sap_routing 表当前记录数: {count}")
        
    except Exception as e:
        logging.exception(f"导入失败: {e}")
        raise
    
    logging.info(f"处理完成，耗时: {time.time() - t0:.2f} 秒")

if __name__ == "__main__":
    main()
