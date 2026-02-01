"""
工时数据格式化工具

功能：处理 SAP 导出的 ZIP 压缩包 (内含 .xls)，解压并转换为 .xlsx 格式。
"""

import os
import zipfile
import shutil
import sys
import logging
import time
import re
import yaml
import quopri
from io import StringIO
from datetime import datetime
from pathlib import Path
import pandas as pd

import argparse
# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[4]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

# ============================================================
# 配置
# ============================================================

def get_base_path() -> Path:
    return PROJECT_ROOT

def get_etl_config_path() -> Path:
    return PROJECT_ROOT / "data_pipelines" / "sources" / "sap" / "config" / "config_sap_labor.yaml"

from shared_infrastructure.env_utils import load_yaml_with_env

def get_labor_hour_base_dir() -> Path:
    """Read base directory from ETL YAML config"""
    try:
        config_path = get_etl_config_path()
        if config_path.exists():
            config = load_yaml_with_env(config_path)
            if config and 'source' in config and 'labor_hour_path' in config['source']:
                custom_path = Path(config['source']['labor_hour_path'])
                # Ensure directory exists (or warn if it's a network path that might be offline)
                if custom_path.exists():
                    return custom_path
                else:
                    logger.warning(f"Configured path does not exist: {custom_path}")
    except Exception as e:
        logger.warning(f"Failed to read ETL config: {e}")

    # Fallback default
    path = get_base_path() / "data" / "raw" / "sap"
    path.mkdir(parents=True, exist_ok=True)
    return path

def get_labor_hour_config():
    """获取工时相关的配置"""
    try:
        config_path = get_etl_config_path()
        if config_path.exists():
            yaml_conf = load_yaml_with_env(config_path)
            if yaml_conf and 'source' in yaml_conf and 'file_patterns' in yaml_conf['source']:
                patterns = yaml_conf['source']['file_patterns']
                return {
                    'zip_filename': patterns.get('zip_source', 'YPP_M03_Q5003.ZIP'),
                    'extracted_filename': patterns.get('excel_intermediate', 'YPP_M03_Q5003_00000.xls'),
                    'output_filename': patterns.get('excel_output', 'YPP_M03_Q5003_00000.xlsx')
                }
    except Exception:
        pass

    return {
        'zip_filename': 'YPP_M03_Q5003.ZIP',
        'extracted_filename': 'YPP_M03_Q5003_00000.xls',
        'output_filename': 'YPP_M03_Q5003_00000.xlsx'
    }

# ============================================================
# 工具函数
# ============================================================

def format_labor_hour(force_refresh: bool = False) -> bool:
    """格式化工时数据"""
    
    def log_callback(message):
        logger.info(message)
        print(message)

    try:
        data_folder = str(get_labor_hour_base_dir())
        labor_config = get_labor_hour_config()
        attachment_name = labor_config['zip_filename']
        target_zip_path = os.path.join(data_folder, attachment_name)
        
        # 检查ZIP文件是否存在
        if not os.path.exists(target_zip_path):
            log_callback(f"[ERROR] 未找到ZIP文件: {target_zip_path}")
            return False

        # --- 优化：检测 ZIP 是否有变化 ---
        from shared_infrastructure.utils.db_sqlserver_only import SQLServerOnlyManager
        db_state = SQLServerOnlyManager()
        
        if not force_refresh:
            changed_list = db_state.filter_changed_files("sap_labor_zip", [target_zip_path])
            if not changed_list:
                log_callback(f"[INFO] SAP ZIP 文件未发生变化，跳过后续解析与导入。")
                return True
        else:
            log_callback(f"[INFO] 强制刷新模式已开启")

        # 检测ZIP文件日期
        try:
            zip_mtime = os.path.getmtime(target_zip_path)
            zip_date_str = datetime.fromtimestamp(zip_mtime).strftime("%Y-%m-%d")
            current_date_str = datetime.now().strftime("%Y-%m-%d")
            
            if zip_date_str != current_date_str:
                log_callback(f"[WARN] 日期不匹配！ZIP文件日期({zip_date_str})与当前日期({current_date_str})不一致")
        except Exception as e:
            log_callback(f"[ERROR] 检测文件日期时出错: {e}")

        # 清理旧文件
        try:
            for file in os.listdir(data_folder):
                if file.startswith('YPP') and file.endswith('.xls'):
                    file_path = os.path.join(data_folder, file)
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        print(f"Cleanup error: {e}")
        except Exception:
            pass

        # 获取 ZIP 文件日期用于命名
        zip_mtime = os.path.getmtime(target_zip_path)
        zip_date_raw = datetime.fromtimestamp(zip_mtime)
        zip_date_suffix = zip_date_raw.strftime("%Y%m%d")
        
        # 新的输出文件名
        output_filename = f"SAP_laborhour_{zip_date_suffix}.xlsx"
        output_file = os.path.join(data_folder, output_filename)

        # 解压文件
        with zipfile.ZipFile(target_zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_folder)
            log_callback(f"[INFO] 解压完成")

        # 转换 XLS (MHTML) -> XLSX using Pandas + Quopri
        input_file = os.path.join(data_folder, labor_config['extracted_filename'])

        if not os.path.exists(input_file):
            log_callback(f"[ERROR] 解压后的文件不存在: {input_file}")
            return False

        log_callback("[INFO] 开始解析 SAP MHTML(.xls) 文件...")
        
        try:
            with open(input_file, 'rb') as f:
                content_bytes = f.read()

            # Decode quoted-printable content
            try:
                decoded_bytes = quopri.decodestring(content_bytes)
                decoded_str = decoded_bytes.decode('utf-8', errors='replace')
            except Exception as e:
                log_callback(f"[WARN] Quopri decoding error: {e}, attempting raw read")
                decoded_str = content_bytes.decode('utf-8', errors='replace')

            # Parse with Pandas
            dfs = pd.read_html(StringIO(decoded_str))

            if not dfs:
                log_callback("[ERROR] 未能在文件中找到数据表")
                return False

            # Selector Strategy: 找到包含数据的那张表（通常是最大的一张）
            target_df = max(dfs, key=lambda df: df.size)
            log_callback(f"[INFO] 找到 {len(dfs)} 个表格，选择了最大的一张 (尺寸: {target_df.size})")

            # --- 用户需求 1: 从第 7 行开始 (索引 6), 并统一命名 26 列 ---
            # 直接跳过前 7 行，并应用完整的业务表头
            SAP_YPP_M03_HEADERS = [
                'Plant', 'WorkCenter', 'WorkCenterDesc', 'CostCenter', 'CostCenterDesc',
                'Material', 'MaterialDesc', 'MaterialType', 'MRPController', 'MRPControllerDesc',
                'ProductionScheduler', 'ProductionSchedulerDesc', 'OrderNumber', 'OrderType',
                'OrderTypeDesc', 'Operation', 'OperationDesc', 'PostingDate', 'ActualStartTime', 
                'ActualFinishTime', 'ActualFinishDate', 'EarnedLaborUnit', 'MachineTime', 
                'EarnedLaborTime', 'ActualQuantity', 'ActualScrapQty', 'TargetQuantity'
            ]

            if len(target_df) > 7:
                # 丢弃前 7 行 (metadata + 原表头)
                target_df = target_df.iloc[7:].reset_index(drop=True)
                
                # 适配列数：如果列数刚好是 26 或更多，则应用表头
                # FIX: Relaxed check to >= 26 (Allow missing TargetQuantity)
                if target_df.shape[1] >= 26:
                    # 动态适配表头长度
                    current_cols_count = target_df.shape[1]
                    expected_count = len(SAP_YPP_M03_HEADERS) # 27
                    
                    if current_cols_count >= expected_count:
                        # 只有当列数足够时才截断
                        target_df = target_df.iloc[:, :expected_count]
                        target_df.columns = SAP_YPP_M03_HEADERS
                    else:
                        # 列数不够 (e.g. 26)，只应用前 N 个表头
                        log_callback(f"[INFO] 列数 ({current_cols_count}) 少于标准 ({expected_count})，应用部分表头")
                        target_df.columns = SAP_YPP_M03_HEADERS[:current_cols_count]

                    log_callback(f"[INFO] 已统一定义业务表头")
                else:
                    log_callback(f"[WARN] 数据列数 ({target_df.shape[1]}) 少于预期 (26)，尝试强制赋值...")
                    target_df.columns = [SAP_YPP_M03_HEADERS[i] if i < len(SAP_YPP_M03_HEADERS) else f"Col_{i}" 
                                       for i in range(target_df.shape[1])]

            # Save to standard XLSX
            target_df.to_excel(output_file, index=False)
            log_callback(f"[INFO] 成功导出文件: {output_filename}")

            # --- 用户需求 2: 直接导入 SQL ---
            try:
                from data_pipelines.sources.sap.etl.etl_sap_labor_hours import (
                    get_db_manager, clean_data, import_data, create_table
                )
                
                log_callback("[INFO] 正在直接导入 SQL Server...")
                db = get_db_manager()
                create_table(db)
                
                # 数据此时已经是标准列名 (e.g. OrderNumber, MachineTime)，无需再次 RENAME
                # 但为了兼容 clean_data 里的可能映射，可以保留一个简单的 strip
                target_df.columns = target_df.columns.astype(str).str.strip()
                
                # 调用 ETL 脚本里的清洗和导入逻辑
                cleaned_df = clean_data(target_df)
                
                # 执行导入
                imported = import_data(db, cleaned_df)
                
                log_callback(f"[INFO] SQL 导入完成: 新增 {imported} 条记录")
                
                # 记录文件处理状态
                db.mark_file_processed("sap_labor_hours", output_file)
                
                # 同时记录原始 ZIP 的处理状态，用于下次判断是否跳过
                db.mark_file_processed("sap_labor_zip", target_zip_path)
                
            except Exception as e:
                log_callback(f"[WARN] 自动导入 SQL 失败 (您可以后续手动运行 ETL 脚本): {e}")

            # Cleanup input file
            try:
                os.remove(input_file)
            except: pass
            
            # --- Cleanup Old SAP Export Files (Keep Latest Only) ---
            try:
                # Find all SAP_laborhour_*.xlsx files
                sap_files = []
                for f in os.listdir(data_folder):
                    if f.startswith('SAP_laborhour_') and f.endswith('.xlsx'):
                        full_path = os.path.join(data_folder, f)
                        sap_files.append((full_path, os.path.getmtime(full_path)))
                
                # Sort by mtime descending (Newest first)
                sap_files.sort(key=lambda x: x[1], reverse=True)
                
                # Keep index 0, delete the rest
                if len(sap_files) > 1:
                    for i in range(1, len(sap_files)):
                        file_to_del = sap_files[i][0]
                        try:
                            os.remove(file_to_del)
                            log_callback(f"[INFO] 自动清理旧备份: {os.path.basename(file_to_del)}")
                        except Exception as e:
                            log_callback(f"[WARN] 清理文件失败: {e}")
            except Exception as e:
                log_callback(f"[WARN] 清理过程异常: {e}")

            return True

        except Exception as e:
            log_callback(f"[ERROR] 转换失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    except Exception as e:
        log_callback(f"[CRITICAL] 异常: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SAP Labor Hour Formatter")
    parser.add_argument("--force", action="store_true", help="Force refresh even if ZIP is unchanged")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    format_labor_hour(force_refresh=args.force)
