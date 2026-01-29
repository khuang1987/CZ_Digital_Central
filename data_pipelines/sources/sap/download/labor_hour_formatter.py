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

def format_labor_hour() -> bool:
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

        # 解压文件
        with zipfile.ZipFile(target_zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_folder)
            log_callback(f"[INFO] 解压完成")

        # 转换 XLS (MHTML) -> XLSX using Pandas + Quopri
        input_file = os.path.join(data_folder, labor_config['extracted_filename'])
        output_file = os.path.join(data_folder, labor_config['output_filename'])

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

            # Selector Strategy: Pick the table with the most cells (rows*cols)
            target_df = max(dfs, key=lambda df: df.size)
            
            log_callback(f"[INFO] 找到 {len(dfs)} 个表格，选择了最大的一张 (行数: {len(target_df)})")

            # --- Data Cleaning & Header Detection ---
            # SAP MHTML exports often have empty rows or metadata before the actual header.
            # Pandas read_html might assign default integer headers (0, 1, 2...).
            # We need to find the real header row (e.g. starting with "Plant" or "Work Center").
            
            header_keywords = ["Plant", "Work Center", "Cost Center"]
            header_found = False

            # Check if current columns allow us to identify directly (unlikely if they are 0,1,2, but check anyway)
            if any(k in str(c) for c in target_df.columns for k in header_keywords):
                header_found = True
            else:
                # Iterate through first 10 rows to find header
                for i in range(min(20, len(target_df))):
                    row_values = target_df.iloc[i].astype(str).values
                    # Check if any keyword matches a value in this row
                    if any(k in v for v in row_values for k in header_keywords):
                        log_callback(f"[INFO] 在第 {i} 行找到表头，正在重置...")
                        
                        # Set this row as header
                        target_df.columns = target_df.iloc[i]
                        
                        # Drop this row and all previous rows
                        target_df = target_df.iloc[i+1:].reset_index(drop=True)
                        header_found = True
                        break
            
            if not header_found:
                log_callback("[WARN] 未能自动识别表头 (未找到 'Plant'/'Work Center')，保留原始格式")

            # Save to standard XLSX
            target_df.to_excel(output_file, index=False)
            log_callback(f"[INFO] 成功转换为标准 Excel: {os.path.basename(output_file)}")

            # Cleanup input file
            try:
                os.remove(input_file)
            except: pass
            
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
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    format_labor_hour()
