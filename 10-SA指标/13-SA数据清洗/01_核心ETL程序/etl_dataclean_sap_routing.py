"""
SAP Routing标准时间表转换脚本
将CSV格式的标准时间表转换为Parquet格式
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any

# 导入ETL通用工具函数
from etl_utils import (
    setup_logging,
    load_config,
    save_to_parquet,
    get_base_dir,
    ensure_directory_exists
)

# 配置基础路径
BASE_DIR = get_base_dir()
CONFIG_PATH = os.path.join(BASE_DIR, "..", "03_配置文件", "config", "config_sap_routing.yaml")

# 如果配置文件不存在，使用默认配置
DEFAULT_CONFIG = {
    "logging": {
        "level": "INFO",
        "file": os.path.join(BASE_DIR, "..", "06_日志文件", "etl_sap_routing.log")
    },
    "output": {
        "base_dir": r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish",
        "parquet": {
            "compression": "snappy"
        },
        "excel": {
            "enabled": True,  # 是否启用Excel输出
            "max_rows": 10000,  # Excel文件最大行数（避免文件过大）
            "include_stats": True  # 是否包含统计信息工作表
        }
    },
    "source": {
        "csv_base_dir": os.path.join(BASE_DIR, "..", "..", "11数据模板"),
        "routing_csv": "1303 Routing及机加工产品清单_1303 Routing.csv",
        "machining_csv": "1303 Routing及机加工产品清单_1303机加工清单.csv"
    }
}


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """清洗DataFrame：处理N/A、空值等"""
    # 替换N/A和#N/A为None
    df = df.replace(["N/A", "#N/A", "N/A ", "N/A\t", "n/a", "#n/a"], None)
    df = df.replace("", None)
    
    # 去除字符串列的前后空格
    for col in df.columns:
        if df[col].dtype == 'object':
            # 先转换为字符串，处理空值
            df[col] = df[col].astype(str)
            # 替换特殊值
            df[col] = df[col].replace(['nan', 'None', 'NaN', 'NULL', 'null'], None)
            # 去除前后空格
            df[col] = df[col].str.strip()
            # 空字符串转为None
            df.loc[df[col] == '', col] = None
    
    return df


def convert_and_merge_standard_time(excel_base_dir: str, factory_files: list, routing_sheet: str, machining_sheet: str, output_path: str, cfg: Dict[str, Any] = None) -> None:
    """
    转换并合并两个工厂的标准时间表
    1. 读取两个工厂的Excel文件中的Routing表和机加工清单表
    2. 合并所有工厂的数据并添加工厂标识
    3. 处理Routing表的单位转换和单位时间计算
    4. 合并机加工清单表的OEE、调试时间、备注字段
    5. 保存为合并后的Parquet文件
    """
    logging.info("=" * 80)
    logging.info("开始转换和合并标准时间表")
    logging.info("=" * 80)
    
    try:
        # 初始化合并后的数据框
        all_routing_dfs = []
        all_machining_dfs = []
        
        # 1. 读取每个工厂的Excel文件
        for factory_config in factory_files:
            factory_file = factory_config["file"]
            factory_code = factory_config["factory_code"]
            routing_sheet = factory_config["routing_sheet"]
            machining_sheet = factory_config["machining_sheet"]
            
            excel_path = os.path.join(excel_base_dir, factory_file)
            
            logging.info(f"读取工厂 {factory_code} 的Excel文件: {excel_path}")
            
            # 检查文件是否存在
            if not os.path.exists(excel_path):
                logging.error(f"Excel文件不存在: {excel_path}")
                continue
                
            # 读取Routing表
            try:
                routing_df = pd.read_excel(excel_path, sheet_name=routing_sheet, engine='openpyxl')
                routing_df['Factory'] = factory_code  # 添加工厂标识
                all_routing_dfs.append(routing_df)
                logging.info(f"工厂 {factory_code} Routing表读取成功，共 {len(routing_df)} 行数据")
            except Exception as e:
                logging.error(f"读取工厂 {factory_code} Routing表失败: {e}")
                continue
                
            # 读取机加工清单表
            try:
                machining_df = pd.read_excel(excel_path, sheet_name=machining_sheet, engine='openpyxl')
                machining_df['Factory'] = factory_code  # 添加工厂标识
                all_machining_dfs.append(machining_df)
                logging.info(f"工厂 {factory_code} 机加工清单表读取成功，共 {len(machining_df)} 行数据")
            except Exception as e:
                logging.error(f"读取工厂 {factory_code} 机加工清单表失败: {e}")
                continue
        
        if not all_routing_dfs:
            raise ValueError("没有成功读取任何Routing数据")
            
        # 合并所有工厂的数据
        routing_df = pd.concat(all_routing_dfs, ignore_index=True)
        machining_df = pd.concat(all_machining_dfs, ignore_index=True) if all_machining_dfs else pd.DataFrame()
        
        logging.info(f"所有工厂Routing数据合并完成，共 {len(routing_df)} 行数据")
        if not machining_df.empty:
            logging.info(f"所有工厂机加工清单数据合并完成，共 {len(machining_df)} 行数据")
        
        # 清洗数据
        routing_df = clean_dataframe(routing_df)
        if not machining_df.empty:
            machining_df = clean_dataframe(machining_df)
        
        # 2. 标准化字段名称（去除空格）
        routing_df.columns = routing_df.columns.str.strip()
        routing_df = routing_df.rename(columns={"Operation/activity": "Operation", "Base Quantity": "Quantity"})
        
        # 3. 处理Operation字段：转换为字符串并补零为4位
        def clean_operation(op):
            if pd.isna(op) or op is None:
                return None
            try:
                op_num = float(op)
                return f"{int(op_num):04d}"
            except (ValueError, TypeError):
                return str(op).strip().zfill(4) if str(op).strip() else None
        
        routing_df["Operation"] = routing_df["Operation"].apply(clean_operation)
        
        # 4. 处理Quantity字段：转换为数值，空值或无效值设为1
        routing_df["Quantity"] = pd.to_numeric(routing_df["Quantity"], errors='coerce')
        routing_df["Quantity"] = routing_df["Quantity"].fillna(1).replace(0, 1)
        
        # 5. 处理Machine和Labor：转换为数值，保持原值（不转换为小时）
        routing_df["Machine"] = pd.to_numeric(routing_df["Machine"], errors='coerce')
        routing_df["Labor"] = pd.to_numeric(routing_df["Labor"], errors='coerce')
        
        logging.info(f"Routing表处理完成，共 {len(routing_df)} 行数据")
        
        # 6. 读取机加工清单表（如果存在）
        if not machining_df.empty:
            # 8. 标准化字段名称（去除空格）
            machining_df.columns = machining_df.columns.str.strip()
            machining_df = machining_df.rename(columns={"Operation/activity": "Operation"})
        
        # 9. 处理Operation字段：转换为字符串并补零为4位（仅在机加工数据存在时处理）
        if not machining_df.empty:
            def clean_operation(op):
                if pd.isna(op) or op is None:
                    return None
                try:
                    op_num = float(op)
                    return f"{int(op_num):04d}"
                except (ValueError, TypeError):
                    return str(op).strip().zfill(4) if str(op).strip() else None
            
            machining_df["Operation"] = machining_df["Operation"].apply(clean_operation)
        
        # 10. 处理OEE字段：转换为数值，保持空值（不设置默认值）
        if not machining_df.empty:
            machining_df["OEE"] = pd.to_numeric(machining_df["OEE"], errors='coerce')
        
        # 11. 处理调试时间字段：转换为数值（保持原值，单位：小时）
        if not machining_df.empty:
            machining_df["调试时间"] = pd.to_numeric(machining_df["调试时间"], errors='coerce')
        
        # 12. 合并两个表：按CFN、Operation、Group匹配（三个字段组合是唯一的）
        if not machining_df.empty:
            # 确保Group字段存在（如果不存在，创建空值）
            if "Group" not in routing_df.columns:
                routing_df["Group"] = ""
            if "Group" not in machining_df.columns:
                machining_df["Group"] = ""
            
            # 统一Group字段为字符串类型
            routing_df["Group"] = routing_df["Group"].astype(str).replace('nan', '').replace('None', '')
            machining_df["Group"] = machining_df["Group"].astype(str).replace('nan', '').replace('None', '')
            
            # 选择机加工清单表中需要合并的字段
            machining_value_cols = ["OEE", "调试时间"]
            if "备注" in machining_df.columns:
                machining_value_cols.append("备注")
            if "分类" in machining_df.columns:
                machining_value_cols.append("分类")
            
            # 合并（使用CFN、Operation、Group作为匹配键）
            merged_df = routing_df.merge(
                machining_df[["CFN", "Operation", "Group"] + machining_value_cols],
                on=["CFN", "Operation", "Group"],
                how="left"
            )
            
            # 重命名调试时间为Setup Time (h)
            merged_df = merged_df.rename(columns={"调试时间": "Setup Time (h)"})
            
            logging.info(f"合并完成，合并后共 {len(merged_df)} 行数据")
            if 'OEE' in merged_df.columns:
                logging.info(f"匹配到OEE: {merged_df['OEE'].notna().sum()} 行")
            if 'Setup Time (h)' in merged_df.columns:
                logging.info(f"匹配到调试时间: {merged_df['Setup Time (h)'].notna().sum()} 行")
        else:
            # 如果没有机加工清单数据，直接使用Routing数据
            merged_df = routing_df.copy()
            logging.info("没有机加工清单数据，仅使用Routing数据")
        
        # 8. 使用共用的保存函数保存为Parquet
        save_to_parquet(merged_df, output_path, cfg)
        logging.info(f"已保存合并后的标准时间表Parquet文件: {output_path}, 行数: {len(merged_df)}")
        
    except Exception as e:
        logging.error(f"转换和合并标准时间表失败: {e}")
        raise


if __name__ == "__main__":
    import argparse
    
    # 添加命令行参数支持
    parser = argparse.ArgumentParser(description='SA指标SAP路由数据清洗ETL脚本')
    parser.add_argument('--mode', choices=['incremental', 'full'], 
                       default='incremental', help='刷新模式: incremental(增量) 或 full(全量)')
    parser.add_argument('--unattended', action='store_true', 
                       help='无人值守模式，不进行交互式提示')
    
    args = parser.parse_args()
    
    # 加载配置
    try:
        if os.path.exists(CONFIG_PATH):
            cfg = load_config(CONFIG_PATH)
        else:
            cfg = DEFAULT_CONFIG
            print(f"配置文件不存在，使用默认配置: {CONFIG_PATH}")
    except Exception as e:
        print(f"加载配置失败，使用默认配置: {e}")
        cfg = DEFAULT_CONFIG
    
    # 设置日志
    setup_logging(cfg)
    
    # 记录运行模式
    if args.unattended:
        logging.info("="*60)
        logging.info("无人值守模式启动")
        logging.info(f"刷新模式: {'增量刷新' if args.mode == 'incremental' else '全量刷新'}")
        logging.info("="*60)
    
    # 设置日志
    setup_logging(cfg)
    
    # 确定Excel文件路径
    excel_base_dir = cfg.get("source", {}).get("excel_base_dir", r"C:\Users\huangk14\OneDrive - Medtronic PLC\General - CZ OPS生产每日产出登记文件夹")
    excel_base_dir = os.path.abspath(excel_base_dir)
    
    factory_files = cfg.get("source", {}).get("factory_files", [
        "133 Routing及机加工产品清单.xlsx",
        "9997 Routing及机加工产品清单.xlsx"
    ])
    routing_sheet = cfg.get("source", {}).get("routing_sheet", "1303 Routing")
    machining_sheet = cfg.get("source", {}).get("machining_sheet", "1303机加工清单")
    
    # 输出文件路径（统一使用latest版本）
    output_base_dir = cfg.get("output", {}).get("base_dir", r"C:\Users\huangk14\OneDrive - Medtronic PLC\CZ Production - 文档\General\POWER BI 数据源 V2\30-MES导出数据\publish")
    ensure_directory_exists(output_base_dir)
    
    merged_parquet = os.path.join(output_base_dir, "SAP_Routing_latest.parquet")
    
    logging.info(f"输出文件名: SAP_Routing_latest.parquet")
    
    try:
        # 检查目录是否存在
        if not os.path.exists(excel_base_dir):
            logging.error(f"Excel文件目录不存在: {excel_base_dir}")
            sys.exit(1)
        
        # 转换并合并两个表
        convert_and_merge_standard_time(excel_base_dir, factory_files, routing_sheet, machining_sheet, merged_parquet, cfg)
        
        logging.info("=" * 80)
        logging.info("转换和合并完成！")
        logging.info("=" * 80)
        
    except Exception as e:
        logging.error(f"程序执行失败: {e}")
        sys.exit(1)

