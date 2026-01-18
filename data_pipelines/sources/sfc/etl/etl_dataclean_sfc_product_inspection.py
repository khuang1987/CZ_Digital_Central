"""
SFC产品检验记录数据清洗ETL脚本
功能：从SharePoint读取产品检验记录Excel数据，进行数据处理、去重、增量处理，输出为Parquet格式供Power BI使用
"""

import os
import sys
import time
import logging
import glob
import json
import hashlib
import threading
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Set
import re
import warnings
warnings.filterwarnings('ignore')
from pathlib import Path

# Windows平台支持
try:
    import msvcrt
    HAS_MSVCRT = True
except ImportError:
    HAS_MSVCRT = False

import pandas as pd
import numpy as np
import yaml

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("警告：未安装pyarrow，将无法保存Parquet格式")
    pq = None


WRITE_PARQUET_OUTPUT = False

# 项目根目录
project_root = str(Path(__file__).resolve().parents[4])

# 导入共享基础设施工具函数
from shared_infrastructure.utils.etl_utils import (
    setup_logging,
    setup_logging_with_rotation,
    load_config,
    save_to_parquet,
    save_to_dual_locations,
    read_sharepoint_excel,
    IncrementalProcessor
)
from shared_infrastructure.utils.path_resolver import (
    get_config_path,
    get_log_path,
    get_state_path,
    get_path_resolver
)
from shared_infrastructure.utils.db_utils import get_default_db_manager

# 获取路径解析器
resolver = get_path_resolver()

# 配置路径
CONFIG_PATH = get_config_path("sfc_product_inspection", "sfc", os.path.dirname(os.path.abspath(__file__)))
LOG_PATH = get_log_path("sfc")
STATE_PATH = get_state_path("sfc_product_inspection", "sfc", os.path.dirname(os.path.abspath(__file__)))


def extract_report_date_from_filename(filename: str) -> datetime:
    """
    从文件名提取报表日期
    IGPR-20251202080001 -> 2025-12-02 08:00:01
    """
    try:
        # 使用正则表达式匹配 IGPR-YYYYMMDDHHMMSS 格式
        pattern = r'IGPR-(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})'
        match = re.search(pattern, filename)
        
        if match:
            year, month, day, hour, minute, second = match.groups()
            return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
        else:
            # 如果无法解析，返回文件修改时间作为备选
            logging.warning(f"无法从文件名提取日期: {filename}，使用当前时间")
            return datetime.now()
            
    except Exception as e:
        logging.warning(f"解析文件名日期失败 {filename}: {e}")
        return datetime.now()


def generate_record_hash(row: pd.Series, key_fields: List[str]) -> str:
    """
    生成记录的唯一hash值（基于业务字段，不含文件名）
    """
    try:
        # 构建关键字的字符串表示
        key_values = []
        for field in key_fields:
            if field in row and pd.notna(row[field]):
                key_values.append(str(row[field]).strip())
            else:
                key_values.append("")  # 空值处理
        
        # 生成hash（不含文件名）
        key_str = "|".join(key_values)
        return hashlib.md5(key_str.encode('utf-8')).hexdigest()
    except Exception as e:
        logging.warning(f"生成记录hash失败: {e}")
        return hashlib.md5(str(row.to_dict()).encode('utf-8')).hexdigest()


def load_etl_state(state_file: str) -> Dict[str, Any]:
    """
    加载ETL状态文件
    """
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            
            # 确保processed_hashes是set类型
            if "processed_hashes" in state and isinstance(state["processed_hashes"], list):
                state["processed_hashes"] = set(state["processed_hashes"])
            
            logging.info(f"已加载ETL状态: {len(state.get('processed_hashes', []))} 条已处理记录")
            return state
        except Exception as e:
            logging.warning(f"加载状态文件失败: {e}，使用空状态")
    
    # 返回默认状态
    return {
        "processed_hashes": set(),
        "processed_files": {},  # 格式: {文件路径: {mtime: 修改时间, hash: 文件hash}}
        "last_update": datetime.now().isoformat(),
        "total_records": 0
    }


def save_etl_state(state: Dict[str, Any], state_file: str) -> None:
    """
    保存ETL状态文件
    """
    try:
        # 创建状态目录
        os.makedirs(os.path.dirname(state_file), exist_ok=True)
        
        # 复制状态以避免修改原始数据
        state_copy = state.copy()
        
        # 转换set为list以便JSON序列化
        if "processed_hashes" in state_copy and isinstance(state_copy["processed_hashes"], set):
            state_copy["processed_hashes"] = list(state_copy["processed_hashes"])
        
        # 更新时间戳
        state_copy["last_update"] = datetime.now().isoformat()
        
        # 保存状态
        with open(state_file, 'w', encoding='utf-8') as f:
            json.dump(state_copy, f, ensure_ascii=False, indent=2)
        
        logging.info(f"ETL状态已保存: {state_file}")
    except Exception as e:
        logging.error(f"保存状态文件失败: {e}")


def filter_incremental_data(df: pd.DataFrame, file_path: str, cfg: Dict[str, Any], state_file: str) -> pd.DataFrame:
    """
    增量过滤：只返回新数据（未处理过的记录）
    基于业务字段进行hash匹配（不含文件名）
    """
    incr_cfg = cfg.get("incremental", {})
    
    if not incr_cfg.get("enabled", False):
        logging.info("增量处理未启用，返回全部数据")
        return df
    
    # 加载处理状态
    state = load_etl_state(state_file)
    processed_hashes = state.get("processed_hashes", set())
    
    # 检查关键字段
    key_fields = incr_cfg.get("key_fields", [])
    missing_fields = [field for field in key_fields if field not in df.columns]
    
    if missing_fields:
        logging.warning(f"增量处理所需字段不存在: {missing_fields}，返回全部数据")
        return df
    
    # 生成每条记录的hash并筛选新数据（不含文件名）
    df['_record_hash'] = df.apply(lambda row: generate_record_hash(row, key_fields), axis=1)
    
    # 筛选新数据（hash不在已处理集合中）
    new_data = df[~df['_record_hash'].isin(processed_hashes)].copy()
    new_data = new_data.drop(columns=['_record_hash'])
    
    logging.info(f"增量过滤: {len(df)} → {len(new_data)} 条新记录")
    
    return new_data


def update_etl_state(df: pd.DataFrame, file_path: str, cfg: Dict[str, Any], state_file: str) -> None:
    """
    更新ETL状态：记录已处理的记录hash和文件信息
    """
    incr_cfg = cfg.get("incremental", {})
    
    if not incr_cfg.get("enabled", False):
        logging.info("增量处理未启用，跳过状态更新")
        return
    
    # 加载当前状态
    state = load_etl_state(state_file)
    
    # 检查关键字段
    key_fields = incr_cfg.get("key_fields", [])
    missing_fields = [field for field in key_fields if field not in df.columns]
    
    if missing_fields:
        logging.warning(f"增量处理所需字段不存在: {missing_fields}，跳过状态更新")
        return
    
    # 生成所有记录的hash（不含文件名）
    df['_record_hash'] = df.apply(lambda row: generate_record_hash(row, key_fields), axis=1)
    new_hashes = set(df['_record_hash'].unique())
    
    # 更新已处理的hash集合
    processed_hashes = state.get("processed_hashes", set())
    processed_hashes.update(new_hashes)
    state["processed_hashes"] = processed_hashes
    
    # 更新文件信息（仅在文件路径有效时）
    file_name = os.path.basename(file_path) if file_path and os.path.exists(file_path) else "processed_data"
    if "processed_files" not in state:
        state["processed_files"] = {}
    
    file_info = {
        "record_count": len(df),
        "processed_time": datetime.now().isoformat(),
        "new_record_count": len(new_hashes)
    }
    
    # 仅在文件路径有效时添加文件元数据
    if file_path and os.path.exists(file_path):
        file_info.update({
            "mtime": os.path.getmtime(file_path),
            "hash": hashlib.md5(str(len(df)).encode('utf-8')).hexdigest()
        })
    
    state["processed_files"][file_name] = file_info
    
    # 更新统计信息
    state["total_records"] = len(processed_hashes)
    
    # 保存状态
    save_etl_state(state, state_file)
    
    logging.info(f"状态更新完成：文件 {file_name}，新记录 {len(new_hashes)} 条，总记录数 {len(processed_hashes)} 条")


def apply_field_mapping(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    应用字段映射
    """
    field_mapping = cfg.get("field_mapping", {})
    
    if not field_mapping:
        return df
    
    result = df.copy()
    renamed_columns = {}
    
    for old_name, new_name in field_mapping.items():
        if old_name in result.columns:
            renamed_columns[old_name] = new_name
    
    if renamed_columns:
        result = result.rename(columns=renamed_columns)
        logging.info(f"字段映射完成: {len(renamed_columns)} 个字段重命名")
    
    return result


def apply_type_conversion(df: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    应用数据类型转换
    """
    type_config = cfg.get("types", {})
    
    if not type_config:
        return df
    
    result = df.copy()
    converted_count = 0
    
    for field, target_type in type_config.items():
        if field in result.columns:
            try:
                if target_type == "datetime":
                    result[field] = pd.to_datetime(result[field])
                elif target_type == "int":
                    result[field] = pd.to_numeric(result[field], errors='coerce').fillna(0).astype('int64')
                elif target_type == "float":
                    result[field] = pd.to_numeric(result[field], errors='coerce')
                elif target_type == "string":
                    result[field] = result[field].astype(str)
                else:
                    result[field] = result[field].astype(target_type)
                
                converted_count += 1
            except Exception as e:
                logging.warning(f"字段 {field} 类型转换失败 ({target_type}): {e}")
    
    if converted_count > 0:
        logging.info(f"数据类型转换完成: {converted_count} 个字段")
    
    return result


def read_product_inspection_data(config: dict, incr_processor: IncrementalProcessor = None) -> pd.DataFrame:
    """
    读取产品检验记录数据
    支持多文件读取和合并，支持文件级增量处理
    
    Args:
        config: 配置字典
        incr_processor: 增量处理器（可选，用于文件级去重）
    """
    source_cfg = config.get("source", {})
    data_path = source_cfg.get("product_inspection_path", "")
    
    if not data_path:
        raise ValueError("未配置产品检验记录数据路径")
    
    logging.info(f"读取产品检验记录数据: {data_path}")
    
    # 处理通配符路径
    if "*" in data_path or "?" in data_path:
        data_files = glob.glob(data_path)
        if not data_files:
            raise FileNotFoundError(f"未找到匹配的数据文件: {data_path}")
        
        # 按文件修改时间排序（最新文件优先）
        data_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        logging.info(f"找到 {len(data_files)} 个产品检验记录数据文件，按修改时间排序")
        
        # 测试模式：限制文件数量
        test_cfg = config.get("test", {})
        test_enabled = test_cfg.get("enabled", False)
        max_files = test_cfg.get("max_files", 20) if test_enabled else len(data_files)
        
        if test_enabled and len(data_files) > max_files:
            data_files = data_files[:max_files]
            logging.info(f"🧪 测试模式：仅读取前 {max_files} 个最新文件")
        
        # 第1层：文件级去重（如果提供了增量处理器）
        if incr_processor:
            original_count = len(data_files)
            data_files = incr_processor.filter_changed_files(data_files)
            if not data_files:
                logging.info("所有文件未变化，无需读取")
                return pd.DataFrame()
        
        # 读取所有文件并合并
        dfs = []
        for i, file_path in enumerate(data_files, start=1):
            try:
                mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                logging.info(f"读取文件 {i}/{len(data_files)}: {os.path.basename(file_path)} (修改时间: {mod_time})")
                df = read_sharepoint_excel(file_path)
                if not df.empty:
                    # 添加文件信息字段（使用中文字段名，后续映射）
                    df['source_file'] = os.path.basename(file_path)
                    df['file_mod_time'] = datetime.fromtimestamp(os.path.getmtime(file_path))
                    
                    # 从文件名提取报表日期（使用中文字段名，后续映射）
                    filename = os.path.basename(file_path)
                    report_date = extract_report_date_from_filename(filename)
                    df['报表日期'] = report_date
                    
                    dfs.append(df)
                    
                    # 标记文件已处理
                    if incr_processor:
                        incr_processor.mark_file_processed(file_path)
            except Exception as e:
                logging.warning(f"读取文件失败 {i}/{len(data_files)}: {file_path}: {e}")
        
        if not dfs:
            raise ValueError("所有产品检验记录数据文件读取失败")
        
        combined_df = pd.concat(dfs, ignore_index=True)
        logging.info(f"合并后产品检验记录数据行数: {len(combined_df)}")
        return combined_df
    
    else:
        # 单文件处理
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"产品检验记录数据文件不存在: {data_path}")
        
        df = read_sharepoint_excel(data_path)
        if not df.empty:
            # 从文件名提取报表日期
            filename = os.path.basename(data_path)
            report_date = extract_report_date_from_filename(filename)
            df['报表日期'] = report_date
            
        return df


def validate_product_inspection_data(df: pd.DataFrame, config: dict) -> bool:
    """
    验证产品检验记录数据完整性
    """
    logging.info("开始验证产品检验记录数据完整性...")
    
    # 检查必要字段
    required_fields = config.get("processing", {}).get("required_fields", 
        ["批次号", "产品序号", "检验结果", "班组"])
    # Note: 合格数 and 不合格数 will be derived during cleaning
    
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        logging.error(f"缺少必要字段: {missing_fields}")
        return False
    
    # 检查数据行数
    if df.empty:
        logging.error("产品检验记录数据为空")
        return False
    
    # 检查关键字段缺失率
    for field in required_fields:
        missing_count = df[field].isnull().sum()
        missing_rate = missing_count / len(df) * 100
        if missing_rate > 5:  # 缺失率超过5%报警
            logging.warning(f"字段 {field} 缺失率较高: {missing_rate:.1f}% ({missing_count}/{len(df)})")
    
    logging.info(f"产品检验记录数据验证通过: {len(df)} 行记录")
    return True


def clean_product_inspection_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    清洗产品检验记录数据
    """
    logging.info("开始清洗产品检验记录数据...")
    
    result = df.copy()
    original_count = len(result)
    
    # 1. 标准化字段名（去除空格）
    result.columns = result.columns.str.strip()
    
    # 2. 标准化批次号：移除IPQCK前缀
    if '批次号' in result.columns:
        result['批次号'] = result['批次号'].astype(str).str.replace(r'^IPQCK', '', regex=True)
        logging.info("已标准化批次号：移除IPQCK前缀")
    
    # 3. 处理机床设备缺失值
    if '机床设备' in result.columns:
        missing_equipment = result['机床设备'].isnull()
        if missing_equipment.any():
            result['机床设备'] = result['机床设备'].astype('object')
            result.loc[missing_equipment, '机床设备'] = result.loc[missing_equipment, '工序编号'].astype(str)
            logging.info(f"已用工序编号填充 {missing_equipment.sum()} 个缺失的机床设备")
    
    # 4. 处理产品序号缺失值
    if '产品序号' in result.columns:
        missing_serial = result['产品序号'].isnull()
        if missing_serial.any():
            result.loc[missing_serial, '产品序号'] = result.loc[missing_serial].groupby('批次号').cumcount() + 1
            logging.info(f"已填充 {missing_serial.sum()} 个缺失的产品序号")
    
    # 5. 标准化检验结果并提取合格数/不合格数
    if '检验结果' in result.columns:
        result['检验结果'] = result['检验结果'].str.strip().str.upper()
        
        # 初始化合格数和不合格数列
        result['合格数'] = 0
        result['不合格数'] = 0
        
        # 提取合格数和不合格数
        import re
        
        # 处理包含具体数值的格式：合格数：100 / 不合格数：0
        count_pattern = r'合格数[：:]\s*(\d+)\s*/\s*不合格数[：:]\s*(\d+)'
        count_matches = result['检验结果'].str.extract(count_pattern)
        
        # 对于匹配到的记录，设置合格数和不合格数
        mask = count_matches[0].notna()
        if mask.any():
            result.loc[mask, '合格数'] = pd.to_numeric(count_matches.loc[mask, 0])
            result.loc[mask, '不合格数'] = pd.to_numeric(count_matches.loc[mask, 1])
        
        # 对于简单的"合格"或"不合格"记录
        simple_mask = result['检验结果'].isin(['合格', 'PASS'])
        result.loc[simple_mask & ~mask, '合格数'] = 1
        result.loc[simple_mask & ~mask, '不合格数'] = 0
        
        fail_mask = result['检验结果'].isin(['不合格', 'FAIL'])
        result.loc[fail_mask & ~mask, '合格数'] = 0
        result.loc[fail_mask & ~mask, '不合格数'] = 1
        
        # 标准化检验结果为简单值
        result.loc[result['检验结果'].str.contains('合格', na=False) | result['检验结果'].str.contains('PASS', na=False), '检验结果'] = '合格'
        result.loc[result['检验结果'].str.contains('不合格', na=False) | result['检验结果'].str.contains('FAIL', na=False), '检验结果'] = '不合格'
        
        logging.info(f"已提取合格数/不合格数信息")
    
    # 6. 业务键去重（保留最新数据）
    before_dedup = len(result)
    
    # 检查业务键是否存在
    business_keys = ['批次号', '产品序号', '工序编号', '工序名称']
    missing_keys = [key for key in business_keys if key not in result.columns]
    
    if missing_keys:
        logging.warning(f"缺少业务键字段 {missing_keys}，使用批次+工序+班组去重")
        backup_keys = ['批次号', '工序编号', '工序名称', '班组']
        backup_missing = [key for key in backup_keys if key not in result.columns]
        if backup_missing:
            logging.warning(f"缺少备选业务键字段 {backup_missing}，使用全行去重")
            result = result.drop_duplicates()
        else:
            result = result.drop_duplicates(subset=backup_keys, keep='first')
            logging.info(f"备选业务键去重: 基于批次+工序+班组移除重复记录")
    else:
        before_business_dedup = len(result)
        result = result.drop_duplicates(subset=business_keys, keep='first')
        
        business_dedup_count = before_business_dedup - len(result)
        logging.info(f"业务键去重: 基于批次号+产品序号+工序编号+工序名称移除 {business_dedup_count} 条重复记录")
        
        # 如果仍有完全重复的行，再进行一次全行去重
        final_before = len(result)
        result = result.drop_duplicates()
        final_dedup_count = final_before - len(result)
        
        if final_dedup_count > 0:
            logging.info(f"全行去重: 移除 {final_dedup_count} 条完全重复记录")
    
    removed_duplicates = before_dedup - len(result)
    logging.info(f"总去重统计: 移除 {removed_duplicates} 条重复记录")
    
    logging.info(f"产品检验记录数据清洗完成: {original_count} → {len(result)} 行")
    return result


def aggregate_employee_efficiency_data(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    按员工+日期+批次+工序聚合，用于计算员工效率
    聚合维度：员工、日期、批次号、工序编号
    指标：合格数、不合格数、最大产品序号（完成数量）
    """
    if df.empty:
        return df
    
    result = df.copy()
    logging.info("开始按员工计算效率指标...")
    
    # 检查必要的字段
    required_fields = ['Employee', 'ReportDate', 'BatchNumber', 'Operation', 'PassQuantity', 'FailQuantity', 'SerialNumber']
    missing_fields = [field for field in required_fields if field not in result.columns]
    if missing_fields:
        logging.error(f"员工聚合所需字段缺失: {missing_fields}")
        return result
    
    # 提取日期维度（从ReportDate提取年月日）
    result['ReportDate_Date'] = result['ReportDate'].dt.date
    result['ReportDate_Year'] = result['ReportDate'].dt.year
    result['ReportDate_Month'] = result['ReportDate'].dt.month
    result['ReportDate_Day'] = result['ReportDate'].dt.day
    result['ReportDate_Week'] = result['ReportDate'].dt.isocalendar().week
    
    # 按员工+日期+批次+工序聚合
    employee_agg = result.groupby([
        'Employee', 
        'ReportDate_Date', 
        'ReportDate_Year', 
        'ReportDate_Month', 
        'ReportDate_Day', 
        'ReportDate_Week',
        'BatchNumber', 
        'Operation', 
        'OperationDescription',
        'Team'
    ]).agg(
        PassQuantity=('PassQuantity', 'sum'),
        FailQuantity=('FailQuantity', 'sum'),
        MaxSerialNumber=('SerialNumber', 'max'),
        CompletedCount=('SerialNumber', 'max'),  # 最大产品序号作为完成数量
        OperationCount=('SerialNumber', 'count'),  # 操作次数
        FileModTime=('FileModTime', 'max'),
        SourceFile=('SourceFile', lambda x: ', '.join(set(x.dropna().astype(str).str.strip())))
    ).reset_index()
    
    logging.info(f"员工级聚合完成: {len(result)} → {len(employee_agg)} 条记录")
    
    # 计算合格率
    employee_agg['PassRate'] = employee_agg.apply(
        lambda row: row['PassQuantity'] / (row['PassQuantity'] + row['FailQuantity']) 
        if (row['PassQuantity'] + row['FailQuantity']) > 0 else 0, 
        axis=1
    )
    
    # 计算效率指标
    employee_agg['Efficiency_Score'] = employee_agg['PassRate']  # 可以扩展为更复杂的效率计算
    
    # 添加时间戳
    employee_agg['AggregatedTime'] = pd.Timestamp.now()
    
    # 重命名字段以便PowerBI使用
    employee_agg = employee_agg.rename(columns={
        'ReportDate_Date': 'Date',
        'ReportDate_Year': 'Year',
        'ReportDate_Month': 'Month', 
        'ReportDate_Day': 'Day',
        'ReportDate_Week': 'Week',
        'MaxSerialNumber': 'MaxSerial',
        'CompletedCount': 'CompletedQuantity',
        'OperationCount': 'OperationCount'
    })
    
    # 统计信息
    total_employees = employee_agg['Employee'].nunique()
    total_days = employee_agg['Date'].nunique()
    avg_pass_rate = employee_agg['PassRate'].mean()
    total_completed = employee_agg['CompletedQuantity'].sum()
    
    logging.info(f"员工效率统计: {total_employees} 个员工, {total_days} 天, 平均合格率 {avg_pass_rate:.4f}, 总完成数 {total_completed}")
    
    return employee_agg


def aggregate_product_inspection_data_batch_level(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    按批次计算真实合格率：以批次为单位，使用最终工序合格数/（最终工序合格数+所有工序不合格数总和）
    """
    if df.empty:
        return df
    
    result = df.copy()
    logging.info("开始按批次计算真实合格率...")
    
    # 检查必要的字段
    required_fields = ['BatchNumber', 'Operation', 'OperationDescription', 'Team', 'Machine']
    missing_fields = [field for field in required_fields if field not in result.columns]
    if missing_fields:
        logging.error(f"聚合所需字段缺失: {missing_fields}")
        return result
    
    # 按批次+产品号+工序编号+班组+机床设备聚合，保留产品序号信息
    operation_agg = result.groupby(['BatchNumber', 'Operation', 'OperationDescription', 'Team', 'Machine']).agg(
        PassQuantity=('PassQuantity', 'sum'),
        FailQuantity=('FailQuantity', 'sum'),
        SerialNumber_count=('SerialNumber', 'count'),
        SerialNumber=('SerialNumber', 'first'),  # 保留第一个产品序号
        FileModTime=('FileModTime', 'max'),
        SourceFile=('SourceFile', lambda x: ', '.join(set(x.dropna().astype(str).str.strip())))
    ).reset_index()
    
    logging.info(f"工序级聚合完成: {len(result)} → {len(operation_agg)} 条记录")
    
    # 过滤掉无效工序编号
    if 'Operation' in operation_agg.columns:
        valid_operations = operation_agg['Operation'].notna()
        operation_agg = operation_agg[valid_operations]
        logging.info(f"过滤无效工序编号: {len(operation_agg)} 条记录")
    
    # 按批次计算最终合格率
    def calculate_batch_pass_rate(group):
        # 找到最终工序（工序编号最大）
        final_operation = group.loc[group['Operation'].astype(str).str.zfill(4).idxmax()]
        
        # 最终工序合格数
        final_pass = final_operation['PassQuantity']
        
        # 所有工序不合格数总和
        total_fail = group['FailQuantity'].sum()
        
        # 计算合格率
        total_count = final_pass + total_fail
        if total_count > 0:
            pass_rate = final_pass / total_count
        else:
            pass_rate = 0
        
        # 返回最终工序的完整信息，加上合格率
        result_row = final_operation.copy()
        result_row['PassRate'] = pass_rate
        result_row['TotalInspectionCount'] = total_count
        
        return result_row
    
    # 按批次分组计算
    batch_results = []
    for batch_name, batch_group in operation_agg.groupby('BatchNumber'):
        try:
            batch_result = calculate_batch_pass_rate(batch_group)
            batch_results.append(batch_result)
        except Exception as e:
            logging.warning(f"批次 {batch_name} 计算失败: {e}")
            continue
    
    if batch_results:
        batch_df = pd.DataFrame(batch_results)
        logging.info(f"批次级合格率计算完成: {len(operation_agg)} → {len(batch_df)} 条记录")
        
        # 计算平均合格率
        avg_pass_rate = batch_df['PassRate'].mean()
        logging.info(f"平均批次合格率: {avg_pass_rate:.4f} (百分比格式)")
        
        return batch_df
    else:
        logging.error("批次级聚合失败，返回空数据")
        return pd.DataFrame()


def save_to_parquet(df: pd.DataFrame, file_path: str) -> bool:
    """
    保存数据到Parquet文件
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if pq is None:
            logging.error("未安装pyarrow，无法保存Parquet文件")
            return False
        
        # 转换为pyarrow表
        table = pa.Table.from_pandas(df)
        
        # 保存到parquet
        pq.write_table(table, file_path)
        
        logging.info(f"已保存Parquet文件: {file_path}, 行数: {len(df)}")
        return True
        
    except Exception as e:
        logging.error(f"保存Parquet文件失败: {e}")
        return False


def save_to_excel(df: pd.DataFrame, file_path: str) -> bool:
    """
    保存数据到Excel文件（用于验证）
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        df.to_excel(file_path, index=False)
        
        logging.info(f"已保存Excel验证文件: {file_path}, 行数: {len(df)}")
        return True
        
    except Exception as e:
        logging.error(f"保存Excel文件失败: {e}")
        return False


def main():
    """主函数"""
    print("=" * 60)
    print("SFC产品检验记录数据清洗ETL")
    print("=" * 60)
    
    # 加载配置
    # 固定使用增量刷新模式（由BAT脚本统一控制）
    # 移除命令行参数，保持ETL脚本纯净
    
    config = load_config(CONFIG_PATH)
    setup_logging_with_rotation(config, project_root)
    
    # 固定使用增量刷新
    is_incremental = True
    force_full_refresh = False
    
    logging.info("="*60)
    logging.info("SFC产品检验ETL启动")
    logging.info("刷新模式: 增量刷新（固定）")
    logging.info("="*60)
    
    try:
        # 初始化增量处理器（状态文件统一放到项目目录）
        state_dir = os.path.join(project_root, "data_pipelines", "sources", "sfc", "state")
        os.makedirs(state_dir, exist_ok=True)
        state_file = os.path.join(state_dir, "etl_sfc_product_inspection_files.json")
        incr_processor = IncrementalProcessor(
            state_file=state_file,
            unique_key_fields=["BatchNumber", "ProductSerial", "Operation", "OperationName"]
        )
        
        # 读取数据（带文件级去重）
        logging.info("开始读取产品检验记录数据...")
        raw_df = read_product_inspection_data(config, incr_processor)
        
        # 如果没有新文件，直接返回
        if raw_df.empty:
            logging.info("没有新文件需要处理，保持现有数据不变")
            return True
        
        # 验证数据（使用中文字段）
        if not validate_product_inspection_data(raw_df, config):
            logging.error("数据验证失败")
            return False
        
        # 清洗数据（使用中文字段）
        cleaned_df = clean_product_inspection_data(raw_df, config)
        
        # 应用字段映射（中文到英文）
        mapped_df = apply_field_mapping(cleaned_df, config)
        
        # 应用类型转换（使用英文字段）
        typed_df = apply_type_conversion(mapped_df, config)
        
        # 增量过滤
        if is_incremental and not force_full_refresh:
            incr_cfg = config.get("incremental", {})
            if incr_cfg.get("enabled", False):
                filtered_df = filter_incremental_data(typed_df, "", config, STATE_PATH)
            else:
                filtered_df = typed_df
        else:
            filtered_df = typed_df
        
        # 仅按员工聚合数据（PowerBI可通过DAX实现批次级分析
        employee_aggregated_df = aggregate_employee_efficiency_data(filtered_df, config)
        
        # 保存结果到双份位置
        output_cfg = config.get("output", {})
        base_dir = output_cfg.get("base_dir", "")
        
        # 本地项目目录
        local_publish_dir = os.path.join(project_root, "data_pipelines", "sources", "sfc", "publish")
        os.makedirs(local_publish_dir, exist_ok=True)
        
        # 员工级主输出文件（重命名为产品检验记录主文件
        parquet_file = os.path.join(base_dir, output_cfg.get("file_name", "SFC_Product_Inspection_latest.parquet"))
        local_parquet_file = os.path.join(local_publish_dir, output_cfg.get("file_name", "SFC_Product_Inspection_latest.parquet"))
        excel_file = os.path.join(base_dir, "excel", output_cfg.get("excel_file", "SFC_Product_Inspection_latest.xlsx"))
        
        # 保存员工级文件到双份位置
        if not employee_aggregated_df.empty:
            if WRITE_PARQUET_OUTPUT:
                save_to_dual_locations(employee_aggregated_df, parquet_file, local_parquet_file, config)
                logging.info(f"产品检验记录数据已保存到双份位置")
            
            if save_to_excel(employee_aggregated_df, excel_file):
                logging.info(f"产品检验记录Excel验证文件已保存: {excel_file}")

            try:
                db = get_default_db_manager()
                db.bulk_insert(employee_aggregated_df, 'sfc_product_inspection_latest', if_exists='replace')
                logging.info(f"已写入SQLite表 sfc_product_inspection_latest: {len(employee_aggregated_df)} 行")
            except Exception as e:
                logging.warning(f"写入SQLite表 sfc_product_inspection_latest 失败: {e}")
        else:
            logging.info("数据为空，跳过文件输出")
        
        # 测试模式额外保存带时间戳的文件
        test_cfg = config.get("test", {})
        if test_cfg.get("enabled", False):
            # 使用新的业务域测试数据目录
            test_output_dir = resolver.get_path("test_data_paths", "sfc")
            if not os.path.exists(test_output_dir):
                os.makedirs(test_output_dir, exist_ok=True)
            
            # 生成时间戳文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 员工级测试文件
            if not employee_aggregated_df.empty:
                test_file = os.path.join(test_output_dir, f"SFC_Product_Inspection_{timestamp}.parquet")
                if WRITE_PARQUET_OUTPUT:
                    if save_to_parquet(employee_aggregated_df, test_file):
                        logging.info(f"测试数据已保存: {test_file}")
        
        # 更新增量状态
        if is_incremental and not force_full_refresh:
            incr_cfg = config.get("incremental", {})
            if incr_cfg.get("enabled", False):
                update_etl_state(typed_df, "processed_data", config, STATE_PATH)
                logging.info("增量处理：状态已更新")
        
        # 保存文件级增量状态
        incr_processor.save()
        
        logging.info("SFC产品检验记录数据处理完成")
        return True
        
    except Exception as e:
        logging.error(f"处理过程中发生错误: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
